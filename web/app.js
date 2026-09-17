/* chatroom 前端 — 房间列表 + 浏览器生成的唯一客户端标识 + 进入房间聊天。
 *
 * 要点:
 *   • cid(唯一客户端标识)由浏览器生成并存在 localStorage, 重连/刷新后身份不变,
 *     服务端据此把同房间的旧连接顶掉, 不会留下"重影"用户。
 *   • 房间列表轮询 GET /api/rooms。聊天走同源 WebSocket(同端口)。
 *   • 收到服务端 Ping 立即回 Pong; 断线按 1→32s 指数退避自动重连。
 */
(() => {
  'use strict';

  const CHAT = 'CHAT', SYSTEM = 'SYSTEM', WELCOME = 'WELCOME', HEARTBEAT = 'HEARTBEAT', HELLO = 'HELLO';
  const ROOMS_POLL_MS = 5000;
  const BACKOFF_BASE_MS = 1000, BACKOFF_CAP_MS = 32000;
  const CLOSE_REPLACED = 4001;          // 服务端顶号时的关闭码: 不重连

  const $ = (id) => document.getElementById(id);
  const viewRooms = $('view-rooms'), viewChat = $('view-chat');
  const roomsBox = $('rooms'), roomsStatus = $('rooms-status');
  const chatTitle = $('chat-title'), chatStatus = $('chat-status');
  const messagesBox = $('messages'), banner = $('banner');
  const input = $('input'), composer = $('composer'), sendBtn = $('send');
  const nickInput = $('nick'), saveNickBtn = $('save-nick'), cidLabel = $('cid');
  const backBtn = $('back');

  // ---------------------------------------------------------------- 身份

  function uuid4() {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
    const bytes = new Uint8Array(16);
    if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
      crypto.getRandomValues(bytes);
    } else {
      for (let i = 0; i < bytes.length; i += 1) bytes[i] = Math.floor(Math.random() * 256);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;      // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80;      // variant
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  const store = {
    get(key) { try { return localStorage.getItem(key); } catch (e) { return null; } },
    set(key, value) { try { localStorage.setItem(key, value); } catch (e) { /* 隐私模式 */ } },
  };

  let cid = store.get('chatroom.cid');
  if (!cid) { cid = uuid4(); store.set('chatroom.cid', cid); }
  let nick = store.get('chatroom.nick') || `访客-${cid.slice(0, 4)}`;

  nickInput.value = nick;
  cidLabel.textContent = `ID ${cid.slice(0, 8)}`;
  cidLabel.title = `客户端唯一标识: ${cid}`;

  saveNickBtn.addEventListener('click', () => {
    const next = nickInput.value.trim();
    if (!next) { nickInput.value = nick; return; }
    nick = next;
    store.set('chatroom.nick', nick);
    flash(roomsStatus, '昵称已保存（下次进入房间生效）');
  });

  // ---------------------------------------------------------------- 视图

  function showRoomsView() {
    viewChat.classList.add('hidden');
    viewRooms.classList.remove('hidden');
    pollRooms();
  }

  function showChatView(name) {
    viewRooms.classList.add('hidden');
    viewChat.classList.remove('hidden');
    chatTitle.textContent = name;
  }

  function flash(node, text) {
    node.textContent = text;
  }

  function showBanner(text, tone) {
    banner.textContent = text;
    banner.className = `banner ${tone || ''}`.trim();
  }

  function hideBanner() { banner.className = 'banner hidden'; }

  function appendMessage(kind, frm, text) {
    const row = document.createElement('div');
    row.className = `msg ${kind}`;
    if (frm) {
      const who = document.createElement('span');
      who.className = 'who';
      who.textContent = frm;
      row.appendChild(who);
    }
    const body = document.createElement('span');
    body.className = 'body';
    body.textContent = text;                 // textContent: 天然防 XSS
    row.appendChild(body);
    const atBottom = messagesBox.scrollHeight - messagesBox.scrollTop - messagesBox.clientHeight < 80;
    messagesBox.appendChild(row);
    if (atBottom) messagesBox.scrollTop = messagesBox.scrollHeight;
  }

  // ---------------------------------------------------------------- 房间列表

  async function pollRooms() {
    try {
      const resp = await fetch('/api/rooms', { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const rooms = await resp.json();
      renderRooms(rooms);
      const mine = rooms.find((r) => r.name === currentRoom);
      if (mine && socket && socket.readyState === WebSocket.OPEN) {
        flash(chatStatus, `${mine.online} 人在线`);
      }
    } catch (err) {
      flash(roomsStatus, `房间列表获取失败: ${err.message}`);
    }
  }

  function renderRooms(rooms) {
    roomsBox.textContent = '';
    if (!rooms.length) {
      const empty = document.createElement('p');
      empty.className = 'muted';
      empty.textContent = '暂无房间';
      roomsBox.appendChild(empty);
      return;
    }
    flash(roomsStatus, `${rooms.length} 个房间`);
    rooms.forEach((room) => {
      const card = document.createElement('button');
      card.type = 'button';
      card.className = `room-card${room.name === currentRoom ? ' current' : ''}`;
      const name = document.createElement('span');
      name.className = 'room-name';
      name.textContent = room.name;
      const online = document.createElement('span');
      online.className = `badge${room.online ? ' live' : ''}`;
      online.textContent = `${room.online} 人在线`;
      card.append(name, online);
      card.addEventListener('click', () => enterRoom(room.name));
      roomsBox.appendChild(card);
    });
  }

  // ---------------------------------------------------------------- 聊天连接

  let socket = null;
  let currentRoom = null;
  let leaving = false;           // 用户主动返回: 不重连
  let replaced = false;          // 被顶号: 不重连
  let backoff = BACKOFF_BASE_MS;
  let reconnectTimer = null;
  let countdownTimer = null;

  function wsUrl() {
    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${scheme}//${location.host}/ws`;
  }

  function clearTimers() {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
  }

  function enterRoom(name) {
    if (currentRoom && currentRoom !== name) closeSocket(true);
    currentRoom = name;
    leaving = false;
    replaced = false;
    backoff = BACKOFF_BASE_MS;
    clearTimers();
    messagesBox.textContent = '';
    showChatView(name);
    showBanner('连接中…', 'warn');
    connect();
  }

  function connect() {
    if (!currentRoom) return;
    let ws;
    try {
      ws = new WebSocket(wsUrl());
    } catch (err) {
      scheduleReconnect(`无法建立连接: ${err.message}`);
      return;
    }
    socket = ws;

    ws.addEventListener('open', () => {
      backoff = BACKOFF_BASE_MS;
      hideBanner();
      flash(chatStatus, '已连接');
      ws.send(JSON.stringify({ type: HELLO, frm: nick, room: currentRoom, cid }));
    });

    ws.addEventListener('message', (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch (e) { return; }
      onMessage(ws, msg);
    });

    ws.addEventListener('close', (event) => {
      if (socket === ws) socket = null;
      if (leaving || replaced) return;
      if (event.code === CLOSE_REPLACED) {
        replaced = true;
        showBanner('这个房间已在别处打开，本连接被替换。请勿重复进入同一房间。', 'error');
        flash(chatStatus, '已被替换');
        return;
      }
      scheduleReconnect(event.reason ? `连接断开: ${event.reason}` : '连接断开');
    });

    ws.addEventListener('error', () => { /* close 事件里统一处理 */ });
  }

  function onMessage(ws, msg) {
    if (msg.type === HEARTBEAT) {
      if (!msg.ack) ws.send(JSON.stringify({ type: HEARTBEAT, ack: true }));
      return;
    }
    if (msg.type === WELCOME) {
      nick = msg.frm || nick;                       // 服务端可能给出 #N 后缀
      nickInput.value = nick;
      showBanner(`已进入「${msg.room || currentRoom}」`, 'ok');
      setTimeout(hideBanner, 1500);
      flash(chatStatus, `${msg.content} 人在线`);
      return;
    }
    if (msg.type === CHAT) { appendMessage('chat', msg.frm, msg.content); return; }
    if (msg.type === SYSTEM) { appendMessage('system', '', msg.content); return; }
  }

  function scheduleReconnect(reason) {
    clearTimers();
    const delay = backoff;
    backoff = Math.min(backoff * 2, BACKOFF_CAP_MS);
    let left = Math.round(delay / 1000);
    showBanner(`${reason}；${left}s 后重连…`, 'error');
    flash(chatStatus, '重连中…');
    countdownTimer = setInterval(() => {
      left -= 1;
      if (left > 0) showBanner(`${reason}；${left}s 后重连…`, 'error');
    }, 1000);
    reconnectTimer = setTimeout(() => { clearTimers(); connect(); }, delay);
  }

  function closeSocket(silent) {
    clearTimers();
    if (socket) {
      const ws = socket;
      socket = null;
      if (silent) leaving = true;
      try { ws.close(1000, 'client left'); } catch (e) { /* ignore */ }
    }
  }

  function leaveRoom() {
    leaving = true;
    closeSocket(true);
    currentRoom = null;
    showRoomsView();
  }

  backBtn.addEventListener('click', leaveRoom);

  composer.addEventListener('submit', (event) => {
    event.preventDefault();
    const text = input.value.trim();
    if (!text) return;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      showBanner('当前未连接，消息未发送。', 'error');
      return;
    }
    socket.send(JSON.stringify({ type: CHAT, frm: nick, content: text, room: currentRoom }));
    appendMessage('mine', '我', text);              // 服务端不回推发送者, 本地回显
    input.value = '';
    input.focus();
  });

  pollRooms();
  setInterval(pollRooms, ROOMS_POLL_MS);
})();
