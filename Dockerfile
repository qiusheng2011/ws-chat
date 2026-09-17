# chatroom 一体化镜像: 后端 + 前端静态资源 + SQLite。
#
# 数据落在容器内 /data/chatroom.db, 必须挂载卷持久化,
# 否则容器删除即丢数据。推荐用仓库里的 docker-compose.yml。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 先装依赖(单独成层, 代码变更可复用缓存)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 后端包 + 前端静态资源
COPY chatroom/ ./chatroom/
COPY web/ ./web/

# SQLite 数据目录(挂卷后重建/重启容器不丢数据)
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 9002

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python3 -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:9002/api/rooms',timeout=4)" || exit 1

CMD ["python3", "-m", "chatroom", "server", \
     "--host", "0.0.0.0", "--port", "9002", \
     "--db", "/data/chatroom.db"]
