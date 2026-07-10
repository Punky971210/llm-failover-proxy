FROM python:3.12-slim

WORKDIR /app

# 时区（与 Windows 保持一致）
ENV TZ=Asia/Shanghai

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG APP_SRC_HASH
COPY app/ ./app/
COPY config.yaml .

# 日志目录（docker-compose 挂载到宿主机的 provider-switch-log）
ENV PROXY_LOG_DIR=/app/logs

EXPOSE 8000

CMD ["python", "-m", "app.main"]
