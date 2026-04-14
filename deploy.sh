#!/bin/bash
# price-api/deploy.sh  — 部署到 VPS（非 root 用户运行）
set -e

APP_DIR="/opt/price-api"
SERVICE_NAME="price-api"

echo "=== 1/5 创建部署目录 ==="
sudo mkdir -p "$APP_DIR"
sudo chown $USER:$USER "$APP_DIR"

echo "=== 2/5 上传代码 ==="
# 本地打包后 scp 上传，或直接在服务器 git clone
# scp price-api.tar.gz $HOST:/tmp/
# ssh $HOST "tar -xzf /tmp/price-api.tar.gz -C $APP_DIR --strip-components=1"
echo "请先上传 price-api 源码到 $APP_DIR"

echo "=== 3/5 安装 Python 依赖 ==="
cd "$APP_DIR"
pip install -r requirements.txt

echo "=== 4/5 配置环境变量 ==="
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    echo "已创建 .env，请编辑填入凭证：nano $APP_DIR/.env"
fi

echo "=== 5/5 安装 systemd 服务 ==="
sudo cp "$APP_DIR/$SERVICE_NAME.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "=== 完成 ==="
sudo systemctl status "$SERVICE_NAME" --no-pager
echo ""
echo "查看日志：journalctl -u $SERVICE_NAME -f"
echo "重启服务：sudo systemctl restart $SERVICE_NAME"
