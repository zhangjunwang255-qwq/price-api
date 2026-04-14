# SRE Price API — 实时行情 API 服务

铂金（GFEX.PT）、钯金（GFEX.PD）实时行情，支持 HTTP 轮询 / WebSocket 推送 / 价格历史查询。

**推荐部署：Railway**（免费，支持常驻后台进程，5 分钟部署完成）

## 架构

```
Railway
  └── TqSdk 后台线程 → PriceStore (内存 + MySQL)
                           │
                      FastAPI → 公网 HTTPS URL
                           │
               Hostinger 网站前端直接调 /quote
```

Railway 提供免费域名：`https://xxx.up.railway.app`

## 环境变量

| 变量 | 说明 | 必填 |
|------|------|------|
| `TQ_USER` | 快期账号 | ✅ |
| `TQ_PASS` | 快期密码 | ✅ |
| `MYSQL_HOST` | MySQL 地址 | ✅（用 Railway 内置数据库则留空） |
| `MYSQL_PORT` | MySQL 端口 | 默认 3306 |
| `MYSQL_USER` | MySQL 用户名 | ✅ |
| `MYSQL_PASSWORD` | MySQL 密码 | ✅ |
| `MYSQL_DATABASE` | 数据库名 | 默认 price_history |
| `PORT` | 服务端口 | 默认 7860（Railway 自动注入 RAILWAY_PORT） |

## Railway 部署步骤（5 分钟）

### 1. 推送代码到 GitHub

把 `price-api/` 目录内容上传到一个 GitHub 仓库（建议命名为 `sre-price-api`）。

```bash
cd price-api
git init
git add .
git commit -m "init"
gh repo create sre-price-api --public --push
```

### 2. Railway 控制台创建项目

1. 打开 [railway.app](https://railway.app)，用 GitHub 登录
2. 点击 **New Project** → **Deploy from GitHub repo** → 选 `sre-price-api`
3. Railway 会自动检测 Python 并开始构建

### 3. 配置环境变量

在 Railway 项目面板 → **Variables** 添加：

```
TQ_USER=你的快期账号
TQ_PASS=你的快期密码
MYSQL_HOST=（留空则用 Railway 内置数据库）
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=（留空则用 Railway 内置数据库）
MYSQL_DATABASE=price_history
```

> **用 Railway 内置 MySQL？** 在 Railway 项目面板 → **Add Database** → **MariaDB**，然后把 `MYSQL_HOST` / `MYSQL_PASSWORD` 留空，Railway 会自动注入 `DATABASE_URL` 环境变量。代码里已支持直接读 `mysql://` 格式。

### 4. 部署

Railway 自动构建并部署。完成后点开生成的域名：

```
https://xxx.up.railway.app/quote     ← 所有品种实时价
https://xxx.up.railway.app/health    ← 健康检查
```

## 接口列表

| 接口 | 方法 | 说明 |
|------|------|------|
| `/quote` | GET | 所有品种最新行情 |
| `/quote/GFEX.PT` | GET | 铂金最新行情 |
| `/history?symbol=GFEX.PT&minutes=60` | GET | 历史价格 |
| `/symbols` | GET | 可用品种列表 |
| `/ws/quote` | WebSocket | 实时推送（每秒） |
| `/health` | GET | 健康检查 |

## 返回示例

### GET /quote

```json
{
  "status": "Running",
  "error": null,
  "data": {
    "GFEX.PT": {
      "symbol": "GFEX.PT2606",
      "instrument_id": "PT2606",
      "price": 1234.5,
      "volume": 12345,
      "datetime": "2025-04-14 12:00:00",
      "change": 2.3,
      "change_pct": 0.19
    },
    "GFEX.PD": { ... }
  }
}
```

## Hostinger 网站接入

### 方式1：前端 JS 轮询（最简单）

铂钯页面 HTML 里加这段：

```html
<script>
async function fetchPrice() {
  try {
    const res = await fetch('https://你的railway地址.up.railway.app/quote');
    const { data, status } = await res.json();
    if (status !== 'Running') return;
    document.getElementById('pt-price').textContent = data['GFEX.PT'].price;
    document.getElementById('pd-price').textContent = data['GFEX.PD'].price;
  } catch (e) { console.error(e); }
}
setInterval(fetchPrice, 2000);
fetchPrice();
</script>
```

### 方式2：WebSocket（延迟更低）

```javascript
const ws = new WebSocket('wss://你的railway地址.up.railway.app/ws/quote');
ws.onmessage = (e) => {
  const { data } = JSON.parse(e.data);
  document.getElementById('pt-price').textContent = data['GFEX.PT'].price;
  document.getElementById('pd-price').textContent = data['GFEX.PD'].price;
};
```

## 本地开发

```bash
pip install -r requirements.txt
export TQ_USER=xxx TQ_PASS=xxx
export MYSQL_HOST=localhost MYSQL_PORT=3306
export MYSQL_USER=root MYSQL_PASSWORD=xxx
python -m app.main
```

## Docker 部署（可选）

```bash
docker build -t price-api .
docker run -d -p 7860:7860 --env-file .env price-api
```

## 修改品种

编辑 `app/config.py` 的 `SYMBOLS` 列表。
