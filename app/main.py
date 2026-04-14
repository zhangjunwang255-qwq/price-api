"""
app/main.py — FastAPI 入口
"""
import logging, os
import asyncio, threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .config import PORT
from .store  import PriceStore
from .tqsdk_worker import start_tqsdk

# ── 日志 ──────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("price-api")

# ── 全局状态 ──────────────────────────────────────────
class AppState:
    status:       str = "Starting"
    error:        str | None = None
    connected_at: str = ""
    symbols:      list[str] = []

app_state = AppState()

# ── 存储 ──────────────────────────────────────────────
store = PriceStore()

# ── 启动 TqSdk 采集线程 ───────────────────────────────
start_tqsdk(
    user     = os.getenv("TQ_USER", ""),
    password = os.getenv("TQ_PASS", ""),
    store    = store,
    app_state= app_state,
)

# ── FastAPI 实例 ──────────────────────────────────────
app = FastAPI(
    title       = "SRE Price API",
    description = "铂金/钯金实时行情 API，支持 HTTP 轮询 / WebSocket 推送 / 价格历史查询",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── WebSocket 客户端管理 ──────────────────────────────
ws_clients: set[WebSocket] = set()
_ws_lock    = threading.Lock()


def broadcast(data: dict):
    """线程安全地推送给所有 WS 客户端"""
    dead = set()
    for ws in ws_clients:
        try:
            asyncio.run(ws.send_json(data))
        except Exception:
            dead.add(ws)
    if dead:
        with _ws_lock:
            ws_clients -= dead


# 注入广播函数给 tqsdk_worker
import app.tqsdk_worker as tw
tw._ws_broadcast = broadcast


@app.websocket("/ws/quote")
async def ws_quote(ws: WebSocket):
    await ws.accept()
    with _ws_lock:
        ws_clients.add(ws)
    log.info("WS 连接 (当前 %d 个)", len(ws_clients))
    try:
        while True:
            await ws.send_json({
                "status": app_state.status,
                "error":  app_state.error,
                "data":   store.latest,
            })
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            ws_clients.discard(ws)
        log.info("WS 断开 (当前 %d 个)", len(ws_clients))


# ── HTTP 接口 ─────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "SRE Price API",
        "version": "1.0.0",
        "status":  app_state.status,
        "endpoints": {
            "/quote":           "所有品种最新行情",
            "/quote/{symbol}":  "单个品种行情",
            "/history":         "价格历史（?symbol=GFEX.PT&minutes=60）",
            "/symbols":         "已订阅品种列表",
            "/health":          "健康检查",
            "/ws/quote":        "WebSocket 实时推送",
        },
    }


@app.get("/quote")
def get_quote():
    return {
        "status":     app_state.status,
        "error":      app_state.error,
        "data":       store.latest,
    }


@app.get("/quote/{symbol}")
def get_quote_one(symbol: str):
    if symbol not in store.latest:
        raise HTTPException(404, f"未知品种: {symbol}")
    return {
        "status": app_state.status,
        "data":   store.latest[symbol],
    }


@app.get("/history")
def get_history(
    symbol:  str = Query(..., description="品种代码，如 SHFE.pg"),
    minutes: int = Query(60,  ge=1, le=1440, description="最近多少分钟"),
    limit:   int = Query(200, ge=1, le=10000, description="最多返回条数"),
):
    records = store.get_history(symbol, minutes=minutes, limit=limit)
    return {
        "symbol":  symbol,
        "minutes": minutes,
        "count":   len(records),
        "records": records,
    }


@app.get("/symbols")
def list_symbols():
    return {
        "symbols": store.symbols,
        "status":  app_state.status,
    }


@app.get("/health")
def health():
    ok = app_state.status in ("Running", "Connecting", "AuthRequired", "Starting")
    return {
        "ok":     ok,
        "status": app_state.status,
        "error":  app_state.error,
    }


# ── 启动 ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    log.info("启动 Price API → 0.0.0.0:%d", PORT)
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, log_level="info", reload=False)
