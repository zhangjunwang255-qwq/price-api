"""
app/tqsdk_worker.py — TqSdk 后台采集线程
"""
import threading, time, logging
from contextlib import suppress

from tqsdk import TqApi, TqAuth

from .config import SYMBOLS, DEADLINE_SEC

log = logging.getLogger("tqsdk-worker")

# 全局引用，shutdown 时关闭
_api_ref = None
_loop_ref = None

# ── 推送给 WebSocket 客户端 ──────────────────────────
_ws_broadcast = None   # 由 main.py 注入


def set_broadcast_fn(fn):
    global _ws_broadcast
    _ws_broadcast = fn


def start_tqsdk(user: str, password: str, store, app_state):
    """在 daemon 线程里启动 TqSdk 连接（断线自动重连）"""
    if not user or not password:
        app_state.status = "AuthRequired"
        app_state.error  = "TQ_USER / TQ_PASS 环境变量未设置"
        log.error("缺少 TQ_USER 或 TQ_PASS")
        return

    def run():
        global _api_ref
        retry_delay = 10  # 重试间隔（秒），断线后逐步增加

        while True:
            app_state.status = "Connecting"
            log.info("TqSdk 连接中 ...")

            try:
                api = TqApi(auth=TqAuth(user, password))
                _api_ref = api
                app_state.status      = "Running"
                app_state.error       = None
                app_state.connected_at = time.strftime("%Y-%m-%d %H:%M:%S")
                retry_delay = 10  # 连接成功，重置退避间隔
                log.info("TqSdk 连接成功，已订阅: %s", SYMBOLS)

                quotes   = {sym: api.get_quote(sym) for sym in SYMBOLS}
                last_log = 0.0

                # 内层循环：正常数据拉取，断线时跳出触发外层重连
                while True:
                    try:
                        api.wait_update(deadline=time.time() + DEADLINE_SEC)
                    except Exception as e:
                        # wait_update 内部断开时抛异常，走到这里重连
                        log.warning("TqSdk 连接断开 (%s)，%.0f 秒后重连 ...", e, retry_delay)
                        app_state.status = "Reconnecting"
                        app_state.error  = str(e)
                        break  # 跳出内层，进入外层重连

                    for sym, q in quotes.items():
                        store.update(
                            symbol        = sym,
                            instrument_id = q.instrument_id,
                            price         = q.last_price,
                            volume        = q.volume,
                            dt            = q.datetime,
                        )

                    # WebSocket 广播
                    if _ws_broadcast:
                        _ws_broadcast({
                            "status": app_state.status,
                            "data":   store.latest,
                        })

                    # 每 30s 心跳日志
                    now = time.time()
                    if now - last_log >= 30:
                        latest = store.latest
                        log.info(
                            "行情 | 铂金=%s | 钯金=%s",
                            latest.get("KQ.m@GFEX.pt", {}).get("price", "N/A"),
                            latest.get("KQ.m@GFEX.pd", {}).get("price", "N/A"),
                        )
                        last_log = now

            except Exception as e:
                app_state.status = "Crashed"
                app_state.error  = str(e)
                log.warning("TqSdk 连接失败: %s，%.0f 秒后重试 ...", e, retry_delay)

            # 无论如何清理连接
            with suppress(Exception):
                _api_ref.close()
            _api_ref = None

            # 指数退避：10s → 20s → 40s → ... → 上限 300s（5分钟）
            log.info("等待 %.0f 秒后重连 ...", retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

    t = threading.Thread(target=run, daemon=True, name="tqsdk-worker")
    t.start()
    log.info("TqSdk 工作线程已启动（断线自动重连）")
