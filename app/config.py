"""
app/config.py — 配置（全部通过环境变量）
"""
import os

TQ_USER   = os.getenv("TQ_USER", "")
TQ_PASS   = os.getenv("TQ_PASS", "")
PORT      = int(os.getenv("PORT", os.getenv("RAILWAY_PORT", "7860")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# PostgreSQL（Railway 内置数据库，或自行设置 postgres://...）
DATABASE_URL = os.getenv("DATABASE_URL", "")

# 默认运行模式：竞标（实时K线，不落库）/ 日常（5min采样入库）
# Railway 重启后自动恢复为此模式，建议保持 "日常"
DEFAULT_MODE = os.getenv("DEFAULT_MODE", "日常")

# 要订阅的品种列表（广期所，pt=铂金 pd=钯金，KQ.m@ 主力连续合约自动换月）
SYMBOLS = ["KQ.m@GFEX.pt", "KQ.m@GFEX.pd"]

# wait_update 超时秒数（保证休市时也能正常轮询）
DEADLINE_SEC = 2.0

# 日常模式采样间隔（秒）：每 5 分钟落一条原始数据
SAMPLE_INTERVAL_SEC = 300

# 每个品种最多保留原始记录条数（超量自动清理最老的）
# 300 条 × 5 分钟 ≈ 25 小时
KEEP_RECORDS = 300
