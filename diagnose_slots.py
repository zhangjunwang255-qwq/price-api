#!/usr/bin/env python3
"""
诊断时间槽数据准确性
验证每个固定时间槽是否拉到最接近的准确数据
"""
import os
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

try:
    import psycopg2
except ImportError:
    print("需要 psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_db_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("错误: 未设置 DATABASE_URL 环境变量")
        sys.exit(1)
    return url


def parse_db_url(url):
    url = url.replace("postgres://", "postgresql://")
    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }


def connect():
    cfg = parse_db_url(get_db_url())
    return psycopg2.connect(**cfg)


def align_to_5min(dt: datetime) -> datetime:
    """对齐到5分钟时间槽"""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def generate_fixed_slots(end_dt: datetime, interval_min: int, count: int) -> list[datetime]:
    """生成固定时间槽列表"""
    slots = []
    current = align_to_5min(end_dt)
    if interval_min == 15:
        current = current.replace(minute=(current.minute // 15) * 15)
    elif interval_min == 60:
        current = current.replace(minute=0)
    for _ in range(count):
        slots.append(current)
        current -= timedelta(minutes=interval_min)
    return list(reversed(slots))


def diagnose(symbol: str, interval: str, limit: int = 20):
    """诊断指定品种和间隔的数据"""
    interval_map = {"5min": 5, "15min": 15, "1hour": 60}
    interval_min = interval_map.get(interval, 5)

    conn = connect()
    cur = conn.cursor()

    print(f"\n{'='*70}")
    print(f"诊断: {symbol} | 间隔: {interval} | 固定时间槽: {interval_min}分钟")
    print(f"{'='*70}")

    # 1. 查看原始数据
    cur.execute("""
        SELECT id, price, dt, created_at
        FROM price_history
        WHERE symbol = %s
        ORDER BY dt DESC
        LIMIT 30
    """, (symbol,))
    raw_rows = cur.fetchall()

    print(f"\n【原始数据】共 {len(raw_rows)} 条（显示最近30条）:")
    print(f"{'ID':>6} | {'Price':>12} | {'dt (原始)':>30} | {'created_at':>30}")
    print("-" * 90)
    for row in raw_rows:
        print(f"{row[0]:>6} | {row[1]:>12.4f} | {str(row[2]):>30} | {str(row[3]):>30}")

    # 2. 查看原始数据的时间分布
    print(f"\n【时间分布】")
    cur.execute("""
        SELECT 
            COUNT(*) as count,
            MIN(dt) as earliest,
            MAX(dt) as latest,
            COUNT(DISTINCT DATE(dt)) as distinct_dates
        FROM price_history
        WHERE symbol = %s
    """, (symbol,))
    row = cur.fetchone()
    print(f"  记录数: {row[0]}")
    print(f"  最早: {row[1]}")
    print(f"  最晚: {row[2]}")
    print(f"  不同天数: {row[3]}")

    # 3. 检查是否有时间戳相同的问题
    cur.execute("""
        SELECT dt, COUNT(*) as cnt
        FROM price_history
        WHERE symbol = %s
        GROUP BY dt
        HAVING COUNT(*) > 1
    """, (symbol,))
    dupes = cur.fetchall()
    if dupes:
        print(f"\n  ⚠️ 警告: 发现 {len(dupes)} 个时间戳有多条记录:")
        for d in dupes:
            print(f"    {d[0]}: {d[1]} 条")
    else:
        print(f"\n  ✅ 无重复时间戳")

    # 4. 生成固定时间槽并验证匹配
    now = datetime.now(timezone.utc)
    slots = generate_fixed_slots(now, interval_min, limit)

    print(f"\n【固定时间槽 & 数据匹配】(从当前时间往前 {limit} 个槽)")
    print(f"{'固定槽时间':>30} | {'匹配到数据':>12} | {'时间差':>10} | {'原始数据时间':>30} | {'Price':>12}")
    print("-" * 100)

    # 查询所有原始数据用于匹配
    cur.execute("""
        SELECT price, dt
        FROM price_history
        WHERE symbol = %s
        ORDER BY dt ASC
    """, (symbol,))
    all_db_rows = cur.fetchall()

    all_prices = [float(r[0]) for r in all_db_rows]
    all_dts = [r[1] for r in all_db_rows]

    for slot in slots:
        best_idx = -1
        best_diff = timedelta.max

        for i, dt in enumerate(all_dts):
            diff = abs(dt - slot)
            if diff < best_diff and diff <= timedelta(minutes=interval_min / 2):
                best_diff = diff
                best_idx = i

        if best_idx >= 0:
            price = all_prices[best_idx]
            matched_dt = all_dts[best_idx]
            diff_min = best_diff.total_seconds() / 60
            status = "✅" if diff_min <= 2 else "⚠️"
            print(f"{str(slot):>30} | {status}匹配   | {diff_min:>8.1f}min | {str(matched_dt):>30} | {price:>12.4f}")
        else:
            print(f"{str(slot):>30} | ❌ 无数据 |       - |                - |            -")

    cur.close()
    conn.close()


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print(f"  python {sys.argv[0]} pt 5min      # 诊断铂金5分钟")
        print(f"  python {sys.argv[0]} pd 15min     # 诊断钯金15分钟")
        print(f"  python {sys.argv[0]} pt 1hour     # 诊断铂金1小时")
        sys.exit(1)

    symbol_map = {"pt": "KQ.m@GFEX.pt", "pd": "KQ.m@GFEX.pd"}
    interval_map = {"5min": "5min", "15min": "15min", "1hour": "1hour"}

    sym_key = sys.argv[1].lower()
    interval = sys.argv[2].lower() if len(sys.argv) > 2 else "5min"

    if sym_key not in symbol_map:
        print(f"未知品种: {sym_key}，可用: pt, pd")
        sys.exit(1)

    if interval not in interval_map:
        print(f"未知间隔: {interval}，可用: 5min, 15min, 1hour")
        sys.exit(1)

    diagnose(symbol_map[sym_key], interval, limit=20)


if __name__ == "__main__":
    main()
