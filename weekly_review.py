# ============================================================
# ASX SYSTEM — weekly_review.py  v2
#
# 月报（原周报，跨度从7天拉长到30天）：只看EOD screener.py选出的
# Top3表现，不再包含落选候选和intraday信号（v1里有，v2按需求砍掉）。
#
#   对每一个Top3入选：
#     1) 已经resolve成WIN/LOSS/TIMEOUT的，直接读screener.py自己
#        算好的outcome/outcome_pct（不重复造轮子）
#     2) 还PENDING的，用日线数据最后一根收盘价算浮盈浮亏
#     3) 不管是否已平仓，都额外列出「入选当天 → 今天」区间内的
#        最高价/最低价，以及各自发生的日期
#        —— 注意：已平仓的股票，这个区间会包含"实际持仓期之外"
#           的走势（因为系统早就空仓了），所以已平仓行会额外标注
#           "(含离场后走势，仅供参考)"，避免误读成"系统抓住了这个
#           高点"
#
# v1 → v2 变更：
#   - REVIEW_DAYS: 7 → 30
#   - 砍掉落选候选（is_selected=0）展示，SQL直接WHERE is_selected=1
#   - 砍掉intraday_signals_log整块（build_intraday_section等）
#   - 新增 get_price_stats_since()：入选日至今的区间最高/最低价+日期
#   - 效率优化：不再单独调fast_info查"现价"，直接复用区间日线数据
#     最后一根收盘价（周报固定周六跑，此时市场已收盘，两者等价，
#     省一次网络请求，Top3拉到30天后单只股票可能到60+条，减少请求
#     量能降低被yfinance限流的风险）
#   - 投递方式：原来是HTML文本按4000字符分段发sendMessage；30天
#     60+条记录太长，改成生成.txt文件用sendDocument整个推送，
#     Telegram消息本身只带一句caption
#   - 报告文件本地留档在 weekly_reports/ 子目录（体积很小，一年
#     52份也就几百KB，留着方便你回溯，不做自动清理）
#
# 触发方式不变：
#   1) crontab周六自动跑一次
#   2) bot.py的/weekly命令通过run_script()调用
#
# 明确不做的事（跟v1一致）：
#   - 不判定"最终胜率"（意义不大，样本大部分还没跑完）
#   - 不做intraday信号的WIN/LOSS/TIMEOUT持久化结果记录
# ============================================================

import os
import sys
import time
import logging
import sqlite3
import requests
import yfinance as yf
import pandas as pd
from datetime import date, datetime, timedelta

import watchlist_db as wdb

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPORTS_DIR = os.path.join(_SCRIPT_DIR, "weekly_reports")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(_SCRIPT_DIR, "weekly_review.log"),
                            encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")

# screener.py维护的announcements.db（signals_history表在这里）
ANN_DB_PATH = os.path.join(_SCRIPT_DIR, "announcements.db")

REVIEW_DAYS = 30  # 月报跨度（自然日）


# ════════════════════════════════════════════════════════════
# 1. Telegram
# ════════════════════════════════════════════════════════════

def send_telegram_text(text: str) -> None:
    """短文本推送，只用于报告本体失败时的错误告警，不用于正文
    （正文改走send_telegram_document）"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID, "text": text[:4000],
        }, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Telegram文本推送失败: {e}")


def send_telegram_document(file_path: str, caption: str = "", retries: int = 2) -> None:
    """
    把报告txt文件当附件推送。这是整份月报唯一的投递路径，
    失败了用户就完全收不到报告，所以这里比其他Telegram调用
    多一层重试（跟get_current_price一样的指数退避思路）。
    """
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过推送")
        return
    if not os.path.exists(file_path):
        log.error(f"待推送文件不存在: {file_path}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    for attempt in range(1, retries + 1):
        try:
            with open(file_path, "rb") as f:
                files = {"document": (os.path.basename(file_path), f, "text/plain")}
                data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
                r = requests.post(url, data=data, files=files, timeout=30)
                r.raise_for_status()
            log.info(f"Telegram文件推送成功: {file_path}")
            return
        except Exception as e:
            log.warning(f"Telegram文件推送失败(第{attempt}/{retries}次): {e}")
            if attempt < retries:
                time.sleep(2.0 * attempt)
    log.error(f"Telegram文件推送最终失败，已放弃: {file_path}")


# ════════════════════════════════════════════════════════════
# 2. 价格区间统计
# ════════════════════════════════════════════════════════════

def get_price_stats_since(ticker: str, since_date: str) -> dict:
    """
    下载since_date（含）到今天的日线数据，返回：
      - high / high_date：区间最高价及发生日期
      - low  / low_date ：区间最低价及发生日期
      - last_close / last_date：最近一根收盘价及日期
        （代替原来单独查fast_info的"现价"——周报固定周六跑，
        市场已收盘，两者数值一致，省一次网络请求）

    period="3mo"给足缓冲，覆盖REVIEW_DAYS=30天的信号，即使
    since_date刚好卡在边界也不会漏数据。

    返回None表示数据获取失败，调用方要能处理这个情况，不能假设
    一定拿得到数据（yfinance偶发限流/网络问题很正常）。
    """
    try:
        df = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        idx = df.index.astype(str).str[:10]
        df = df.set_axis(idx)
        df = df[df.index >= since_date]
        if df.empty:
            return None

        high_date = df["High"].idxmax()
        low_date = df["Low"].idxmin()

        return {
            "high": round(float(df["High"].max()), 4),
            "high_date": high_date,
            "low": round(float(df["Low"].min()), 4),
            "low_date": low_date,
            "last_close": round(float(df["Close"].iloc[-1]), 4),
            "last_date": df.index[-1],
        }
    except Exception as e:
        log.warning(f"get_price_stats_since失败 [{ticker}]: {e}")
        return None


# ════════════════════════════════════════════════════════════
# 3. EOD Top3表现（signals_history，screener.py维护）
# ════════════════════════════════════════════════════════════

def query_eod_top3(days: int = REVIEW_DAYS) -> list:
    """只拉is_selected=1（Top3入选），落选候选不再查询。"""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    if not os.path.exists(ANN_DB_PATH):
        log.warning(f"announcements.db不存在: {ANN_DB_PATH}")
        return []
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if "signals_history" not in tables:
                log.warning("signals_history表不存在")
                return []
            rows = conn.execute("""
                SELECT ticker, signal_date, tier_level, composite_score,
                       entry_price, outcome, outcome_pct, holding_days
                FROM signals_history
                WHERE signal_date >= ? AND is_selected = 1
                ORDER BY signal_date DESC, composite_score DESC
            """, (cutoff,)).fetchall()
        cols = ["ticker", "signal_date", "tier_level", "composite_score",
                "entry_price", "outcome", "outcome_pct", "holding_days"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error(f"query_eod_top3失败: {e}")
        return []


def _format_row(r: dict) -> str:
    """单只股票的三行展示：标题 / 状态 / 区间高低点。"""
    name = wdb.get_company_name(r["ticker"]) or ""
    head = (f"🏆 {r['ticker']} {name} [{r.get('tier_level', '?')}]  "
            f"入选日:{r['signal_date']}  入场价:${r.get('entry_price')}")

    stats = get_price_stats_since(r["ticker"], r["signal_date"])
    is_resolved = bool(r["outcome"]) and r["outcome"] != "PENDING"

    # 状态行：已平仓读screener.py算好的outcome；PENDING用区间数据里
    # 最后一根收盘价算浮盈浮亏
    if is_resolved:
        pct = r.get("outcome_pct")
        pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"
        status_line = (f"   状态: 已{r['outcome']} {pct_str}"
                        f"（持有{r.get('holding_days', '?')}个交易日）")
    else:
        entry = r.get("entry_price")
        if stats and entry:
            cur = stats["last_close"]
            pct = round((cur / entry - 1) * 100, 2)
            status_line = (f"   状态: PENDING  现价:${cur} ({pct:+.2f}%，"
                            f"截至{stats['last_date']})")
        else:
            status_line = "   状态: PENDING  现价查询失败"

    # 区间高低点行：已平仓的额外标注"含离场后走势"，避免误读
    if stats:
        range_note = "（含离场后走势，仅供参考）" if is_resolved else ""
        range_line = (f"   区间最高:${stats['high']}（{stats['high_date']}）  "
                       f"区间最低:${stats['low']}（{stats['low_date']}）{range_note}")
    else:
        range_line = "   区间高低点查询失败"

    return f"{head}\n{status_line}\n{range_line}"


def build_eod_section(rows: list) -> str:
    if not rows:
        return f"过去{REVIEW_DAYS}天无Top3入选记录，或数据库暂不可用。"

    lines = [f"📊 EOD Top3选股表现（共{len(rows)}只）\n"]

    resolved_count = 0
    for i, r in enumerate(rows):
        if r["outcome"] and r["outcome"] != "PENDING":
            resolved_count += 1
        lines.append(_format_row(r))
        lines.append("")
        # 轻微限速，避免连续密集请求yfinance被限流（30天*3只/天，
        # 最多可能60+次请求，这里牺牲一点总耗时换稳定性）
        if i < len(rows) - 1:
            time.sleep(0.3)

    pending_count = len(rows) - resolved_count
    lines.append(f"（已平仓{resolved_count}只 / 进行中{pending_count}只）")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 4. 主流程
# ════════════════════════════════════════════════════════════

def build_report() -> str:
    today = date.today().isoformat()
    since = (date.today() - timedelta(days=REVIEW_DAYS)).isoformat()

    rows = query_eod_top3(REVIEW_DAYS)

    lines = [
        f"月报：Top3选股表现 ({since} ~ {today})",
        "=" * 40,
        "",
        build_eod_section(rows),
        "",
        "=" * 40,
        "说明：",
        "1. 「区间最高/最低」统计的是入选当天到今天的日线区间；",
        "   已平仓的股票该区间可能包含离场后的走势，不代表实际",
        "   持仓期间表现。",
        "2. 这是回顾快照，不是严格胜率统计——PENDING的浮盈浮亏",
        "   只是当前状态，不是最终结果。",
    ]
    return "\n".join(lines)


def main() -> None:
    log.info(f"=== weekly_review.py 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ===")
    try:
        wdb.init_watchlist_db()
        report = build_report()

        os.makedirs(_REPORTS_DIR, exist_ok=True)
        today = date.today().isoformat()
        file_path = os.path.join(_REPORTS_DIR, f"weekly_review_{today}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report)

        since = (date.today() - timedelta(days=REVIEW_DAYS)).isoformat()
        caption = f"月报 Top3选股表现 {since} ~ {today}"
        send_telegram_document(file_path, caption=caption)

    except Exception as e:
        log.error(f"weekly_review.py 执行失败: {e}", exc_info=True)
        send_telegram_text(f"weekly_review.py 执行失败: {e}")

    log.info("=== weekly_review.py 完成 ===")


if __name__ == "__main__":
    main()
