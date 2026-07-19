# ============================================================
# ASX SYSTEM — weekly_review.py  v1
#
# 每周复盘：回顾过去7天的两块东西，给一个"直观感受"，
# 不追求严格统计意义上的胜率（大部分信号根本没到resolve的时间）。
#
#   1) EOD选股表现（screener.py写入的signals_history，Top10候选池全部）
#      - 已经resolve成WIN/LOSS/TIMEOUT的，直接读screener.py自己
#        算好的outcome/outcome_pct（不重复造轮子）
#      - 还PENDING的，现查一次实时价，算个浮盈浮亏
#
#   2) intraday_monitor.py盘中信号表现（v4新增的intraday_signals_log，
#      追加式历史记录）
#      - 用触发信号以来的日线数据，检查有没有碰过止损/目标价
#        （不是简单看当前价，会漏掉"曾经触发过目标/止损又走回来"的情况）
#      - 都没碰到的，现查一次实时价，算个浮盈浮亏
#
# 触发方式：
#   1) crontab周六自动跑一次，推送到Telegram
#   2) bot.py的/weekly命令通过run_script()调用（跟/eod、/morning一样，
#      命令本身不处理结果，本脚本自己发Telegram）
#
# 明确不做的事：
#   - 不判定"最终胜率"（意义不大，样本大部分还没跑完）
#   - 不给intraday_monitor.py的信号建立WIN/LOSS/TIMEOUT这种持久化
#     结果记录（那是screener.py已经给EOD候选做的一整套东西，
#     如果以后要给四个模式也做同样严谨的回测，是单独的项目）
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

REVIEW_DAYS = 7


# ════════════════════════════════════════════════════════════
# 1. Telegram
# ════════════════════════════════════════════════════════════

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chunk in [text[i:i + 4000] for i in range(0, len(text), 4000)]:
        try:
            r = requests.post(url, json={
                "chat_id": CHAT_ID, "text": chunk,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=10)
            r.raise_for_status()
        except Exception as e:
            log.error(f"Telegram推送失败: {e}")
        time.sleep(0.5)


# ════════════════════════════════════════════════════════════
# 2. 价格 & 历史数据
# ════════════════════════════════════════════════════════════

def get_current_price(ticker: str, retries: int = 3) -> float:
    """现价查询，跟bot.py的get_stock_price同样的重试模式（各脚本独立
    实现一份，不跨文件共享，跟这个代码库现有风格一致）"""
    for attempt in range(1, retries + 1):
        try:
            fi = yf.Ticker(ticker).fast_info
            price = fi.last_price
            if price is not None:
                return round(float(price), 4)
        except Exception as e:
            if attempt < retries:
                time.sleep(1.0 * attempt)
            else:
                log.warning(f"get_current_price失败 [{ticker}]: {e}")
    return None


def get_daily_history_since(ticker: str, since_date: str) -> pd.DataFrame:
    """
    下载从since_date（不含）到今天的日线数据，用于检查intraday_monitor.py
    的信号触发后有没有碰过止损/目标价。period="3mo"留足够余量，
    覆盖REVIEW_DAYS=7天信号 + 万一signal_date刚好在3个月边界的情况。
    """
    try:
        df = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        idx = df.index.astype(str).str[:10]
        df = df.set_axis(idx)
        return df[df.index > since_date]
    except Exception as e:
        log.warning(f"get_daily_history_since失败 [{ticker}]: {e}")
        return None


# ════════════════════════════════════════════════════════════
# 3. EOD选股表现（signals_history，screener.py维护）
# ════════════════════════════════════════════════════════════

def query_eod_candidates(days: int = REVIEW_DAYS) -> list:
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
                       is_selected, entry_price, outcome, outcome_pct, holding_days
                FROM signals_history
                WHERE signal_date >= ?
                ORDER BY is_selected DESC, composite_score DESC
            """, (cutoff,)).fetchall()
        cols = ["ticker", "signal_date", "tier_level", "composite_score",
                "is_selected", "entry_price", "outcome", "outcome_pct", "holding_days"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error(f"query_eod_candidates失败: {e}")
        return []


def build_eod_section(rows: list) -> str:
    if not rows:
        return "📊 <b>EOD选股表现</b>\n\n（过去7天无新增候选，或数据库暂不可用）"

    lines = [f"📊 <b>EOD选股表现</b>（Top10候选池，共{len(rows)}只）\n"]

    top3 = [r for r in rows if r["is_selected"]]
    rest = [r for r in rows if not r["is_selected"]]

    def _fmt_row(r: dict) -> str:
        name = wdb.get_company_name(r["ticker"]) or ""
        head = f"<b>{r['ticker']}</b> {name} [{r.get('tier_level','?')}] {r['signal_date']}入选"
        if r["outcome"] and r["outcome"] != "PENDING":
            pct = r.get("outcome_pct")
            pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"
            return f"  {head}\n    已{r['outcome']}：{pct_str}（持有{r.get('holding_days','?')}个交易日）"
        else:
            entry = r.get("entry_price")
            cur = get_current_price(r["ticker"])
            if entry and cur:
                pct = round((cur / entry - 1) * 100, 2)
                return f"  {head}\n    PENDING：入场${entry} → 现价${cur} ({pct:+.2f}%)"
            else:
                return f"  {head}\n    PENDING：入场${entry}，现价查询失败"

    if top3:
        lines.append("🏆 <b>Top3入选：</b>")
        for r in top3:
            lines.append(_fmt_row(r))
    if rest:
        lines.append("\n📋 <b>落选候选（T1-T4筛选出但未进Top3）：</b>")
        for r in rest:
            lines.append(_fmt_row(r))

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 4. 盘中信号表现（intraday_signals_log，intraday_monitor.py维护）
# ════════════════════════════════════════════════════════════

def build_intraday_section(rows: list) -> str:
    if not rows:
        return "🔔 <b>盘中信号表现</b>\n\n（过去7天intraday_monitor.py未触发任何信号）"

    lines = [f"🔔 <b>盘中信号表现</b>（模式1-4，共{len(rows)}条）\n"]

    for r in rows:
        name  = r.get("company_name") or ""
        head  = (f"<b>{r['ticker']}</b> {name} {r['mode']} "
                 f"{r['signal_time'][:16].replace('T',' ')}")
        entry = r.get("price")
        stop  = r.get("stop_loss")
        target = r.get("target_1r")

        hist = get_daily_history_since(r["ticker"], r["signal_date"])
        status_note = None
        if hist is not None and not hist.empty and stop is not None:
            low_min = float(hist["Low"].min())
            if low_min <= stop:
                status_note = f"⚠️ 已触及止损区间（期间最低${low_min:.3f} ≤ 止损${stop:.3f}）"
        if status_note is None and hist is not None and not hist.empty and target is not None:
            high_max = float(hist["High"].max())
            if high_max >= target:
                status_note = f"✅ 已触及目标价（期间最高${high_max:.3f} ≥ 目标${target:.3f}）"

        if status_note is None:
            cur = get_current_price(r["ticker"])
            if entry and cur:
                pct = round((cur / entry - 1) * 100, 2)
                status_note = f"触发价${entry} → 现价${cur} ({pct:+.2f}%)，止损${stop}"
                if target:
                    status_note += f" 目标${target}"
            else:
                status_note = f"触发价${entry}，现价查询失败"

        lines.append(f"  {head}\n    {status_note}")

    lines.append(
        "\n⚠️ 「已触及止损/目标」是看触发以来的日线最高低点，"
        "「未触及」的走势用现价做快照，不是逐笔精确回放。"
    )
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 5. 主流程
# ════════════════════════════════════════════════════════════

def build_report() -> str:
    today = date.today().isoformat()
    since = (date.today() - timedelta(days=REVIEW_DAYS)).isoformat()

    eod_rows      = query_eod_candidates(REVIEW_DAYS)
    intraday_rows = wdb.get_recent_signal_log(REVIEW_DAYS)

    lines = [
        f"📅 <b>周报：过去7天回顾</b> ({since} ~ {today})",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        build_eod_section(eod_rows),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        build_intraday_section(intraday_rows),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "⚠️ 这是「直观感受」快照，不是严格胜率统计——大部分信号还没到"
        "screener.py的20个交易日resolve窗口，PENDING/浮盈浮亏都只是"
        "当前状态，不是最终结果。",
    ]
    return "\n".join(lines)


def main() -> None:
    log.info(f"=== weekly_review.py 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ===")
    wdb.init_watchlist_db()
    report = build_report()
    send_telegram(report)
    log.info("=== weekly_review.py 完成 ===")


if __name__ == "__main__":
    main()
