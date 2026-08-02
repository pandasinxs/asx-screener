# ============================================================
# ASX SYSTEM — weekly_review.py  v4
#
# 月报：只看EOD screener.py选出的Top3表现，不含落选候选和
# intraday信号。
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
# v3 → v4 变更：
#   - 不再单独生成/推送原始report.txt——完整report现在只以
#     "内嵌在两份prompt txt里"的形式存在，Telegram每周只收到
#     两个文件（X版 + 小红书版），而不是三个
#   - Telegram消息本体改发一段代码算好的精简结论（去重股票数、
#     已平仓胜率、平均收益、最大盈亏），2-3行，不再是完整report
#   - X_PROMPT_TEMPLATE / XHS_PROMPT_TEMPLATE 两份prompt模板直接
#     写死在这个文件里，不再放在外部social_media_prompts.md让
#     脚本运行时读取解析——单文件部署，改语气/结构直接改这两个
#     常量就行，不用担心两个文件不同步，也不用管路径配置
#   - main()里rows/stats只查/算一次，短结论和两份prompt共用同一份，
#     避免重复查库、重复对70+只票发yfinance请求（v3那版如果直接
#     加一次独立的"短结论"计算，会让请求量翻倍，这里提前避开了）
#
# 触发方式不变：
#   1) crontab周六自动跑一次
#   2) bot.py的/weekly命令通过run_script()调用
#
# 明确不做的事：
#   - 不判定跨月度的"最终胜率"趋势（意义不大，样本本身有右侧
#     截断偏差，参见说明区块里的resolve窗口说明）
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


def compute_summary_stats(rows: list) -> dict:
    """
    去重＋汇总统计，在这里用Python确定性算好，不依赖后续任何
    人工或LLM在生成文案时自己去重/求平均——37笔outcome_pct的
    平均值，人心算或LLM心算都很容易算错个零点几个百分点，而且
    这种错误报告本身和文案结构看起来都完全正常，属于典型的
    "silent wrong number"，比脚本直接崩溃更难发现、也更容易
    在发出去之后才被读者挑出来。

    返回None的字段表示样本不足（比如没有任何已平仓记录），
    调用方要处理这个情况。
    """
    distinct_tickers = len(set(r["ticker"] for r in rows))
    total_signals = len(rows)

    resolved = [r for r in rows if r["outcome"] and r["outcome"] != "PENDING"]
    pending_count = total_signals - len(resolved)

    win = [r for r in resolved if r["outcome"] == "WIN"]
    loss = [r for r in resolved if r["outcome"] == "LOSS"]
    timeout = [r for r in resolved if r["outcome"] == "TIMEOUT"]

    pct_values = [r["outcome_pct"] for r in resolved if r.get("outcome_pct") is not None]
    avg_pct = round(sum(pct_values) / len(pct_values), 2) if pct_values else None
    win_rate = round(len(win) / len(resolved) * 100, 1) if resolved else None

    valid = [r for r in resolved if r.get("outcome_pct") is not None]
    best = max(valid, key=lambda r: r["outcome_pct"]) if valid else None
    worst = min(valid, key=lambda r: r["outcome_pct"]) if valid else None

    return {
        "distinct_tickers": distinct_tickers,
        "total_signals": total_signals,
        "resolved_count": len(resolved),
        "pending_count": pending_count,
        "win_count": len(win),
        "loss_count": len(loss),
        "timeout_count": len(timeout),
        "win_rate": win_rate,
        "avg_pct": avg_pct,
        "best": best,
        "worst": worst,
    }


def build_summary_block(stats: dict) -> str:
    """
    渲染成文本区块，放在报告最前面。后续喂给LLM生成社媒文案的
    prompt要求直接抄这里的数字，不允许重新求和/心算。
    """
    if stats["resolved_count"] == 0:
        return (f"去重股票数:{stats['distinct_tickers']}  总信号数:{stats['total_signals']}\n"
                f"本期尚无已平仓记录，暂不计算胜率/平均收益。")

    lines = [
        f"去重股票数:{stats['distinct_tickers']}   总信号数:{stats['total_signals']}",
        f"已平仓:{stats['resolved_count']}笔（WIN {stats['win_count']} / "
        f"LOSS {stats['loss_count']} / TIMEOUT {stats['timeout_count']}）"
        f"   进行中:{stats['pending_count']}笔",
        f"胜率:{stats['win_rate']}%   平均收益(已平仓):{stats['avg_pct']:+.2f}%",
    ]
    if stats["best"] is not None:
        b = stats["best"]
        lines.append(f"最大单笔盈利:{b['outcome_pct']:+.1f}%（{b['ticker']}，{b['signal_date']}入选）")
    if stats["worst"] is not None:
        w = stats["worst"]
        lines.append(f"最大单笔亏损:{w['outcome_pct']:+.1f}%（{w['ticker']}，{w['signal_date']}入选）")
    return "\n".join(lines)


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

    lines = [f"📊 EOD Top3选股表现（共{len(rows)}笔信号，逐笔明细如下）\n"]

    for i, r in enumerate(rows):
        lines.append(_format_row(r))
        lines.append("")
        # 轻微限速，避免连续密集请求yfinance被限流（30天*3只/天，
        # 最多可能60+次请求，这里牺牲一点总耗时换稳定性）
        if i < len(rows) - 1:
            time.sleep(0.3)

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 4. 主流程
# ════════════════════════════════════════════════════════════

def build_report(rows: list, stats: dict) -> str:
    """
    组装完整report文本。rows/stats必须由调用方先查/算好再传进来
    （query_eod_top3 + compute_summary_stats），不在这里重新查——
    这个report的内容后面要塞进两份prompt里，如果每次都重新查一遍
    数据库、重新对70+只票发yfinance请求，一次月报的请求量会直接
    翻倍。
    """
    today = date.today().isoformat()
    since = (date.today() - timedelta(days=REVIEW_DAYS)).isoformat()

    lines = [
        f"月报：Top3选股表现 ({since} ~ {today})",
        "=" * 40,
        "",
        "📈 数据汇总（已用代码算好，写文案时直接引用，不要重新求和/心算）",
        build_summary_block(stats),
        "",
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
        "3. 入选日期越接近本报告右边界的信号，越大概率还是",
        "   PENDING——这是因为screener.py固定的20个交易日resolve",
        "   窗口还没到期，不是系统卡住了，属于正常的右侧截断。",
    ]
    return "\n".join(lines)


def build_short_conclusion(stats: dict, since: str, today: str) -> str:
    """
    发到Telegram消息本体的精简结论，2-3行。完整数据不在这里，
    只存在于两份prompt txt里（内嵌了完整report），自己要看明细
    直接翻那两个文件。
    """
    header = f"月报 Top3选股表现 {since} ~ {today}"
    if stats["resolved_count"] == 0:
        return (f"{header}\n去重{stats['distinct_tickers']}只 / "
                f"{stats['total_signals']}次信号，本期尚无已平仓记录。")

    line2 = (f"已平仓{stats['resolved_count']}笔"
              f"（{stats['win_count']}胜/{stats['loss_count']}负/"
              f"{stats['timeout_count']}超时），胜率{stats['win_rate']}%，"
              f"平均收益{stats['avg_pct']:+.2f}%")

    parts = []
    if stats["best"] is not None:
        b = stats["best"]
        parts.append(f"最大盈利{b['outcome_pct']:+.1f}%（{b['ticker']}）")
    if stats["worst"] is not None:
        w = stats["worst"]
        parts.append(f"最大亏损{w['outcome_pct']:+.1f}%（{w['ticker']}）")

    lines = [header, line2]
    if parts:
        lines.append("／".join(parts))
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 5. 社媒文案prompt（模板直接写在代码里，单文件部署，
#    改语气/结构直接改这两个常量，不用管外部文件路径）
# ════════════════════════════════════════════════════════════

X_PROMPT_TEMPLATE = """You are helping me draft an X (Twitter) thread summarizing my personal
ASX quantitative trading system's performance for the trailing period
covered in the report below. Use ONLY the numbers in that report — do
not estimate, round generously, or invent figures.

INPUT:
[PASTE REPORT HERE]

The report begins with a "📈 数据汇总" block containing pre-computed,
code-verified numbers: distinct ticker count, total signal count,
resolved/pending split, WIN/LOSS/TIMEOUT breakdown, win rate, average
return, and best/worst trade. These numbers were computed
deterministically in Python, not by you.

Structure the thread into three labeled parts — 概括/汇总 (overview),
典型上涨案例 (case study), 结论/复盘 (conclusion & retrospective) —
as 7-9 tweets total.

PART 1 — 概括/汇总:
1. USE THE NUMBERS IN THE "📈 数据汇总" BLOCK VERBATIM for the headline
   stats (distinct stock count, total signals, resolved/pending
   counts, win/loss/timeout breakdown, win rate, average return).
   Do NOT recompute these by re-adding or re-averaging the per-stock
   rows yourself — manually summing dozens of percentages is exactly
   the kind of arithmetic LLMs get subtly wrong (off by a few tenths
   of a percent) without any obvious sign something's off. If the
   summary block is missing or looks incomplete, say so instead of
   estimating.
2. Note the distinct stock count vs total signal count, and that
   repeated tickers reflect the persistence factor in the scoring
   model, not N separate new discoveries.
3. Include one line quantifying risk control: the range of losses
   among LOSS trades (e.g., "losses ranged from -X% to -Y%, no
   single trade blew past the stop") — the actual evidence the
   system's risk framework is working.

PART 2 — 典型上涨案例:
4. Use the trade with the highest POSITIVE outcome_pct (the "best"
   field in the summary block, only if it's actually positive) as a
   single case study — ticker, entry date, holding period, return.
   Explicitly caveat it as one example out of N distinct stocks, not
   representative of the period's overall result. If no trade in the
   report has a positive outcome_pct, do NOT force one — say plainly
   that no winning trade closed this period, and name the smallest
   loss instead without dressing it up as a win.

PART 3 — 结论/复盘:
5. Write a genuine retrospective grounded in the actual per-stock
   rows — not generic "will keep optimizing" filler. Look for a
   concrete pattern (e.g., the same ticker losing multiple times in
   a row, a cluster of losses in the same week) and name it
   specifically, using real tickers/dates from the report.
6. Optional: if any single tier tag (e.g. [T2], [T3]) has ≥3 resolved
   trades this period and its win rate/avg return looks notably
   different from the others, mention it as something to watch —
   explicitly frame it as a small-sample observation, not a
   conclusion. Skip this if no tier clears the ≥3 threshold or
   nothing stands out; don't manufacture a pattern.
7. One tweet on what's still pending / what I'm watching next.
8. Final tweet: plain disclaimer — personal system log, not
   investment advice, past performance not predictive.

TONE: matter-of-fact, slightly self-deprecating about losses, no
hype language, minimal emoji (📊 optional on tweet 1 only).
Fintwit/quant readers distrust vague claims — every number must be
traceable back to the input report.

Write the output in English. Output only the numbered thread,
nothing else.
"""

XHS_PROMPT_TEMPLATE = """You are helping me draft a Xiaohongshu post about my personal ASX
quant trading system, covering the period in the report below. Use
ONLY the numbers from that report.

INPUT:
[PASTE REPORT HERE]

The report begins with a "📈 数据汇总" block containing pre-computed,
code-verified numbers: distinct ticker count, total signal count,
resolved/pending split, WIN/LOSS/TIMEOUT breakdown, win rate, average
return, and best/worst trade. These numbers were computed
deterministically in Python, not by you.

Structure the post into three labeled parts — 概括/汇总、典型上涨案例、
结论/复盘：

PART 1 — 概括/汇总:
1. USE THE NUMBERS IN THE "📈 数据汇总" BLOCK VERBATIM. Do NOT
   recompute distinct stock count, win/loss/timeout counts, win
   rate, or average return by re-adding or re-averaging the
   per-stock rows yourself — manually summing dozens of percentages
   is exactly the kind of arithmetic LLMs get subtly wrong (off by a
   few tenths of a percent) without any obvious sign something's
   off. If the summary block is missing or looks incomplete, say so
   instead of estimating.
2. Include one short mention of risk control — the range of losses,
   to show stops are functioning, not catastrophic.

PART 2 — 典型上涨案例 ("案例卡片"):
3. Use the trade with the highest POSITIVE outcome_pct (the "best"
   field in the summary block, only if it's actually positive):
   ticker, entry date, holding period, return, 2-3 sentences.
   Explicitly label it as a single example, not the month's overall
   result. If no trade has a positive outcome_pct, do NOT force a
   winning story — say so honestly instead.

PART 3 — 结论/复盘:
4. Write a genuine retrospective grounded in the actual per-stock
   rows — not generic "继续优化" filler. Point to something concrete
   (a ticker that lost repeatedly, a cluster of losses in the same
   week) using real tickers/dates from the report, and reflect on it
   honestly.
5. Optional: if any single tier tag (e.g. [T2], [T3]) has ≥3 resolved
   trades this period and stands out from the others, mention it as
   something to watch — explicitly frame it as a small-sample
   observation, not a conclusion. Skip if nothing clears that bar.
6. If the same ticker produced more than one WIN/LOSS during what
   looks like the same underlying price move, treat it as one story
   beat when discussing repeated losers, not multiple separate
   failures.

ALSO INCLUDE:
7. A simple accompanying image layout suggestion: a small stats
   table (已平仓 / 胜率 / 平均收益 / 最大盈利 / 最大亏损) using the
   exact numbers from the summary block, described in words
   (rows/columns) so I can build it myself — do not fabricate a
   chart or invent numbers not in the report.
8. Tone: first-person, reflective, "记录/踩坑" style rather than
   "晒收益" style — small imperfections and honesty read better on
   this platform than a highlight reel.
9. Structure: short opening line (1-2 sentences), then the three
   parts above as short paragraphs (≤3 sentences each) with line
   breaks between them (小红书 reading style — no dense text walls).
   End with a plain one-line disclaimer.
10. Length: roughly 300-500 Chinese characters for the post body,
    excluding the image layout description.
11. Write the output in Simplified Chinese.

Output only the post text, then the image layout description,
nothing else.
"""


def build_social_prompts(report: str) -> dict:
    """把report内容塞进两份prompt模板的占位符，返回可以直接
    复制粘贴发给LLM的完整文本。"""
    return {
        "x": X_PROMPT_TEMPLATE.replace("[PASTE REPORT HERE]", report),
        "xhs": XHS_PROMPT_TEMPLATE.replace("[PASTE REPORT HERE]", report),
    }


def main() -> None:
    log.info(f"=== weekly_review.py 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ===")
    try:
        wdb.init_watchlist_db()

        # rows/stats只查/算一次，短结论和两份prompt共用，避免重复
        # 查库、重复对70+只票发yfinance请求
        rows = query_eod_top3(REVIEW_DAYS)
        stats = compute_summary_stats(rows)
        report = build_report(rows, stats)

        today = date.today().isoformat()
        since = (date.today() - timedelta(days=REVIEW_DAYS)).isoformat()

        # 1) 精简结论直接发Telegram文本
        send_telegram_text(build_short_conclusion(stats, since, today))

        # 2) X / 小红书 prompt（已嵌入本周完整数据，复制粘贴直接发给LLM）
        os.makedirs(_REPORTS_DIR, exist_ok=True)
        prompts = build_social_prompts(report)

        x_path = os.path.join(_REPORTS_DIR, f"weekly_review_{today}_x_prompt.txt")
        with open(x_path, "w", encoding="utf-8") as f:
            f.write(prompts["x"])
        send_telegram_document(x_path, caption="X thread prompt（已嵌入本周数据，直接复制粘贴发给LLM）")

        xhs_path = os.path.join(_REPORTS_DIR, f"weekly_review_{today}_xhs_prompt.txt")
        with open(xhs_path, "w", encoding="utf-8") as f:
            f.write(prompts["xhs"])
        send_telegram_document(xhs_path, caption="小红书 prompt（已嵌入本周数据，直接复制粘贴发给LLM）")

    except Exception as e:
        log.error(f"weekly_review.py 执行失败: {e}", exc_info=True)
        send_telegram_text(f"weekly_review.py 执行失败: {e}")

    log.info("=== weekly_review.py 完成 ===")


if __name__ == "__main__":
    main()
