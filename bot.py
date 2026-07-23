# ============================================================
# ASX TRADING BOT v3
# 新API：asx.api.markitdigital.com
# 修复：yfinance新闻字段 content.title
# v3.1：新增 /backtest 命令，查询signals_history回测统计
# v3.2：新增 /htbt 命令，查询backtest_engine.py的历史模拟回测结果
#       ——注意区分：
#         /backtest → announcements.db 的 signals_history 表
#                     每天screener.py实际选出的信号，前向追踪的真实结果，
#                     反映"实盘系统目前跑得怎么样"
#         /htbt     → backtest_results.db 的 signals_history_backtest 表
#                     backtest_engine.py离线跑出来的历史模拟结果，
#                     跟真实交易完全无关，只用来判断"改这个参数会不会
#                     让系统更好"这个调参问题。两个数据库、两张表、
#                     两套统计逻辑，完全独立，互不影响。
# ============================================================

import os, subprocess, logging, re, time, sqlite3, html, csv
import yfinance as yf
import requests
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict
from telegram import Update
from telegram.ext import (Application, CommandHandler,
                          MessageHandler, filters, ContextTypes)
from google import genai

import watchlist_db as wdb

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/home/ubuntu/logs/bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

gemini_client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None
AEST = timezone(timedelta(hours=10))

# signals_history和announcements在同一个DB（实盘前向追踪数据）
ANN_DB_PATH = "/home/ubuntu/asx/announcements.db"

# backtest_engine.py产出的历史模拟回测数据库（离线调参用，与实盘数据完全独立）
HTBT_DB_PATH = "/home/ubuntu/asx/backtest_results.db"

ASX_ANN_URL = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/json',
    'Referer': 'https://www.asx.com.au'
}

SYSTEM = """你是一位严谨的ASX专业股票分析师助手。

【核心原则】
1. 准确性优先：只陈述可验证的事实。无法确认的信息必须标注"未经证实"或"需自行核查"。
2. 禁止捏造：不编造数据、价格、公告内容或分析结论。数据来源不明时直接说"暂无数据"。
3. 自我验证：给出任何观点前，先内部自问"真实情况是不是这样？有没有相反证据？"，确认逻辑成立再输出。
4. 逻辑严密：每个结论必须有依据支撑，不做跳跃性推断。前提不充分时说明结论的局限性。
5. 风险优先：分析交易机会时，先陈述风险，再陈述机会。

【用户策略】
- EOD波段：收盘后扫描技术突破形态，持仓数天到数周
- First Pullback：开盘有公告催化，等回踩VWAP入场，持仓1-2天

【输出格式】简洁中文，100-200字。结论不确定时明确说"不确定"。"""

# ── 工具函数 ──────────────────────────────────────────────────
def auth(update: Update) -> bool:
    return update.effective_chat.id == CHAT_ID

def ask_gemini(prompt: str) -> str:
    if not gemini_client:
        return ""
    try:
        r = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return r.text.strip()
    except Exception as e:
        if '429' in str(e):
            print("Gemini限速，跳过AI分析")
            return ""
        return ""

def get_stock_announcements(code: str) -> list:
    code  = code.upper().replace('.AX', '')
    today = date.today().isoformat()
    items = []
    page  = 0
    while len(items) < 10:
        try:
            r = requests.get(ASX_ANN_URL,
                             params={'itemsPerPage': 100, 'page': page},
                             headers=ASX_HEADERS, timeout=10)
            all_items = r.json().get('data', {}).get('items', [])
            if not all_items: break
            for item in all_items:
                if item.get('symbol', '') == code:
                    items.append({
                        'date'     : item.get('date', '')[:10],
                        'headline' : item.get('headline', '')[:70],
                        'sensitive': item.get('isPriceSensitive', False)
                    })
                if len(items) >= 5: break
            if all_items and all_items[-1].get('date','')[:10] < (
                    date.today().isoformat()[:8] + '01'):
                break
            if len(all_items) < 100: break
            page += 1
            time.sleep(0.3)
        except: break
    return items[:5]

def get_yf_news(code: str) -> list:
    try:
        stock  = yf.Ticker(f"{code.upper().replace('.AX','')}.AX")
        today  = date.today().isoformat()
        result = []
        for n in (stock.news or [])[:8]:
            content = n.get('content', {})
            title   = content.get('title', '')
            pub     = content.get('pubDate', '')[:10]
            if title:
                result.append({
                    'title' : title,
                    'date'  : pub,
                    'today' : pub == today,
                    'source': content.get('provider', {}).get('displayName', '')
                })
        result.sort(key=lambda x: x['today'], reverse=True)
        return result
    except:
        return []

def get_stock_price(code: str, retries: int = 3) -> dict:
    ticker = f"{code.upper().replace('.AX','')}.AX"
    for attempt in range(1, retries + 1):
        try:
            fi = yf.Ticker(ticker).fast_info
            price = fi.last_price
            if price is None:
                raise ValueError("last_price为空")
            prev_close = fi.previous_close
            if prev_close and float(prev_close) != 0:
                change = round((float(price) / float(prev_close) - 1) * 100, 2)
            else:
                change = 0.0
                log.warning(f"get_stock_price [{ticker}]: previous_close无效({prev_close})，change设为0")
            return {'price': round(float(price), 3), 'change': change}
        except Exception as e:
            if attempt < retries:
                log.warning(f"get_stock_price重试 [{ticker}] {attempt}/{retries}: {e}")
                time.sleep(1.5 * attempt)
            else:
                log.error(f"get_stock_price最终失败 [{ticker}]：{e}")
    return {}

def format_stock_info(code: str, anns: list, news: list, price: dict) -> str:
    lines = [f"📊 <b>{code.upper().replace('.AX','')}.AX</b>"]
    if price:
        chg = f" ({'+' if price.get('change',0)>=0 else ''}{price.get('change',0)}%)"
        lines.append(f"💰 现价：${price.get('price','N/A')}{chg}")
    if anns:
        lines.append("\n📋 <b>ASX最近公告：</b>")
        for a in anns:
            flag = "⭐ " if a['sensitive'] else ""
            lines.append(f"  {flag}{a['date']}  {a['headline']}")
    if news:
        lines.append("\n📰 <b>近期新闻：</b>")
        for n in news[:4]:
            today_flag = "🔴 今日 " if n['today'] else ""
            lines.append(f"  • {today_flag}{n['title']}")
    return "\n".join(lines)

# ── 回测查询函数（实盘前向追踪：announcements.db / signals_history）──────

def _query_backtest(mode: str) -> str:
    """
    从signals_history查询回测统计。
    mode: "overall" | "tier" | "catalyst"
    返回格式化文本，供Telegram发送。

    注意：这里查的是实盘信号的前向追踪结果（screener.py每天真实选出的
    信号，记录到announcements.db后逐日更新outcome），不是历史模拟。
    历史模拟调参请用 /htbt 命令（查backtest_results.db，完全独立的数据源）。
    """
    if not os.path.exists(ANN_DB_PATH):
        return "⚠️ 暂无回测数据，signals_history数据库尚未创建。\n请确认screener.py v15已运行至少一次。"

    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:

            # ── 先检查表是否存在 + 记录总数 ──────────────────────
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if "signals_history" not in tables:
                return "⚠️ signals_history表尚未创建，请确认screener.py v15已运行至少一次。"

            total_all = conn.execute(
                "SELECT COUNT(*) FROM signals_history"
            ).fetchone()[0]
            total_pending = conn.execute(
                "SELECT COUNT(*) FROM signals_history WHERE outcome='PENDING'"
            ).fetchone()[0]
            total_done = total_all - total_pending

            if total_all == 0:
                return "📊 signals_history暂无记录，等待screener.py运行后积累数据。"

            if total_done == 0:
                return (
                    f"📊 回测数据积累中\n\n"
                    f"总记录：{total_all} 条\n"
                    f"PENDING（持仓中）：{total_pending} 条\n"
                    f"已结算：0 条\n\n"
                    f"⏳ 信号尚未触发止盈/止损/超时，等待更多交易日积累结果。"
                )

            # ── 期望值计算函数 ────────────────────────────────────
            def calc_ev(rows):
                """rows: [(outcome, outcome_pct), ...]"""
                wins    = [r[1] for r in rows if r[0] == "WIN"     and r[1] is not None]
                losses  = [r[1] for r in rows if r[0] == "LOSS"    and r[1] is not None]
                timeouts= [r[1] for r in rows if r[0] == "TIMEOUT" and r[1] is not None]
                n       = len(wins) + len(losses) + len(timeouts)
                if n == 0:
                    return None, None, None, None, 0
                win_rate   = round(len(wins) / n * 100, 1)
                avg_win    = round(sum(wins)    / len(wins),    2) if wins    else 0
                avg_loss   = round(sum(losses)  / len(losses),  2) if losses  else 0
                ev         = round(
                    (len(wins) / n * avg_win) + (len(losses) / n * avg_loss), 2
                ) if wins or losses else None
                return win_rate, avg_win, avg_loss, ev, n

            # ════════════════════════════════════════════════════
            if mode == "overall":
                # 整体 + Top3 vs 落选候选 对比
                rows_all = conn.execute("""
                    SELECT outcome, outcome_pct FROM signals_history
                    WHERE outcome != 'PENDING'
                """).fetchall()
                rows_sel = conn.execute("""
                    SELECT outcome, outcome_pct FROM signals_history
                    WHERE outcome != 'PENDING' AND is_selected = 1
                """).fetchall()
                rows_not = conn.execute("""
                    SELECT outcome, outcome_pct FROM signals_history
                    WHERE outcome != 'PENDING' AND is_selected = 0
                """).fetchall()

                # 分布统计
                dist = {}
                for r in rows_all:
                    dist[r[0]] = dist.get(r[0], 0) + 1

                wr_all, aw_all, al_all, ev_all, n_all = calc_ev(rows_all)
                wr_sel, aw_sel, al_sel, ev_sel, n_sel = calc_ev(rows_sel)
                wr_not, aw_not, al_not, ev_not, n_not = calc_ev(rows_not)

                ev_str = f"{ev_all:+.2f}%" if ev_all is not None else "N/A"
                lines  = [
                    f"📊 <b>回测整体统计（实盘前向追踪）</b>",
                    f"总记录：{total_all} 条（含{total_pending}个PENDING）",
                    f"已结算：{total_done} 条",
                    f"",
                    f"<b>结果分布：</b>",
                ]
                for outcome, cnt in sorted(dist.items()):
                    pct = round(cnt / total_done * 100, 1)
                    lines.append(f"  {outcome}: {cnt}条 ({pct}%)")

                lines += [
                    f"",
                    f"<b>整体期望值分析（{n_all}条已结算）：</b>",
                    f"  胜率：{wr_all}%  平均盈利：{aw_all:+.2f}%  平均亏损：{al_all:+.2f}%",
                    f"  期望值：{ev_str}",
                    f"  （期望值>0说明系统长期有正收益）",
                    f"",
                    f"<b>Top3精选 vs 落选候选对比：</b>",
                    f"  Top3（{n_sel}条）：胜率{wr_sel}%  EV:{f'{ev_sel:+.2f}%' if ev_sel is not None else 'N/A'}",
                    f"  落选（{n_not}条）：胜率{wr_not}%  EV:{f'{ev_not:+.2f}%' if ev_not is not None else 'N/A'}",
                ]
                if ev_sel is not None and ev_not is not None:
                    diff = round(ev_sel - ev_not, 2)
                    if diff > 0:
                        lines.append(f"  ✅ Top3评分系统有效：精选比落选高{diff:+.2f}%")
                    elif diff < 0:
                        lines.append(f"  ⚠️ Top3评分系统待改进：精选比落选低{abs(diff):.2f}%")
                    else:
                        lines.append(f"  ➡️ Top3与落选期望值相近，评分区分度不足")

                return "\n".join(lines)

            # ════════════════════════════════════════════════════
            elif mode == "tier":
                rows_by_tier = conn.execute("""
                    SELECT tier_level, outcome, outcome_pct
                    FROM signals_history
                    WHERE outcome != 'PENDING'
                    ORDER BY tier_level
                """).fetchall()

                if not rows_by_tier:
                    return "📊 暂无已结算记录，等待积累数据。"

                # 按层级分组
                tier_rows = defaultdict(list)
                for r in rows_by_tier:
                    tier_rows[r[0]].append((r[1], r[2]))

                lines = [f"📊 <b>回测按层级分组（实盘前向追踪）</b>（已结算{total_done}条）\n"]
                for tier in ["T1", "T2", "T3", "T4"]:
                    if tier not in tier_rows:
                        continue
                    tr    = [(o, p) for o, p in tier_rows[tier]]
                    wr, aw, al, ev, n = calc_ev([(o, p) for o, p in tr])
                    ev_str = f"{ev:+.2f}%" if ev is not None else "N/A"
                    dist_t = {}
                    for o, _ in tr:
                        dist_t[o] = dist_t.get(o, 0) + 1
                    dist_str = " ".join(f"{k}:{v}" for k, v in sorted(dist_t.items()))
                    lines.append(
                        f"<b>{tier}</b>（{n}条）：{dist_str}\n"
                        f"  胜率:{wr}%  盈:{aw:+.2f}%  亏:{al:+.2f}%  EV:{ev_str}"
                    )

                lines.append(f"\n⚠️ 样本量&lt;30条时结论仅供参考，建议积累更多数据后判断。")
                return "\n".join(lines)

            # ════════════════════════════════════════════════════
            elif mode == "catalyst":
                rows_cat = conn.execute("""
                    SELECT
                        CASE WHEN catalyst >= 0.5 THEN '有催化剂' ELSE '无催化剂' END as cat,
                        outcome, outcome_pct
                    FROM signals_history
                    WHERE outcome != 'PENDING'
                """).fetchall()

                if not rows_cat:
                    return "📊 暂无已结算记录，等待积累数据。"

                cat_rows = defaultdict(list)
                for r in rows_cat:
                    cat_rows[r[0]].append((r[1], r[2]))

                lines = [f"📊 <b>回测：催化剂有无对比（实盘前向追踪）</b>（已结算{total_done}条）\n"]
                for cat_name in ["有催化剂", "无催化剂"]:
                    if cat_name not in cat_rows:
                        lines.append(f"<b>{cat_name}</b>：暂无数据")
                        continue
                    tr    = cat_rows[cat_name]
                    wr, aw, al, ev, n = calc_ev(tr)
                    ev_str = f"{ev:+.2f}%" if ev is not None else "N/A"
                    dist_t = {}
                    for o, _ in tr:
                        dist_t[o] = dist_t.get(o, 0) + 1
                    dist_str = " ".join(f"{k}:{v}" for k, v in sorted(dist_t.items()))
                    lines.append(
                        f"<b>{cat_name}</b>（{n}条）：{dist_str}\n"
                        f"  胜率:{wr}%  盈:{aw:+.2f}%  亏:{al:+.2f}%  EV:{ev_str}"
                    )

                # 结论
                ev_with = calc_ev(cat_rows.get("有催化剂", []))[3]
                ev_without = calc_ev(cat_rows.get("无催化剂", []))[3]
                if ev_with is not None and ev_without is not None:
                    lines.append("")
                    diff = round(ev_with - ev_without, 2)
                    if diff > 0:
                        lines.append(f"✅ 催化剂有效：有催化剂比无催化剂期望值高{diff:+.2f}%")
                    elif diff < 0:
                        lines.append(f"⚠️ 催化剂暂无明显优势，差距{abs(diff):.2f}%")
                    else:
                        lines.append(f"➡️ 催化剂有无暂无明显差距")

                lines.append(f"\n⚠️ 样本量&lt;30条时结论仅供参考。")
                return "\n".join(lines)

            else:
                return "未知查询模式"

    except Exception as e:
        log.error(f"_query_backtest失败 [{mode}]: {e}")
        return f"❌ 查询失败：{e}"


# ══════════════════════════════════════════════════════════════
# 历史回测(模拟)查询函数 —— backtest_results.db / signals_history_backtest
#
# 跟上面 _query_backtest() 的关键区别：
#   _query_backtest()  查 announcements.db 的 signals_history
#                      → 实盘每天真实选出的信号，前向追踪结果
#   _query_htbt()      查 backtest_results.db 的 signals_history_backtest
#                      → backtest_engine.py离线跑出来的历史模拟结果，
#                        用来验证"改这个参数会不会更好"，跟实盘交易无关
#
# 数据库用只读模式打开（file:...?mode=ro）——backtest_engine.py可能正在
# VM上通过nohup长时间写入这个db，bot.py这边只应该读，不应该有任何写操作，
# 避免锁冲突，也避免bot意外污染回测数据。
# ══════════════════════════════════════════════════════════════

def _htbt_connect():
    if not os.path.exists(HTBT_DB_PATH):
        return None
    try:
        return sqlite3.connect(f"file:{HTBT_DB_PATH}?mode=ro", uri=True)
    except Exception as e:
        log.error(f"_htbt_connect失败: {e}")
        return None


def _htbt_calc_stats(rows: list) -> dict:
    """rows: [(outcome, outcome_pct), ...] → 胜率/盈亏比等汇总。"""
    n = len(rows)
    if n == 0:
        return {"n": 0, "win_rate": 0.0, "avg_win": 0.0,
                "avg_loss": 0.0, "profit_factor": None}
    wins = sum(1 for o, _ in rows if o == "WIN")
    pcts = [p for _, p in rows if p is not None]
    win_rate = round(wins / n * 100, 1)
    pos = [p for p in pcts if p > 0]
    neg = [p for p in pcts if p < 0]
    avg_win  = round(sum(pos) / len(pos), 2) if pos else 0.0
    avg_loss = round(sum(neg) / len(neg), 2) if neg else 0.0
    gross_profit = sum(pos)
    gross_loss   = abs(sum(neg))
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
    return {"n": n, "win_rate": win_rate, "avg_win": avg_win,
            "avg_loss": avg_loss, "profit_factor": pf}


def _htbt_latest_param_set(conn) -> str:
    """按最后一次运行时间，找到最近跑过的参数集名字。"""
    row = conn.execute("""
        SELECT param_set FROM signals_history_backtest
        GROUP BY param_set ORDER BY MAX(run_timestamp) DESC LIMIT 1
    """).fetchone()
    return row[0] if row else ""


def _query_htbt_leaderboard(conn) -> str:
    rows = conn.execute("""
        SELECT param_set,
               COUNT(*) AS n,
               SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) AS wins,
               AVG(outcome_pct) AS avg_pct,
               MIN(signal_date) AS date_from, MAX(signal_date) AS date_to
        FROM signals_history_backtest
        WHERE outcome != 'PENDING' AND is_selected = 1
        GROUP BY param_set
        ORDER BY (wins * 1.0 / n) DESC
    """).fetchall()
    if not rows:
        return ("📊 历史回测(模拟)暂无已结算数据\n"
                "请先在VM上跑一次 backtest_engine.py")

    lines = ["📊 <b>参数实验排行榜</b>（历史模拟，非实盘）\n"]
    for param_set, n, wins, avg_pct, date_from, date_to in rows:
        wr = round(wins / n * 100, 1) if n else 0
        lines.append(f"<b>{html.escape(param_set)}</b>  {n}笔  胜率{wr}%  平均{avg_pct:+.2f}%")
    return "\n".join(lines)


def _query_htbt_detail(conn, param_set: str) -> str:
    rows_all = conn.execute("""
        SELECT outcome, outcome_pct FROM signals_history_backtest
        WHERE outcome != 'PENDING' AND is_selected = 1 AND param_set = ?
    """, (param_set,)).fetchall()
    if not rows_all:
        return (f"📊 参数集「{html.escape(param_set)}」暂无已结算数据\n"
                f"（检查名字是否打对了，用 /htbt 看排行榜里的实际名字）")

    stats = _htbt_calc_stats(rows_all)
    pf_str = f"{stats['profit_factor']}" if stats["profit_factor"] is not None else "N/A"
    lines = [
        f"📊 <b>[{html.escape(param_set)}]</b>（历史模拟，非实盘）",
        f"样本：{stats['n']}笔（Top3）  胜率：{stats['win_rate']}%",
        f"平均盈/亏：{stats['avg_win']:+.2f}% / {stats['avg_loss']:+.2f}%  盈亏比：{pf_str}",
    ]

    outcome_dist = {}
    for o, _ in rows_all:
        outcome_dist[o] = outcome_dist.get(o, 0) + 1
    outcome_str = "  ".join(f"{oc}:{outcome_dist[oc]}" for oc in ["WIN", "LOSS", "TIMEOUT"] if oc in outcome_dist)
    lines.append(f"出场原因：{outcome_str}")

    tier_rows = conn.execute("""
        SELECT tier_level, outcome, outcome_pct FROM signals_history_backtest
        WHERE outcome != 'PENDING' AND is_selected = 1 AND param_set = ?
    """, (param_set,)).fetchall()
    tier_grouped = defaultdict(list)
    for tl, o, p in tier_rows:
        tier_grouped[tl].append((o, p))
    if tier_grouped:
        lines.append("\n<b>分层级：</b>")
        for tl in sorted(tier_grouped.keys()):
            s = _htbt_calc_stats(tier_grouped[tl])
            lines.append(f"  {tl}: {s['n']}笔  胜率{s['win_rate']}%  "
                        f"平均{(s['avg_win'] if s['n'] else 0):+.2f}%/{(s['avg_loss'] if s['n'] else 0):+.2f}%")

    # 健康度分层：早期跑的实验可能没有这个字段（health_status为NULL），
    # 用IS NOT NULL过滤，避免把"没有数据"误显示成"accumulating"之类的假分组
    health_rows = conn.execute("""
        SELECT health_status, outcome, outcome_pct FROM signals_history_backtest
        WHERE outcome != 'PENDING' AND is_selected = 1 AND param_set = ?
              AND health_status IS NOT NULL
    """, (param_set,)).fetchall()
    if health_rows:
        health_grouped = defaultdict(list)
        for hs, o, p in health_rows:
            health_grouped[hs].append((o, p))
        lines.append("\n<b>跨日健康度分层：</b>")
        for hs in sorted(health_grouped.keys()):
            s = _htbt_calc_stats(health_grouped[hs])
            lines.append(f"  {hs}: {s['n']}笔  胜率{s['win_rate']}%")

    lines.append(f"\n⚠️ 历史模拟，样本&lt;30笔仅供参考")
    return "\n".join(lines)


def _query_htbt_params(conn, param_set: str) -> str:
    try:
        row = conn.execute("""
            SELECT params_json, params_file, git_commit, first_seen_at
            FROM experiment_metadata WHERE param_set = ?
        """, (param_set,)).fetchone()
    except sqlite3.OperationalError:
        return "⚠️ experiment_metadata表还不存在，可能这个db是旧版本跑出来的，还没有参数档案功能"
    if not row:
        return f"没有找到参数集「{html.escape(param_set)}」的档案记录"
    params_json, params_file, git_commit, first_seen_at = row
    return (
        f"📋 <b>参数集 [{html.escape(param_set)}] 详情</b>\n\n"
        f"首次运行：{first_seen_at}\n"
        f"参数文件：{html.escape(params_file or '(baseline)')}\n"
        f"git commit：{html.escape(git_commit or '(未知)')}\n\n"
        f"实际参数内容：\n<pre>{html.escape(params_json or '')}</pre>"
    )


def _query_htbt_htf(conn, param_set: str) -> str:
    """
    查询小时级变种策略（intraday_htf_signals表）的结果——完全独立于
    signals_history_backtest那张EOD核心回测表，物理上是两张不同的表。

    ⚠️ 这里的警告文案和backtest_engine.py的htf_report()保持逐字一致，
    确保不管从VM命令行还是Telegram查，看到的边界声明都一样，
    不会因为查询入口不同就把这层"这不是验证intraday_monitor.py"的
    提醒漏掉。
    """
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "intraday_htf_signals" not in tables:
            return ("⚠️ intraday_htf_signals表尚未创建，"
                    "请先用 --include-hourly-intraday 跑一次 backtest_engine.py")

        if param_set == "latest":
            param_set = _htbt_latest_param_set(conn)
            if not param_set:
                return "暂无已跑过的历史回测实验"

        rows = conn.execute("""
            SELECT htf_mode, outcome, outcome_pct FROM intraday_htf_signals
            WHERE outcome != 'PENDING' AND param_set = ?
        """, (param_set,)).fetchall()
    except Exception as e:
        return f"❌ 查询失败：{e}"

    lines = [
        f"🧪 <b>[{html.escape(param_set)}]</b> 小时级变种（⚠️非intraday_monitor.py验证，反应更慢的粗颗粒度独立研究）",
        "",
    ]

    if not rows:
        lines.append(f"参数集「{html.escape(param_set)}」暂无已结算的小时级变种信号")
        return "\n".join(lines)

    n = len(rows)
    wins = sum(1 for _, o, _ in rows if o == "WIN")
    pcts = [p for _, _, p in rows if p is not None]
    win_rate = round(wins / n * 100, 1) if n else 0
    avg_pct = round(sum(pcts) / len(pcts), 2) if pcts else 0
    lines.append(f"样本：{n}笔  胜率：{win_rate}%  平均：{avg_pct:+.2f}%")

    mode_grouped = defaultdict(list)
    for mode, o, p in rows:
        mode_grouped[mode or "unknown"].append((o, p))
    lines.append("\n<b>按模式拆解：</b>")
    for mode, mrows in mode_grouped.items():
        mn = len(mrows)
        mwins = sum(1 for o, _ in mrows if o == "WIN")
        mwr = round(mwins / mn * 100, 1) if mn else 0
        lines.append(f"  {html.escape(mode)}: {mn}笔  胜率{mwr}%")

    return "\n".join(lines)


def _export_htbt_csv(param_set: str) -> tuple:
    """
    把某个param_set的完整交易明细导出成CSV临时文件。
    返回 (文件路径, 错误信息)——成功时错误信息为None，失败时文件路径为None。

    导出范围：T1-T4全部候选（不只是Top3精选），因为Claude做深挖分析时
    往往需要看全量数据自己筛选，而不是先被bot这边过滤掉一部分。
    """
    conn = _htbt_connect()
    if conn is None:
        return None, "⚠️ 历史回测数据库不存在（backtest_results.db）。"
    try:
        if param_set == "latest":
            param_set = _htbt_latest_param_set(conn)
            if not param_set:
                return None, "暂无已跑过的历史回测实验"

        cursor = conn.execute("""
            SELECT * FROM signals_history_backtest
            WHERE outcome != 'PENDING' AND param_set = ?
            ORDER BY signal_date
        """, (param_set,))
        col_names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return None, (f"参数集「{param_set}」暂无已结算数据"
                          f"（检查名字是否打对了，用 /htbt 看排行榜里的实际名字）")

        safe_name = re.sub(r"[^A-Za-z0-9_\-]", "_", param_set)
        path = f"/tmp/htbt_{safe_name}_{int(time.time())}.csv"
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)
        return path, None
    except Exception as e:
        log.error(f"_export_htbt_csv失败 [{param_set}]: {e}")
        return None, f"❌ CSV导出失败：{e}"
    finally:
        conn.close()


def _query_htbt(args: list) -> str:
    """
    /htbt              → 参数实验排行榜
    /htbt latest       → 最近一次跑完的实验详情
    /htbt <参数集名字>  → 某个具体实验的详情（胜率/分层/健康度）
    /htbt params <参数集名字> → 查看该实验实际用的参数内容+git commit
    /htbt csv <参数集名字|latest> → 导出该实验完整交易明细CSV并推送文件
                                    （这个分支在cmd_htbt里单独处理，不走这个
                                    返回文字的函数，因为要发文件而不是文字）
    """
    conn = _htbt_connect()
    if conn is None:
        return ("⚠️ 历史回测数据库不存在（backtest_results.db）。\n"
                "请先在VM上跑一次 backtest_engine.py。")
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "signals_history_backtest" not in tables:
            return "⚠️ signals_history_backtest表尚未创建，请先跑一次 backtest_engine.py。"

        if not args:
            return _query_htbt_leaderboard(conn)
        if args[0] == "board":
            return _query_htbt_leaderboard(conn)
        if args[0] == "params" and len(args) > 1:
            return _query_htbt_params(conn, args[1])
        if args[0] == "htf":
            target = args[1] if len(args) > 1 else "latest"
            return _query_htbt_htf(conn, target)
        if args[0] == "latest":
            latest = _htbt_latest_param_set(conn)
            if not latest:
                return "暂无已跑过的历史回测实验"
            return _query_htbt_detail(conn, latest)
        # 否则把第一个参数当作param_set名字查详情
        return _query_htbt_detail(conn, args[0])
    except Exception as e:
        log.error(f"_query_htbt失败: {e}")
        return f"❌ 查询失败：{e}"
    finally:
        conn.close()


# ── 命令处理 ──────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text(
        """👋 ASX交易助手已就绪

           📌 命令：
              /morning   — 立刻运行早盘扫描
              /eod       — 立刻运行EOD扫描
              /weekly    — 立刻运行周报（EOD选股+盘中信号过去7天回顾，
                           另外每周六也会自动推送一次）
              /news BHP  — 查看公告、新闻和AI分析
              /status    — 查看系统状态
              /logs      — 查看最近日志
              /watch BHP 15  — 添加BHP到长期监测，15天
              /unwatch BHP   — 移出监测队列
              /watchlist     — 查看当前监测队列

              📊 实盘信号回测（前向追踪，真实数据）：
              /backtest           — 整体胜率和期望值
              /backtest tier      — 按T1-T4层级分组
              /backtest catalyst  — 有无催化剂对比

              🧪 历史回测（离线模拟，调参用，非实盘）：
              /htbt               — 参数实验排行榜
              /htbt latest        — 最近一次实验详情
              /htbt 参数集名字     — 某个具体实验详情
              /htbt params 参数集名字 — 查看该实验实际参数内容+git commit
              /htbt csv 参数集名字|latest — 导出交易明细CSV文件
                                    （唯一产出文件的命令，其他都是文字总结）
              /htbt htf [参数集名字|latest] — 小时级变种策略结果
                                    （独立实验性数据，不是intraday_monitor.py验证）

           💬 直接用中文问我，例：
              "BHP最近有什么公告？"
              "今天first pullback有什么值得关注的?" """
    )

async def run_script(update, script, label):
    await update.message.reply_text(f"⏳ 正在运行{label}，完成后结果发到这里...")
    env = {**os.environ,
           "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
           "TELEGRAM_CHAT_ID": str(CHAT_ID)}
    try:
        r = subprocess.run(
            ["python3", f"/home/ubuntu/asx/{script}"],
            capture_output=True, text=True, timeout=1800, env=env)
        if r.returncode == 0:
            await update.message.reply_text(f"✅ {label}运行完成")
        else:
            await update.message.reply_text(f"❌ 出错：\n{r.stderr[-500:]}")
    except subprocess.TimeoutExpired:
        await update.message.reply_text("⏰ 超时（超过30分钟）")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_morning(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await run_script(update, "morning_scanner.py", "早盘扫描")

async def cmd_eod(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await run_script(update, "screener.py", "EOD扫描")

async def cmd_weekly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /weekly：手动触发一次周报（跟crontab周六自动跑的是同一个脚本
    weekly_review.py，命令本身不处理结果，脚本自己往Telegram推送——
    跟/eod、/morning的模式完全一致）。
    """
    if not auth(update): return
    await run_script(update, "weekly_review.py", "周报")

async def cmd_news(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    if not ctx.args:
        await update.message.reply_text("用法：/news BHP 或 /news BHP.AX")
        return
    code = ctx.args[0].upper().replace('.AX', '')
    await update.message.reply_text(f"🔍 查找 {code}.AX 数据...")
    anns  = get_stock_announcements(code)
    news  = get_yf_news(code)
    price = get_stock_price(code)
    body  = format_stock_info(code, anns, news, price)
    if anns or news:
        prompt = (f"{SYSTEM}\n\n请分析以下{code}.AX的最新信息，"
                  f"给出简洁的交易相关看法和风险提示：\n\n{body}")
        analysis = ask_gemini(prompt)
        msg = f"{body}\n\n🤖 <b>AI分析：</b>\n{analysis}"
    else:
        msg = f"{body}\n\n⚠️ 未找到公告或新闻数据"
    await update.message.reply_text(msg[:4000], parse_mode='HTML')

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    def last_run(path):
        if os.path.exists(path):
            t = datetime.fromtimestamp(os.path.getmtime(path), tz=AEST)
            return t.strftime("%m-%d %H:%M AEST")
        return "尚未运行"
    now = datetime.now(tz=AEST).strftime("%m-%d %H:%M AEST")
    await update.message.reply_text(
        f"📊 系统状态  {now}\n\n"
        f"✅ Bot运行正常\n"
        f"📈 EOD上次：{last_run('/home/ubuntu/logs/eod.log')}\n"
        f"🌅 Morning上次：{last_run('/home/ubuntu/logs/morning.log')}\n"
        f"🔍 Monitor上次：{last_run('/home/ubuntu/logs/monitor.log')}\n\n"
        f"🖥  VM: 158.179.23.237"
    )

async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    t    = ctx.args[0] if ctx.args else "eod"
    path = f"/home/ubuntu/logs/{t}.log"
    try:
        r   = subprocess.run(["tail", "-30", path], capture_output=True, text=True)
        txt = r.stdout.strip() or "日志为空"
        await update.message.reply_text(f"📋 {t}.log：\n\n{txt[-3500:]}")
    except Exception as e:
        await update.message.reply_text(f"读取失败：{e}")

async def cmd_backtest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /backtest           → 整体胜率 + 期望值 + Top3 vs 落选候选对比
    /backtest tier      → 按T1-T4层级分组统计
    /backtest catalyst  → 有无催化剂对比

    查的是实盘信号的前向追踪结果（announcements.db / signals_history）。
    历史模拟调参请用 /htbt（查backtest_results.db，完全独立的数据源）。
    """
    if not auth(update): return

    mode_map = {
        "tier":     "tier",
        "catalyst": "catalyst",
    }
    arg  = ctx.args[0].lower() if ctx.args else ""
    mode = mode_map.get(arg, "overall")

    await update.message.reply_text("📊 查询实盘回测数据中...")
    result = _query_backtest(mode)
    await update.message.reply_text(result[:4000], parse_mode="HTML")

async def cmd_htbt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /htbt                      → 参数实验排行榜
    /htbt latest               → 最近一次跑完的实验详情
    /htbt 参数集名字            → 某个具体实验的详情（胜率/分层/健康度）
    /htbt params 参数集名字     → 查看该实验实际用的参数内容+git commit
    /htbt csv 参数集名字|latest → 导出该实验完整交易明细CSV，作为文件推送
                                （不是文字总结——这是唯一能产出CSV文件的入口，
                                给Claude做深挖分析用；日常查看用不带csv的其他子命令）
    /htbt htf [参数集名字|latest] → 小时级变种策略结果（intraday_htf_signals表，
                                完全独立的实验性数据，不传参数集名字默认查latest）

    查的是backtest_engine.py离线跑出来的历史模拟结果
    （backtest_results.db / signals_history_backtest），跟 /backtest
    查询的实盘信号追踪数据完全独立，互不影响，也不会互相覆盖。
    """
    if not auth(update): return

    if ctx.args and ctx.args[0] == "csv":
        if len(ctx.args) < 2:
            await update.message.reply_text(
                "用法：/htbt csv 参数集名字\n或：/htbt csv latest（最近一次实验）"
            )
            return
        param_set = ctx.args[1]
        await update.message.reply_text(f"📄 正在导出「{param_set}」的交易明细CSV...")
        path, err = _export_htbt_csv(param_set)
        if err:
            await update.message.reply_text(err)
            return
        try:
            with open(path, "rb") as f:
                await update.message.reply_document(
                    document=f, filename=os.path.basename(path),
                    caption=f"🧪 {param_set} 历史回测明细（T1-T4全部候选，非仅Top3）"
                )
        except Exception as e:
            log.error(f"发送CSV文件失败 [{path}]: {e}")
            await update.message.reply_text(f"❌ 文件发送失败：{e}")
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
        return

    await update.message.reply_text("🧪 查询历史回测(模拟)数据中...")
    result = _query_htbt(ctx.args)
    await update.message.reply_text(result[:4000], parse_mode="HTML")

async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "用法：/watch 代码 天数\n例如：/watch BHP 15\n"
            "（重复添加同一股票会累加监测天数，最长45天）"
        )
        return
    code_raw = ctx.args[0].upper().replace(".AX", "")
    ticker = f"{code_raw}.AX"
    try:
        days = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text(f"❌ 天数必须是整数，你输入的是：{ctx.args[1]}")
        return
    if days < 1:
        await update.message.reply_text("❌ 天数至少为1天")
        return
    if days > wdb.MAX_MONITOR_DAYS:
        await update.message.reply_text(
            f"⚠️ 单次添加天数不能超过{wdb.MAX_MONITOR_DAYS}天，"
            f"已自动调整为{wdb.MAX_MONITOR_DAYS}天"
        )
        days = wdb.MAX_MONITOR_DAYS
    await update.message.reply_text(f"🔍 正在验证 {ticker} ...")
    price_info = get_stock_price(code_raw)
    if not price_info:
        await update.message.reply_text(
            f"❌ 暂时无法获取 {ticker} 的价格数据。\n"
            f"已重试3次仍失败，可能是Yahoo Finance瞬时限流/网络问题，"
            f"也可能代码确实不存在。\n"
            f"建议稍等1分钟后重试；如果反复失败，请确认代码正确"
            f"（例如 BHP、CBA、WES 等不含后缀的代码）。"
        )
        return
    company_name = ticker
    try:
        info = yf.Ticker(ticker).info
        company_name = info.get("longName", ticker)
    except Exception:
        pass
    wdb.init_watchlist_db()
    result = wdb.upsert_watchlist_manual(ticker, company_name, days)
    if result.get("action") == "error":
        await update.message.reply_text(f"❌ 添加失败：{result.get('error', '未知错误')}")
        return
    price_line = f"💰 现价：${price_info.get('price', 'N/A')}"
    if result["action"] == "created":
        await update.message.reply_text(
            f"✅ <b>已加入监测队列</b>\n\n"
            f"{company_name} ({ticker})\n"
            f"{price_line}\n"
            f"📅 监测天数：{result['new_total']}天\n\n"
            f"系统将在交易时段每15分钟扫描一次，"
            f"出现突破/回踩/尾盘确认信号时会推送给你。",
            parse_mode="HTML"
        )
    else:
        capped_note = "（已达到45天上限）" if result.get("capped") else ""
        await update.message.reply_text(
            f"✅ <b>监测天数已累加</b>\n\n"
            f"{company_name} ({ticker})\n"
            f"{price_line}\n"
            f"📅 原有进度：已监测{result['days_elapsed']}天\n"
            f"📅 新增：+{result['added_days']}天{capped_note}\n"
            f"📅 当前总监测天数：{result['new_total']}天\n"
            f"🔁 第{result['reselect_count']}次加入/续期",
            parse_mode="HTML"
        )

async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    if not ctx.args:
        await update.message.reply_text("用法：/unwatch 代码\n例如：/unwatch BHP")
        return
    ticker = f"{ctx.args[0].upper().replace('.AX', '')}.AX"
    ok = wdb.remove_from_watchlist(ticker)
    if ok:
        await update.message.reply_text(f"✅ 已将 {ticker} 移出监测队列")
    else:
        await update.message.reply_text(f"⚠️ {ticker} 当前不在监测队列中")

async def cmd_watchlist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    items = wdb.list_watchlist_for_display(include_exited=False)
    if not items:
        await update.message.reply_text("📭 当前监测队列为空\n用 /watch 代码 天数 添加股票")
        return
    lines = [f"📋 <b>当前监测队列（{len(items)}只）</b>\n"]
    for it in items:
        remain     = it["total_days"] - it["days_elapsed"]
        source_tag = "🤖EOD" if it["source"] == "eod" else "✋手动"
        score_str  = f" 评分:{it['composite_score']}" if it.get("composite_score") is not None else ""
        last_sig   = (f" | 上次信号:{it['last_signal_mode']}({it['last_signal_date']})"
                      if it.get("last_signal_mode") else "")
        lines.append(
            f"{source_tag} <b>{it['ticker']}</b> {it.get('company_name','')}\n"
            f"   进度:{it['days_elapsed']}/{it['total_days']}天（剩{remain}天）"
            f"{score_str}{last_sig}"
        )
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3900] + "\n\n…（队列较长，已截断）"
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_ai(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    msg   = update.message.text
    codes = list(set(re.findall(r'\b([A-Z]{3,4})\.AX\b', msg.upper())))
    context_data = ""
    if codes:
        await update.message.reply_text("🔍 正在查找相关数据...")
        for code in codes[:2]:
            anns  = get_stock_announcements(code)
            news  = get_yf_news(code)
            price = get_stock_price(code)
            info  = format_stock_info(code, anns, news, price)
            if anns or news:
                context_data += info + "\n\n"
            time.sleep(0.3)
    else:
        await update.message.reply_text("🤔 思考中...")
    prompt = f"{SYSTEM}\n\n"
    if context_data:
        prompt += f"以下是相关股票的最新数据：\n{context_data}\n\n"
    prompt += f"用户问题：{msg}"
    answer = ask_gemini(prompt)
    await update.message.reply_text(answer)

# ── 主程序 ───────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("morning",   cmd_morning))
    app.add_handler(CommandHandler("eod",       cmd_eod))
    app.add_handler(CommandHandler("weekly",    cmd_weekly))
    app.add_handler(CommandHandler("news",      cmd_news))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("logs",      cmd_logs))
    app.add_handler(CommandHandler("backtest",  cmd_backtest))
    app.add_handler(CommandHandler("htbt",      cmd_htbt))
    app.add_handler(CommandHandler("watch",     cmd_watch))
    app.add_handler(CommandHandler("unwatch",   cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_ai))
    print("Bot v3.2 启动中...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
