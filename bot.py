# ============================================================
# ASX TRADING BOT v3
# 新API：asx.api.markitdigital.com
# 修复：yfinance新闻字段 content.title
# v3.1：新增 /backtest 命令，查询signals_history回测统计
# ============================================================

import os, subprocess, logging, re, time, sqlite3
import yfinance as yf
import requests
from datetime import datetime, date, timezone, timedelta
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

# signals_history和announcements在同一个DB
ANN_DB_PATH = "/home/ubuntu/asx/announcements.db"

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

# ── 回测查询函数 ──────────────────────────────────────────────

def _query_backtest(mode: str) -> str:
    """
    从signals_history查询回测统计。
    mode: "overall" | "tier" | "catalyst"
    返回格式化文本，供Telegram发送。
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
                    f"📊 <b>回测整体统计</b>",
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
                from collections import defaultdict
                tier_rows = defaultdict(list)
                for r in rows_by_tier:
                    tier_rows[r[0]].append((r[1], r[2]))

                lines = [f"📊 <b>回测按层级分组</b>（已结算{total_done}条）\n"]
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

                from collections import defaultdict
                cat_rows = defaultdict(list)
                for r in rows_cat:
                    cat_rows[r[0]].append((r[1], r[2]))

                lines = [f"📊 <b>回测：催化剂有无对比</b>（已结算{total_done}条）\n"]
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

# ── 命令处理 ──────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text(
        """👋 ASX交易助手已就绪

           📌 命令：
              /morning   — 立刻运行早盘扫描
              /eod       — 立刻运行EOD扫描
              /news BHP  — 查看公告、新闻和AI分析
              /status    — 查看系统状态
              /logs      — 查看最近日志
              /watch BHP 15  — 添加BHP到长期监测，15天
              /unwatch BHP   — 移出监测队列
              /watchlist     — 查看当前监测队列
              /backtest           — 整体胜率和期望值
              /backtest tier      — 按T1-T4层级分组
              /backtest catalyst  — 有无催化剂对比

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
    """
    if not auth(update): return

    mode_map = {
        "tier":     "tier",
        "catalyst": "catalyst",
    }
    arg  = ctx.args[0].lower() if ctx.args else ""
    mode = mode_map.get(arg, "overall")

    await update.message.reply_text("📊 查询回测数据中...")
    result = _query_backtest(mode)
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
    app.add_handler(CommandHandler("news",      cmd_news))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("logs",      cmd_logs))
    app.add_handler(CommandHandler("backtest",  cmd_backtest))
    app.add_handler(CommandHandler("watch",     cmd_watch))
    app.add_handler(CommandHandler("unwatch",   cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_ai))
    print("Bot v3.1 启动中...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()