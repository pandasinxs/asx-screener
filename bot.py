# ============================================================
# ASX TRADING BOT v3
# 新API：asx.api.markitdigital.com
# 修复：yfinance新闻字段 content.title
# ============================================================

import os, subprocess, logging, re, time
import yfinance as yf
import requests
from datetime import datetime, date, timezone, timedelta
from telegram import Update
from telegram.ext import (Application, CommandHandler,
                          MessageHandler, filters, ContextTypes)
from google import genai

import watchlist_db as wdb

logging.basicConfig(level=logging.WARNING)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

gemini_client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None
AEST = timezone(timedelta(hours=10))

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
            return ""   # 静默跳过，不报错给用户
        return ""

def get_stock_announcements(code: str) -> list:
    """获取某只股票的最近公告（从今日全量公告中筛选）"""
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
            # 如果页面里最旧的条目已经是3天前，停止翻页
            if all_items and all_items[-1].get('date','')[:10] < (
                    date.today().isoformat()[:8] + '01'):
                break
            if len(all_items) < 100: break
            page += 1
            time.sleep(0.3)
        except: break
    return items[:5]

def get_yf_news(code: str) -> list:
    """获取yfinance新闻（修复content.title）"""
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

def get_stock_price(code: str) -> dict:
    try:
        fi = yf.Ticker(f"{code.upper().replace('.AX','')}.AX").fast_info
        return {
            'price' : round(float(fi.last_price), 3),
            'change': round(float(fi.regular_market_day_change_percent or 0), 2)
        }
    except:
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

async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /watch BHP 15
    添加股票到长期盘中监测队列，并指定监测天数。
    重复添加同一股票 → 累加天数（封顶45天，由watchlist_db.MAX_MONITOR_DAYS控制）。
    """
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
            f"❌ 无法获取 {ticker} 的价格数据，代码可能不存在或暂时无法访问。\n"
            f"请确认代码正确后重试（例如 BHP、CBA、WES 等不含后缀的代码）。"
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
    """/unwatch BHP —— 从监测队列移出指定股票"""
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
    """/watchlist —— 查看当前所有正在监测的股票"""
    if not auth(update): return

    items = wdb.list_watchlist_for_display(include_exited=False)
    if not items:
        await update.message.reply_text("📭 当前监测队列为空\n用 /watch 代码 天数 添加股票")
        return

    lines = [f"📋 <b>当前监测队列（{len(items)}只）</b>\n"]
    for it in items:
        remain = it["total_days"] - it["days_elapsed"]
        source_tag = "🤖EOD" if it["source"] == "eod" else "✋手动"
        score_str = f" 评分:{it['composite_score']}" if it.get("composite_score") is not None else ""
        last_sig = (f" | 上次信号:{it['last_signal_mode']}({it['last_signal_date']})"
                    if it.get("last_signal_mode") else "")
        lines.append(
            f"{source_tag} <b>{it['ticker']}</b> {it.get('company_name','')}\n"
            f"   进度:{it['days_elapsed']}/{it['total_days']}天（剩{remain}天）"
            f"{score_str}{last_sig}"
        )

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3900] + "\n\n…（队列较长，已截断，完整数据见watchlist.db）"
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
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("morning", cmd_morning))
    app.add_handler(CommandHandler("eod",     cmd_eod))
    app.add_handler(CommandHandler("news",    cmd_news))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("logs",    cmd_logs))
    app.add_handler(CommandHandler("watch",     cmd_watch))
    app.add_handler(CommandHandler("unwatch",   cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_ai))
    print("Bot v3 启动中...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
