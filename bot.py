# ============================================================
# ASX TRADING BOT v2
# Telegram + Gemini AI + ASX公告/新闻查询
# ============================================================

import os, subprocess, logging, re, time
import yfinance as yf
import requests
from datetime import datetime, timezone, timedelta
from telegram import Update
from telegram.ext import (Application, CommandHandler,
                          MessageHandler, filters, ContextTypes)
from google import genai

logging.basicConfig(level=logging.WARNING)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

gemini_client = genai.Client(api_key=GEMINI_KEY)
AEST = timezone(timedelta(hours=10))

SYSTEM = """你是一个ASX（澳大利亚股票交易所）专属交易助手。
用户使用两套策略：
1. EOD波段策略：收盘后扫描技术突破形态，持仓数天到数周。
2. First Pullback策略：开盘有公告催化，等回踩VWAP入场，持仓1-2天。
请用简洁中文回答（100-200字）。分析个股时给出风险提示。"""

# ── 工具函数 ──────────────────────────────────────────────────
def auth(update: Update) -> bool:
    return update.effective_chat.id == CHAT_ID

def ask_gemini(prompt: str) -> str:
    try:
        r = gemini_client.models.generate_content(
            model='gemini-2.0-flash-lite',
            contents=prompt
        )
        return r.text
    except Exception as e:
        return f"AI分析失败：{e}"

def get_stock_info(code: str) -> dict:
    """获取ASX公告 + yfinance新闻 + 基本价格"""
    code   = code.upper().replace('.AX', '')
    ticker = f"{code}.AX"
    data   = {'code': code, 'announcements': [], 'news': [],
               'price': None, 'change': None}

    # ASX官方公告（最近5条）
    try:
        url = (f"https://www.asx.com.au/asx/1/company/{code}"
               f"/announcements?count=5&market_sensitive=false")
        r = requests.get(url, timeout=8,
                         headers={'User-Agent': 'Mozilla/5.0'})
        for ann in r.json().get('data', [])[:5]:
            data['announcements'].append({
                'date'     : str(ann.get('document_release_date', ''))[:10],
                'title'    : ann.get('header', '')[:80],
                'sensitive': ann.get('market_sensitive', False)
            })
    except: pass

    # yfinance新闻（最近5条）
    try:
        stock = yf.Ticker(ticker)
        for n in (stock.news or [])[:5]:
            data['news'].append(n.get('title', '')[:80])
    except: pass

    # 基本价格
    try:
        fi = yf.Ticker(ticker).fast_info
        data['price']  = round(float(fi.last_price), 3)
        data['change'] = round(float(fi.regular_market_day_change_percent or 0), 2)
    except: pass

    return data

def format_stock_info(d: dict) -> str:
    lines = [f"📊 <b>{d['code']}.AX</b>"]
    if d['price'] is not None:
        chg = f" ({'+' if (d['change'] or 0) >= 0 else ''}{d['change']}%)"
        lines.append(f"💰 现价：${d['price']}{chg}")
    if d['announcements']:
        lines.append("\n📋 <b>ASX最新公告：</b>")
        for a in d['announcements']:
            flag = "⭐ " if a['sensitive'] else ""
            lines.append(f"  {flag}{a['date']}  {a['title']}")
    if d['news']:
        lines.append("\n📰 <b>近期新闻：</b>")
        for n in d['news']:
            lines.append(f"  • {n}")
    return "\n".join(lines)

# ── 命令处理 ──────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text(
        "👋 ASX交易助手已就绪\n\n"
        "📌 命令：\n"
        "/morning   — 立刻运行早盘扫描\n"
        "/eod       — 立刻运行EOD扫描\n"
        "/news BHP  — 查看公告、新闻和AI分析\n"
        "/status    — 查看系统状态\n"
        "/logs      — 查看最近日志\n\n"
        "💬 直接用中文问我，例：\n"
        "   "BHP最近有什么公告？"\n"
        "   "今天first pullback策略怎么找入场点？""
    )

async def run_script(update, script, label):
    await update.message.reply_text(f"⏳ 正在运行{label}，完成后结果发到这里...")
    env = {**os.environ,
           "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
           "TELEGRAM_CHAT_ID": str(CHAT_ID)}
    try:
        r = subprocess.run(
            ["python3", f"/home/ubuntu/asx/{script}"],
            capture_output=True, text=True,
            timeout=1800, env=env
        )
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
    await update.message.reply_text(f"🔍 查找 {code}.AX 的公告和新闻...")

    d         = get_stock_info(code)
    formatted = format_stock_info(d)

    if d['announcements'] or d['news']:
        prompt = (
            f"{SYSTEM}\n\n"
            f"请分析以下{code}.AX的最新信息，"
            f"给出简洁的交易相关看法和风险提示：\n\n{formatted}"
        )
        analysis = ask_gemini(prompt)
        msg = f"{formatted}\n\n🤖 <b>AI分析：</b>\n{analysis}"
    else:
        msg = f"{formatted}\n\n⚠️ 未找到公告或新闻数据"

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
        r   = subprocess.run(["tail", "-30", path],
                             capture_output=True, text=True)
        txt = r.stdout.strip() or "日志为空"
        await update.message.reply_text(
            f"📋 {t}.log 最近30行：\n\n{txt[-3500:]}"
        )
    except Exception as e:
        await update.message.reply_text(f"读取失败：{e}")

async def cmd_ai(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    msg = update.message.text

    # 自动检测股票代码（2-5个大写字母）
    codes = list(set(re.findall(r'\b([A-Z]{2,5})(?:\.AX)?\b', msg)))

    context_data = ""
    if codes:
        await update.message.reply_text("🔍 正在查找相关数据...")
        for code in codes[:2]:
            d = get_stock_info(code)
            if d['announcements'] or d['news']:
                context_data += format_stock_info(d) + "\n\n"
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
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_ai)
    )
    print("Bot v2 启动中...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()