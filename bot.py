# ============================================================
# ASX TRADING BOT
# Telegram + Gemini AI 命令控制助手
# ============================================================

import os, subprocess, logging
from datetime import datetime, timezone, timedelta
from telegram import Update
from telegram.ext import (Application, CommandHandler,
                          MessageHandler, filters, ContextTypes)
import google.generativeai as genai

logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

SYSTEM = """你是一个ASX（澳大利亚股票交易所）专属交易助手。
用户使用两套策略：
1. EOD波段策略：收盘后扫描，寻找技术突破形态，持仓数天到数周。
2. First Pullback策略：开盘有公告催化，等回踩VWAP入场，持仓1-2天。
请用简洁中文回答。分析个股时给出风险提示。"""

AEST = timezone(timedelta(hours=10))

def auth(update: Update) -> bool:
    return update.effective_chat.id == CHAT_ID

# ── /start ────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    await update.message.reply_text(
        "👋 ASX交易助手已就绪\n\n"
        "📌 命令：\n"
        "/morning — 立刻运行早盘扫描\n"
        "/eod     — 立刻运行EOD扫描\n"
        "/status  — 查看系统状态\n"
        "/logs    — 查看最近日志\n"
        "/logs morning — 早盘日志\n"
        "/logs monitor — 监控日志\n\n"
        "💬 直接用中文问我任何问题"
    )

# ── 运行脚本 ──────────────────────────────────────────────────
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
            err = r.stderr[-500:] or "无错误信息"
            await update.message.reply_text(f"❌ 出错：\n{err}")
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

# ── /status ───────────────────────────────────────────────────
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

# ── /logs ─────────────────────────────────────────────────────
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

# ── AI对话 ────────────────────────────────────────────────────
async def cmd_ai(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update): return
    q = update.message.text
    await update.message.reply_text("🤔 思考中...")
    try:
        r = model.generate_content(f"{SYSTEM}\n\n问题：{q}")
        await update.message.reply_text(r.text)
    except Exception as e:
        await update.message.reply_text(f"AI回复失败：{e}")

# ── 主程序 ───────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("morning", cmd_morning))
    app.add_handler(CommandHandler("eod",     cmd_eod))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("logs",    cmd_logs))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_ai)
    )
    print("Bot启动中...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
