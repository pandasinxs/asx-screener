import os
import time
import asyncio
import requests
from google import genai
from telegram import Bot
from data_collector import get_top_asx_movers, get_stock_comprehensive_data, serialize_to_prompt

GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
try: CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
except: CHAT_ID = 0

# 社交平台 API 密钥（可先在环境里配好，没配也不影响程序运行，会提示跳过）
X_API_KEY      = os.environ.get("X_API_KEY", "") 
XHS_COOKIE     = os.environ.get("XHS_COOKIE", "") 

client = genai.Client(api_key=GEMINI_KEY, http_options={'api_version': 'v1'})
tg_bot = Bot(token=TELEGRAM_TOKEN)

async def post_to_x(text):
    """自动发推特"""
    if not X_API_KEY:
        print("ℹ️ 提示：未配置 X_API_KEY，已自动跳过 X (Twitter) 自动发布。")
        return
    print("📤 正在同步发布至 X (Twitter)...")
    # 这里对接 Twitter API 逻辑

def post_to_xiaohongshu(text):
    """自动发小红书"""
    if not XHS_COOKIE:
        print("ℹ️ 提示：未配置 XHS_COOKIE，已自动跳过小红书自动发布。")
        return
    print("📤 正在同步发布至小红书...")
    # 这里对接小红书 Webhook 或推流 API

async def send_to_telegram(text):
    if CHAT_ID == 0: return
    try:
        await tg_bot.send_message(chat_id=CHAT_ID, text=text[:4000])
        print("✅ Telegram 消息推送成功！")
    except Exception as e: print(f"❌ TG 发送失败: {e}")

async def run_pipeline_for_stock(ticker):
    """单只股票的闭环管道"""
    raw_data = get_stock_comprehensive_data(ticker)
    final_prompt = serialize_to_prompt(raw_data)
    
    # 带重试的 Gemini 调用
    ai_report = None
    for attempt in range(1, 4):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=final_prompt,
                config={'max_output_tokens': 2500, 'temperature': 0.4}
            )
            ai_report = response.text
            break
        except Exception as e:
            if "503" in str(e) and attempt < 3:
                time.sleep(5)
            else:
                print(f"❌ {ticker} Gemini 调用失败: {e}")
                return

    if ai_report:
        # 🧠 解析出三种不同平台的文案
        parts = ai_report.split("#### 🔴 ")
        tg_text, x_text, xhs_text = "", "", ""
        
        for part in parts:
            if part.startswith("PLATFORM_TELEGRAM"):
                tg_text = part.replace("PLATFORM_TELEGRAM", "").strip()
            elif part.startswith("PLATFORM_X"):
                x_text = part.replace("PLATFORM_X", "").strip()
            elif part.startswith("PLATFORM_XIAOHONGSHU"):
                xhs_text = part.replace("PLATFORM_XIAOHONGSHU", "").strip()

        # 🚀 矩阵式全网同时发射
        if tg_text: await send_to_telegram(f"【{ticker} 机构内参】\n\n" + tg_text)
        if x_text:  await post_to_x(x_text)
        if xhs_text: post_to_xiaohongshu(xhs_text)

async def main():
    # 1. 自动筛选今日最劲爆的 3 只全场异动标的
    top_stocks = get_top_asx_movers(limit=3)
    
    # 2. 循环对每只标的进行深度分析并多平台分发
    for stock in top_stocks:
        print(f"\n🚀 开始处理今日异动星标: {stock['ticker']} (今日涨跌: {stock['pct_change']:.2f}%)")
        await run_pipeline_for_stock(stock['ticker'])
        time.sleep(2) # 礼貌间歇

if __name__ == "__main__":
    asyncio.run(main())
