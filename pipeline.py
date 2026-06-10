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

# （前面的 import 和变量保持不变...）

async def run_pipeline_for_stock(ticker):
    raw_data = get_stock_comprehensive_data(ticker)
    final_prompt = serialize_to_prompt(raw_data)
    
    ai_report = None
    for attempt in range(1, 4):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=final_prompt,
                config={'max_output_tokens': 2500, 'temperature': 0.3}
            )
            ai_report = response.text
            break
        except Exception as e:
            if "503" in str(e): time.sleep(5)
            else: return

    if ai_report:
        # 🌟 核心修复：精准切割三份文案
        parts = ai_report.split("#### 🔴 ")
        
        for part in parts:
            part = part.strip()
            if not part: continue
            
            if part.startswith("PLATFORM_TELEGRAM"):
                tg_text = part.replace("PLATFORM_TELEGRAM", "").strip()
                await send_to_telegram(tg_text)
                
            elif part.startswith("PLATFORM_X"):
                x_text = part.replace("PLATFORM_X", "").strip()
                # 预留发布：print("X文案生成成功，等待发布")
                
            elif part.startswith("PLATFORM_XIAOHONGSHU"):
                xhs_text = part.replace("PLATFORM_XIAOHONGSHU", "").strip()
                # 预留发布：print("小红书文案生成成功，等待发布")
