import os
import asyncio
from google import genai
from google.genai import types
from telegram import Bot
from data_collector import get_stock_comprehensive_data, serialize_to_prompt

# 1. 严格对接你现有的环境变量命名（在 Oracle 系统环境或 .env 中加载）
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

try:
    CHAT_ID    = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
except ValueError:
    CHAT_ID    = 0

# 2. 安全检查：如果密钥没配对，立刻熔断报错，防止后续报错难以排查
if not GEMINI_KEY or not TELEGRAM_TOKEN or CHAT_ID == 0:
    print("❌ 错误：环境变量读取失败，请检查系统的环境变量配置！")
    print(f"  - GEMINI_API_KEY: {'已加载' if GEMINI_KEY else '缺失'}")
    print(f"  - TELEGRAM_TOKEN: {'已加载' if TELEGRAM_TOKEN else '缺失'}")
    print(f"  - TELEGRAM_CHAT_ID: {CHAT_ID if CHAT_ID != 0 else '缺失或格式错误'}")
    exit(1)

# 3. 初始化全新一代 Gemini 客户端（2026年标准 SDK）
client = genai.Client(api_key=GEMINI_KEY)

# 4. 初始化 Telegram Bot 客户端
tg_bot = Bot(token=TELEGRAM_TOKEN)

async def send_to_telegram(text):
    """异步向你的 Telegram 频道/群聊推送内容"""
    try:
        print(f"📤 正在向 TG 频道发送内参 (Chat ID: {CHAT_ID})...")
        # Telegram 单条消息上限 4096 字符，做安全截断防止爆接口
        await tg_bot.send_message(chat_id=CHAT_ID, text=text[:4000], parse_mode=None)
        print("✅ Telegram 消息推送成功！")
    except Exception as e:
        print(f"❌ Telegram 发送失败，原因: {e}")

async def main():
    # 测试跑通模拟数据对象 MAD.AX
    ticker = "MAD.AX"
    
    print(f"🔄 [Step 1] 正在提取 {ticker} 的多维历史时空数据...")
    raw_data = get_stock_comprehensive_data(ticker)
    
    print(f"🔄 [Step 2] 正在拼装黄金推理 Prompt...")
    final_prompt = serialize_to_prompt(raw_data)
    
    # 设置防说死安全熔断，降低温度（0.3）确保财务推理高度严谨
    config = types.GenerationConfig(
        max_output_tokens=1200,
        temperature=0.3,
    )
    
    print(f"🧠 [Step 3] 正在调用 gemini-2.5-flash 启动思维链推理（Thinking）...")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=final_prompt,
            config=config
        )
        ai_report = response.text
        
        print("\n" + "="*20 + " AI 交易内参产出 " + "="*20)
        print(ai_report)
        print("="*56)
        
        # [Step 4] 核心闭环：将产出的高价值内参自动推送至你的 Telegram
        await send_to_telegram(ai_report)
        
    except Exception as e:
        print(f"\n❌ Gemini API 调用失败，原因: {e}")

if __name__ == "__main__":
    # 由于 python-telegram-bot v20+ 采用全异步架构，必须用 asyncio 驱动
    asyncio.run(main())
