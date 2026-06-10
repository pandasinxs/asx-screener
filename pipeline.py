import os
import asyncio
from google import genai
from google.genai import types
from telegram import Bot
from data_collector import get_stock_comprehensive_data, serialize_to_prompt

# 1. 完美复刻你原本能成功读取的系统变量逻辑
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID_RAW    = os.environ.get("TELEGRAM_CHAT_ID", "")

# 2. 智能转换：把字符串的 CHAT_ID 安全转成 Telegram 要求的数字格式
try:
    CHAT_ID    = int(CHAT_ID_RAW) if CHAT_ID_RAW else 0
except ValueError:
    CHAT_ID    = 0

# 3. 安全熔断提示
if not GEMINI_KEY or not TELEGRAM_TOKEN or CHAT_ID == 0:
    print("❌ 错误：检测到系统环境变量有缺失！")
    print(f"  - GEMINI_API_KEY: {'✅ 正常' if GEMINI_KEY else '❌ 缺失'}")
    print(f"  - TELEGRAM_TOKEN: {'✅ 正常' if TELEGRAM_TOKEN else '❌ 缺失'}")
    print(f"  - TELEGRAM_CHAT_ID: {'✅ 正常' if CHAT_ID != 0 else '❌ 缺失或非纯数字'}")
    print("\n💡 提示：如果依然显示缺失，请在服务器终端运行一次你的系统变量载入指令（如 source ~/.bashrc）。")
    exit(1)

# 后面的初始化客户端和 main() 保持不变...
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
