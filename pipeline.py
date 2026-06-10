import os
import time
import asyncio
from google import genai
from telegram import Bot
from data_collector import get_top_asx_movers, get_stock_comprehensive_data, serialize_to_prompt

# 1. 完美读取你的系统环境变量（从 ~/.asx_env 载入的）
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
try: 
    CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
except: 
    CHAT_ID = 0

# 2. 初始化全新一代生产级付费网关客户端
client = genai.Client(api_key=GEMINI_KEY, http_options={'api_version': 'v1'})
tg_bot = Bot(token=TELEGRAM_TOKEN)

async def send_to_telegram(text):
    """安全推送 Telegram 消息"""
    if CHAT_ID == 0: 
        print("ℹ️ 提示：未配置有效的 CHAT_ID，跳过 TG 发送。")
        return
    try:
        await tg_bot.send_message(chat_id=CHAT_ID, text=text[:4000])
        print("✅ Telegram 消息推送成功！")
    except Exception as e: 
        print(f"❌ TG 发送失败: {e}")

async def run_pipeline_for_stock(ticker):
    """单只股票的【数据 ➜ AI ➜ 切片】闭环管道"""
    print(f"🔄 正在提取 {ticker} 的真实时空数据与实时新闻...")
    raw_data = get_stock_comprehensive_data(ticker)
    
    print(f"🔄 正在拼装全新黄金 Prompt...")
    final_prompt = serialize_to_prompt(raw_data)
    
    print(f"🧠 正在调用 gemini-2.5-flash（带3次防塞车自动重试）...")
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
            if "503" in str(e) and attempt < 3:
                print(f"⚠️ 触发 Google 付费节点临时拥堵 (503)，5秒后进行第 {attempt} 次自动重试...")
                time.sleep(5)
            else:
                print(f"❌ {ticker} Gemini 最终调用失败: {e}")
                return

    if ai_report:
        # 🌟 精准切割三份平台文案
        parts = ai_report.split("#### 🔴 ")
        
        for part in parts:
            part = part.strip()
            if not part: continue
            
            if part.startswith("PLATFORM_TELEGRAM"):
                tg_text = part.replace("PLATFORM_TELEGRAM", "").strip()
                # 精准推送干净、无污染的机构内参到 Telegram
                await send_to_telegram(f"【{ticker} 机构内参】\n\n" + tg_text)
                
            elif part.startswith("PLATFORM_X"):
                x_text = part.replace("PLATFORM_X", "").strip()
                print(f"🖨️ [X 文案已就绪] 长度: {len(x_text)} 字，等待后续自动发布功能激活。")
                
            elif part.startswith("PLATFORM_XIAOHONGSHU"):
                xhs_text = part.replace("PLATFORM_XIAOHONGSHU", "").strip()
                print(f"🖨️ [小红书文案已就绪] 长度: {len(xhs_text)} 字，等待后续自动发布功能激活。")

async def main():
    """【整个自动化工厂的核心总发动机】"""
    print("====== ASX 自动多矩阵自媒体生产线启动 ======")
    
    # 1. 自动筛选今日最具增值、暴动潜力的 3 只股票
    top_stocks = get_top_asx_movers(limit=3)
    
    if not top_stocks:
        print("ℹ️ 今日市场平静，未扫描到满足异动条件的标的。")
        return
        
    # 2. 依次灌入管道处理
    for stock in top_stocks:
        print(f"\n🚀 开始处理今日异动星标: {stock['ticker']} (今日绝对涨跌: {abs(stock['pct_change']):.2f}%)")
        await run_pipeline_for_stock(stock['ticker'])
        time.sleep(2) # 礼貌防高频间歇
        
    print("\n====== 今日全矩阵内容生产完毕！====== ")

if __name__ == "__main__":
    # 彻底激活异步引擎
    asyncio.run(main())
