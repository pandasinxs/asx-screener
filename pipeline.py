import os
import time
import asyncio
from google import genai
from telegram import Bot
from data_collector import get_top_asx_movers, get_stock_comprehensive_data, serialize_to_prompt

# 1. 载入系统环境变量
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
        # Telegram 单条消息上限是 4096 字符，这里加一层安全裁剪，防止塞爆
        await tg_bot.send_message(chat_id=CHAT_ID, text=text[:4000])
        print("✅ Telegram 消息推送成功！")
    except Exception as e: 
        print(f"❌ TG 发送失败: {e}")

async def run_pipeline_for_stock(ticker):
    """
    🌟 核心整改：每只股票独立享有完整的 Gemini 算力和 Token 额度，彻底杜绝截断
    """
    print(f"🔄 [数据端] 正在提取 {ticker} 的真实K线指标与实时新闻...")
    raw_data = get_stock_comprehensive_data(ticker)
    
    print(f"🔄 [结构端] 正在为 {ticker} 拼装独立黄金 Prompt...")
    final_prompt = serialize_to_prompt(raw_data)
    
    print(f"🧠 [AI端] 正在调用 gemini-2.5-flash 为 {ticker} 进行专有全矩阵内容生成...")
    ai_report = None
    
    # 🌟 疯狗流抗压升级：每 30 秒轰炸一次，连续死磕 10 分钟（共 20 次）
    max_retries = 20
    retry_delay = 30
    
    for attempt in range(1, max_retries + 1):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=final_prompt,
                config={
                    'max_output_tokens': 3000,
                    'temperature': 0.3
                }
            )
            ai_report = response.text
            break # 只要抓到一次成功，立刻 break 终止循环，绝不多发一次请求！
        except Exception as e:
            if "503" in str(e) and attempt < max_retries:
                print(f"⚠️ 警告：Google 付费机房持续大塞车 (503)。")
                print(f"   ⚔️ 开启高频死磕：已等待 {retry_delay} 秒，即将进行第 {attempt}/{max_retries} 次冲锋...")
                time.sleep(retry_delay)
            else:
                print(f"❌ {ticker} 历经 {attempt} 次疯狂死磕后依然遭遇致命错误: {e}")
                return

    if not ai_report:
        print(f"⚠️ {ticker} 未能生成有效的 AI 报告，跳过分发。")
        return

    # 🌟 文本切分安全阀：确保即使格式微调也能正确抓取
    parts = ai_report.split("#### 🔴 ")
    
    tg_text, x_text, xhs_text = "", "", ""
    for part in parts:
        part = part.strip()
        if not part: continue
        
        if part.startswith("PLATFORM_TELEGRAM"):
            tg_text = part.replace("PLATFORM_TELEGRAM", "").strip()
        elif part.startswith("PLATFORM_X"):
            x_text = part.replace("PLATFORM_X", "").strip()
        elif part.startswith("PLATFORM_XIAOHONGSHU"):
            xhs_text = part.replace("PLATFORM_XIAOHONGSHU", "").strip()

    # 🚀 精准单独分发
    if tg_text:
        await send_to_telegram(f"【{ticker} 机构内参】\n\n" + tg_text)
    else:
        print(f"⚠️ 警告：未能成功解析出 {ticker} 的 Telegram 文案")

    if x_text:
        print(f"🖨️ [X 文案已就绪] 长度: {len(x_text)} 字")
    if xhs_text:
        print(f"🖨️ [小红书文案已就绪] 长度: {len(xhs_text)} 字")

async def main():
    print("====== ASX 自动多矩阵自媒体生产线启动 ======")
    
    # 1. 自动筛选今日最具增值、暴动潜力的 3 只股票
    top_stocks = get_top_asx_movers(limit=3)
    
    if not top_stocks:
        print("ℹ️ 今日市场平静，未扫描到满足异动条件的标的。")
        return
        
    # 2. 依次灌入管道处理（单件流，1只处理完再进下1只）
    for stock in top_stocks:
        print(f"\n🚀 === 开始独立处理标的: {stock['ticker']} ===")
        await run_pipeline_for_stock(stock['ticker'])
        print(f"🏁 === 标的 {stock['ticker']} 处理完毕 ===")
        time.sleep(3) # 留出 3 秒给服务器喘息，防止触发高频风控
        
    print("\n====== 今日全矩阵内容全自动化生产完毕！====== ")

if __name__ == "__main__":
    asyncio.run(main())
