import os
import sys
import time
import asyncio
from google import genai
from google.genai import types
from dotenv import load_dotenv

# 确保能正确导入你刚刚重构的动态数据收集器
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import data_collector

# 加载环境变量
load_dotenv(os.path.expanduser("~/.asx_env"))

# 初始化 Gemini 客户端
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("❌ 错误：未在环境变量中找到 GEMINI_API_KEY")
    sys.exit(1)

client = genai.Client(api_key=api_key)

async def run_pipeline_for_stock(ticker):
    """为单只异动股运行完整的‘提取-生成-切割’流水线，带 10 分钟疯狗重试机制"""
    print(f"\n🚀 开始处理股票: {ticker} ...")
    
    # 1. 提取最新的全盘精细化量化与公告数据
    try:
        raw_data = data_collector.get_stock_comprehensive_data(ticker)
        # 将数据组装成量化专家级 Prompt
        prompt = data_collector.serialize_to_prompt(raw_data)
    except Exception as e:
        print(f"❌ 提取 {ticker} 数据失败: {e}")
        return

    # 配置高稳定性模型参数
    config = types.GenerateContentConfig(
        temperature=0.2,  # 调低随机性，逼迫 Gemini 严谨对齐数据
        max_output_tokens=2500
    )

    # 2. 呼叫 Gemini 生产硬核报告（带抗拥堵重试）
    ai_report = None
    max_duration = 600  # 极限抗死磕 10 分钟
    retry_interval = 30  # 每 30 秒冲锋一次
    start_time = time.time()
    attempt = 1

    print("🤖 正在呼叫 Gemini 量化专家大脑生产精炼评估报告...")
    while time.time() - start_time < max_duration:
        try:
            # 统一使用官方推荐的旗舰主力模型
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=config
            )
            if response.text:
                ai_report = response.text
                print(f"✅ Gemini 成功在第 {attempt} 次尝试中交卷！")
                break
        except Exception as e:
            error_msg = str(e)
            print(f"⚠️ 🧠 Gemini 接口发生拥堵 (尝试 {attempt}): {error_msg}")
            
            # 触发 503 核心熔断自愈：如果是谷歌服务器波动，立刻无脑重试
            if "503" in error_msg or "ResourceExhausted" in error_msg or "Overloaded" in error_msg:
                print(f"🔄 触发抗拥堵机制：系统将在 {retry_interval} 秒后再次疯狂冲锋...")
                await asyncio.sleep(retry_interval)
                attempt += 1
                continue
            else:
                # 遇到非服务器波动的硬性错误（如 Prompt 语法错误），直接终止，防止浪费 Token
                print("❌ 触发硬性错误，放弃重试。")
                break

    if not ai_report:
        print(f"❌ 轰炸 10 分钟结束，Gemini 依旧瘫痪，放弃处理 {ticker}。")
        return

    # 3. 精密切割文本手术
    print("✂️ 正在对 AI 原始长文进行多矩阵渠道切割分发...")
    parts = ai_report.split("#### 🔴 ")
    
    platforms_data = {}
    for part in parts:
        if not part.strip(): continue
        lines = part.split("\n")
        header = lines[0].strip() # 提取 PLATFORM_TELEGRAM / PLATFORM_X 等标签
        content = "\n".join(lines[1:]).strip()
        platforms_data[header] = content

    # 4. 打印最终干净利落的硬核分发文本
    print(f"\n================ 🏆 {ticker} 分渠道成品展示 ================")
    if "PLATFORM_TELEGRAM" in platforms_data:
        print(f"\n📬 [准备发往 Telegram 机构频道]:\n{platforms_data['PLATFORM_TELEGRAM']}\n")
    if "PLATFORM_X" in platforms_data:
        print(f"📬 [准备发往 X 推特短讯]:\n{platforms_data['PLATFORM_X']}\n")
    if "PLATFORM_XIAOHONGSHU" in platforms_data:
        print(f"📬 [准备发往 小红书 爆款池]:\n{platforms_data['PLATFORM_XIAOHONGSHU']}\n")
    print("========================================================\n")

async def main():
    print("🏁 动态量化天网流水线总闸开启...")
    
    # 从全新重构的 ASX 漏斗中捞出当天真正的野生前 3 名异动股
    top_stocks = data_collector.get_top_asx_movers(limit=3)
    
    # 🌟 核心防御：如果今天遇上公众假期或大盘没有任何异动，这里拿到的 top_stocks 就是 []
    # 循环不会执行，直接优雅退场，不浪费一丁点 Token！
    if not top_stocks:
        print("🏁 全盘无异动数据，流水线安全打卡当下班，未触发任何 AI 计费。")
        return

    for stock in top_stocks:
        # 顺次安全处理每只被锁定的野生黑马
        await run_pipeline_for_stock(stock['ticker'])
        # 渠道间适当休眠，保护服务器
        await asyncio.sleep(2)
        
    print("🎉 今日所有黑马股量化内参全部生产完毕！")

if __name__ == "__main__":
    asyncio.run(main())
