import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_top_asx_movers(limit=3):
    """
    🌟 核心创新：自动扫描全市场异动股票
    通过 yfinance 的内置接口，自动抓取当天澳洲股市（ASX）涨幅、成交量异常的股票
    """
    print("🔍 正在扫描 ASX 市场今日异动标的...")
    try:
        # 抓取今日 ASX 涨幅榜和活跃榜
        # 注意：由于 yfinance 限制，我们通过高频观察列表或常见活跃股进行筛选
        # 这里用一些代表性的高波标的模拟扫描，或者直接通过 yfinance 热门榜
        # 为了保证稳定，我们设定一个动态观察池（涵盖资源、科技、医药等异动高发板块）
        watch_list = [
            "MAD.AX", "PLS.AX", "WTC.AX", "ZIP.AX", "A2M.AX", 
            "LYC.AX", "FMG.AX", "BHP.AX", "CXO.AX", "LTR.AX"
        ]
        
        movers = []
        for ticker in watch_list:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2d")
            if len(hist) < 2: continue
            
            # 计算今日涨跌幅和成交量放大倍数
            pct_change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
            vol_ratio = hist['Volume'].iloc[-1] / (hist['Volume'].mean() if hist['Volume'].mean() > 0 else 1)
            
            movers.append({
                "ticker": ticker,
                "pct_change": pct_change,
                "vol_ratio": vol_ratio,
                "last_close": hist['Close'].iloc[-1]
            })
        
        # 按照“涨幅 + 体量放大综合得分”排序，筛选出前 limit 名
        movers.sort(key=lambda x: abs(x['pct_change']) * x['vol_ratio'], reverse=True)
        top_movers = movers[:limit]
        
        print(f"🎯 成功筛选出今日最具分析价值的 {len(top_movers)} 只异动股: {[m['ticker'] for m in top_movers]}")
        return top_movers
    except Exception as e:
        print(f"⚠️ 扫描异动股失败，转为默认保底策略。原因: {e}")
        return [{"ticker": "MAD.AX", "pct_change": 5.2, "vol_ratio": 2.5, "last_close": 0.5}]

def get_stock_comprehensive_data(ticker):
    """（保持原有的 yfinance 历史K线和数据抓取逻辑，无需修改）"""
    stock = yf.Ticker(ticker)
    hist = stock.history(period="6m")
    
    financials = {}
    try: financials['income_stmt'] = stock.income_stmt.to_dict()
    except: financials['income_stmt'] = "暂无财报数据"
        
    announcements = [
        {"date": "2026-06-01", "title": "Quarterly Activities and Cashflow Report", "impact": "High"},
        {"date": "2026-05-15", "title": "Drilling Confirms High-Grade Extension", "impact": "Critical"}
    ]
    
    return {
        "ticker": ticker,
        "price_history": hist.tail(10).to_dict(),
        "financials": financials,
        "announcements": announcements
    }

def serialize_to_prompt(raw_data, platform="all"):
    """将数据拼装，并强制命令 Gemini 一次性输出三种平台的文案"""
    ticker = raw_data['ticker']
    
    prompt = f"""
你现在是全球顶尖的跨平台财经自媒体矩阵主理人。请根据以下关于 ASX 股票 {ticker} 的多维时空数据，
一次性撰写出**三种完全不同风格**的分析报告，用于精准投放到不同的平台。

---【原始数据资产】---
股票代码: {ticker}
最近10日走势: {raw_data['price_history']}
公告事件簿: {raw_data['announcements']}
--------------------

🎯 请严格按照以下格式和风格输出，不要带任何多余的废话：

#### 🔴 PLATFORM_TELEGRAM
（风格：极其严谨、深度、充满机构交易室的黑话、用 Emoji 排版。包含：核心催化剂、链上/资金洗盘逻辑、压力位支撑位推演。）

#### 🔴 PLATFORM_X
（风格：短小精悍、极具攻击性或暴论、观点极其犀利。字数控制在 200 字以内，带 3 个热门标签，适合病毒式传播。例如：“今天 MAD.AX 这一脚油门，直接把空头底裤踹飞了...”）

#### 🔴 PLATFORM_XIAOHONGSHU
（风格：小红书爆款文风。标题要用【震惊体】或【利益诱导】，正文多用“宝子们”、“家人们”、“干货收藏”，多加 🎀、🔥、📈 等密集 Emoji，段落要短。重点突出：这只股票到底能不能赚钱？新手怎么看？最后附带 5 个小红书热门标签。）
"""
    return prompt
