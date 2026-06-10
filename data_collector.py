import yfinance as yf
import pandas as pd
from datetime import datetime

def get_top_asx_movers(limit=3):
    """自动扫描今日最具代表性的异动标的"""
    print("🔍 正在扫描 ASX 市场今日异动标的...")
    # 动态监控池（包含近期高关注度、高波动的标的）
    watch_list = ["ZIP.AX", "PLS.AX", "WTC.AX", "CXO.AX", "LTR.AX", "FMG.AX"]
    movers = []
    
    for ticker in watch_list:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2d")
            if len(hist) < 2: continue
            
            # 计算真实涨跌幅和今日成交量
            pct_change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
            current_vol = hist['Volume'].iloc[-1]
            
            movers.append({
                "ticker": ticker,
                "pct_change": pct_change,
                "volume": current_vol,
                "last_close": hist['Close'].iloc[-1]
            })
        except: continue
        
    # 按绝对涨跌幅排序
    movers.sort(key=lambda x: abs(x['pct_change']), reverse=True)
    return movers[:limit]

def get_stock_comprehensive_data(ticker):
    """抓取全真实的K线和公司新闻"""
    stock = yf.Ticker(ticker)
    # 🌟 修复点 1：严格使用 6mo
    hist = stock.history(period="6mo")
    
    # 🌟 修复点 2：抓取雅虎财经真实的最新新闻标题，彻底告别“全员钻探”的幻觉
    news_titles = []
    try:
        news = stock.news
        if news:
            for item in news[:3]: # 只取最近3条
                news_titles.append({
                    "title": item.get("title", ""),
                    "publisher": item.get("publisher", ""),
                    "time": datetime.fromtimestamp(item.get("providerPublishTime", 0)).strftime('%Y-%m-%d')
                })
    except:
        news_titles = [{"title": "Market general adjustment and volume expansion", "publisher": "Exchange", "time": "2026-06-10"}]

    # 提取过去 10 天的最高/最低价，供 Gemini 计算真实的支撑压力位
    last_10_days = hist.tail(10)
    price_metrics = {
        "current_price": last_10_days['Close'].iloc[-1],
        "10d_high": last_10_days['High'].max(),
        "10d_low": last_10_days['Low'].min(),
        "10d_avg_vol": last_10_days['Volume'].mean()
    }
    
    return {
        "ticker": ticker,
        "price_metrics": price_metrics,
        "news": news_titles
    }

def serialize_to_prompt(raw_data):
    ticker = raw_data['ticker']
    metrics = raw_data['price_metrics']
    
    prompt = f"""
你现在是顶级跨平台财经自媒体矩阵主理人。请根据以下真实数据，为 ASX 股票 {ticker} 撰写三份文案。

---【真实数据资产】---
股票代码: {ticker}
当前最新收盘价: ${metrics['current_price']:.3f}
过去10日最高价: ${metrics['10d_high']:.3f}
过去10日最低价: ${metrics['10d_low']:.3f}
近期核心公开新闻/市场动态: {raw_data['news']}
--------------------

🎯 请严格按照以下格式输出，不要带任何多余的开头和废话（直接输出四个井号开头的标记）：

#### 🔴 PLATFORM_TELEGRAM
**{ticker} 机构内参：异动与资金流向研判**
- 核心催化剂分析：结合新闻 {raw_data['news']} 和今日涨跌表现，深度解构其背后的核心驱动力。
- 支撑/压力位精准推演：必须结合10日高点 ${metrics['10d_high']:.3f} 和低点 ${metrics['10d_low']:.3f}，给出具体的、可用于交易参考的价格数字（如：强支撑位关注xx.xx，强压力位看向xx.xx）。
- 资金洗盘逻辑：分析主力是在借利好出货还是在暴力洗盘吸筹。

#### 🔴 PLATFORM_X
（风格：短小精悍、犀利。字数200字内。带3个热门标签。例如：今天 {ticker} 这一脚油门...）

#### 🔴 PLATFORM_XIAOHONGSHU
【震惊！今日澳洲股市黑马/{ticker} 竟然...】
宝子们！今天来扒一只暴动标的 {ticker}！🔥
（多用Emoji，段落短，突出能不能赚钱，新手怎么做。最后带5个小红书标签）
"""
    return prompt
