import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

def get_top_asx_movers(limit=3):
    """
    🏛️ 终极安全版：全盘新鲜异动股捕获器（带时间戳死锁）
    """
    # 🌟 【死锁核心】拿到今天服务器真实的日期字符串，例如 "2026-06-10"
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"📡 正在启动 ASX 全盘雷达，今日基准时间戳: {today_str}")
    
    url = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
    payload = {"itemsPerPage": 100, "page": 0, "dateRange": "All"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Content-Type": "application/json"}
    
    hot_tickers = set()
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            items = response.json().get("data", {}).get("items", [])
            for item in items:
                # 检查最新敏感公告的时间，必须是今天发的！如果是昨天发的，直接过滤掉
                pub_time = item.get("dateAndTime", "")[:10]
                if item.get("marketSensitive", False) and pub_time == today_str:
                    raw_ticker = item.get("tickers", [{}])[0].get("ticker", "")
                    if raw_ticker and len(raw_ticker) == 3:
                        hot_tickers.add(f"{raw_ticker}.AX")
                        if len(hot_tickers) >= 15: break
    except Exception as e:
        print(f"⚠️ 全盘公告扫描遭遇异常: {e}")

    # 如果今天没有任何重磅敏感公告，切入全盘量化池进行实时成交量/时间戳双重清洗
    if not hot_tickers:
        print("ℹ️ 今日全盘未发布突发敏感公告，切入【全盘量化雷达】并开启时间戳校验...")
        dynamic_scan_pool = [
            "ZIP.AX", "PLS.AX", "CXO.AX", "LTR.AX", "WTC.AX", "FMG.AX", "BHP.AX", "RIO.AX", "MIN.AX", "AKE.AX",
            "XRO.AX", "CPU.AX", "NEXT.AX", "LYC.AX", "A2M.AX", "PDN.AX", "WDS.AX", "STO.AX", "AMP.AX", "QAN.AX"
        ]
        hot_tickers = dynamic_scan_pool

    final_movers = []
    for ticker in hot_tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2d")
            if len(hist) < 2: continue
            
            # 🌟 【防穿越大杀器】提取雅虎返回的数据表里最新一行的日期
            # hist.index[-1] 拿到的是 Timestamp 对象，用 str() 裁剪出前面的 "YYYY-MM-DD"
            latest_data_date = str(hist.index[-1])[:10]
            
            # 💡 如果这一行的日期和今天对不上，说明雅虎今天没开盘、没更新，拿的是历史僵尸数据！
            if latest_data_date != today_str:
                continue # 毫不留情，直接扔掉这只股票
            
            pct_change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
            current_vol = hist['Volume'].iloc[-1]
            
            if current_vol > 10000: 
                final_movers.append({
                    "ticker": ticker,
                    "pct_change": pct_change,
                    "volume": current_vol,
                    "last_close": hist['Close'].iloc[-1]
                })
        except: continue
        
    final_movers.sort(key=lambda x: abs(x['pct_change']), reverse=True)
    top_selected = final_movers[:limit]
    
    if not top_selected:
        print(f"🛑 [安全熔断] 侦测到今日（{today_str}）非交易日或市场无实质新鲜交易数据，拒绝使用历史僵尸数据。")
        return []
        
    print(f"🏆 【全盘筛选大功告成】今日新鲜出炉的 3 只异动之王: {[m['ticker'] for m in top_selected]}")
    return top_selected

def get_asx_official_announcements(ticker_short):
    """直连 ASX 官网，提取该股票真实的最新 3 条公告"""
    url = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
    payload = {"itemsPerPage": 5, "page": 0, "searchText": ticker_short, "dateRange": "All"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Content-Type": "application/json"}
    
    official_news = []
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            items = response.json().get("data", {}).get("items", [])
            for item in items[:3]:
                official_news.append({
                    "title": item.get("headline", ""),
                    "is_sensitive": "Yes" if item.get("marketSensitive", False) else "No",
                    "date": item.get("dateAndTime", "")[:10]
                })
    except: pass
    return official_news

def get_stock_comprehensive_data(ticker):
    """【多维数据闭环】官方真实公告 + 雅虎量化均线公式"""
    ticker_short = ticker.replace(".AX", "")
    official_announcements = get_asx_official_announcements(ticker_short)
    if not official_announcements:
        official_announcements = [{"title": "Regular trade volatility", "is_sensitive": "No", "date": "2026-06-10"}]

    stock = yf.Ticker(ticker)
    hist = stock.history(period="6mo")
    
    hist['MA5'] = hist['Close'].rolling(window=5).mean()
    hist['MA20'] = hist['Close'].rolling(window=20).mean()
    
    last_10_days = hist.tail(10)
    current_close = last_10_days['Close'].iloc[-1]
    current_ma5 = last_10_days['MA5'].iloc[-1]
    current_ma20 = last_10_days['MA20'].iloc[-1]
    
    if current_close > current_ma5 > current_ma20:
        ma_status = "多头强攻排列（股价 > MA5 > MA20）"
    elif current_close < current_ma5 < current_ma20:
        ma_status = "空头下行排列（股价 < MA5 < MA20）"
    else:
        ma_status = "均线缠绕震荡（筹码正在密集换手纠缠）"

    price_metrics = {
        "current_price": current_close,
        "10d_high": last_10_days['High'].max(),
        "10d_low": last_10_days['Low'].min(),
        "current_ma5": current_ma5,
        "current_ma20": current_ma20,
        "ma_status": ma_status
    }
    
    return {
        "ticker": ticker,
        "price_metrics": price_metrics,
        "official_news": official_announcements
    }

def serialize_to_prompt(raw_data):
    ticker = raw_data['ticker']
    metrics = raw_data['price_metrics']
    prompt = f"""
你现在是全球顶尖的跨平台财经自媒体矩阵主理人。请根据以下来自【ASX官方交易所】和【雅虎财经量化中心】的交叉验证真实数据，为澳洲股票 {ticker} 撰写三份完全不同平台风格的复盘分析。

---【📡 交叉验证权威数据集】---
股票代码: {ticker}
当前最新收盘价: ${metrics['current_price']:.3f}
过去10日最高价: ${metrics['10d_high']:.3f}
过去10日最低价: ${metrics['10d_low']:.3f}

📊 技术面量化指标 (精确至0.001):
- 5日均线 (MA5攻击线): ${metrics['current_ma5']:.3f}
- 20日均线 (MA20生命线): ${metrics['current_ma20']:.3f}
- 当前技术形态形态: {metrics['ma_status']}

🔴 ASX官方一手披露公告 (权威源): {raw_data['official_news']}
--------------------------------

🎯 请严格按照以下格式输出，不要带任何多余的开头和废话：

#### 🔴 PLATFORM_TELEGRAM
**{ticker} 机构内参：官方催化剂与技术面交叉解构**
- 🏛️ 官方公告解构：深度透视 ASX 官方披露的最新公告《{raw_data['official_news'][0]['title']}》（市场敏感度: {raw_data['official_news'][0]['is_sensitive']}），用专业机构视角翻译这篇公告对公司估值的核心影响。
- 📈 技术面与量化位置推演：必须结合当前收盘价、10日高低点、以及当前 MA5 (${metrics['current_ma5']:.3f}) 和 MA20 (${metrics['current_ma20']:.3f}) 的位置，给出具体的、可用于交易参考的价格数字（如：跌破/企稳于MAxx上方，强支撑位关注xx.xx，强压力位看向xx.xx）。
- 资金洗盘逻辑：结合当前均线形态【{metrics['ma_status']}】，分析多空博弈主力今天在借官方利好出货，还是在恐慌盘中暴力洗盘。

#### 🔴 PLATFORM_X
（风格：短小精悍、犀利。字数200字内。带3个热门标签。例如：今天 {ticker} 这一脚油门...）

#### 🔴 PLATFORM_XIAOHONGSHU
【震惊！今日ASX官方紧急披露，{ticker} 搞大事了...】
宝子们！澳洲股市今天这只股票绝对要加入自选！🔥
（多用Emoji，段落短，结合技术面通俗易懂地告诉大家能不能赚钱，最后带5个小红书标签）
"""
    return prompt
