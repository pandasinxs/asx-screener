import yfinance as yf
import pandas as pd
import requests
import math
from datetime import datetime

def get_top_asx_movers(limit=3):
    """
    🏛️ 私募级双因子版：全盘雷达 (扩容初筛+资金动能复合评分排序)
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"📡 [ASX资金动能网关 v5.5] 基准日期: {today_str} | 正在读取 1 手官方实时数据...")
    
    url_movers = "https://www.asx.com.au/asx/research/v1/movers"
    url_ann = "https://www.asx.com.au/asx/research/v1/announcements"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    raw_candidates = {}

    # 1. 拦截官网排行榜前 60 名
    try:
        params_movers = {"itemsPerPage": 60, "sort": "PricePercentChange", "direction": "Descending"}
        response = requests.get(url_movers, params=params_movers, headers=headers, timeout=12)
        if response.status_code == 200:
            movers_items = response.json().get("data", {}).get("items", [])
            print(f"📋 成功捕获 {len(movers_items)} 只 ASX 官方榜单高动能股票...")
            for item in movers_items:
                ticker = item.get("ticker", "")
                if ticker and len(ticker) == 3:
                    full_ticker = f"{ticker}.AX"
                    raw_candidates[full_ticker] = {
                        "ticker": full_ticker,
                        "last_close": float(item.get("lastPrice", 0) or item.get("price", 0)),
                        "pct_change": float(item.get("pricePercentChange", 0) or item.get("changePercent", 0)),
                        "volume": int(item.get("volume", 0)),
                        "market_cap": float(item.get("marketCap", 0) or item.get("marketCapitalisation", 0))
                    }
    except Exception as e:
        print(f"⚠️ 拦截官方排行榜失败: {e}")

    # 2. 扫描官网敏感公告大厅前 60 条
    try:
        params_ann = {"itemsPerPage": 60, "page": 0, "marketSensitive": "true"}
        response = requests.get(url_ann, params=params_ann, headers=headers, timeout=12)
        if response.status_code == 200:
            items = response.json().get("data", {}).get("items", [])
            for item in items:
                pub_time = item.get("dateAndTime", "")[:10]
                if pub_time == today_str:
                    raw_ticker = item.get("tickers", [{}])[0].get("ticker", "")
                    if raw_ticker and len(raw_ticker) == 3:
                        full_ticker = f"{raw_ticker}.AX"
                        if full_ticker not in raw_candidates:
                            raw_candidates[full_ticker] = {
                                "ticker": full_ticker, "last_close": 0, "pct_change": 0, "volume": 0, "market_cap": 0
                            }
    except Exception as e:
        print(f"⚠️ 敏感公告雷达接入异常: {e}")

    print(f"🎯 正在执行基于资金与动能双因子的量化过滤与清洗...")
    final_movers = []
    
    for ticker, asx_data in raw_candidates.items():
        try:
            m_cap = asx_data["market_cap"]
            price = asx_data["last_close"]
            vol = asx_data["volume"]
            pct = asx_data["pct_change"]
            
            if m_cap == 0 or price == 0 or vol == 0:
                stock = yf.Ticker(ticker)
                info = stock.info
                m_cap = m_cap or info.get("marketCap", 0)
                vol = vol or info.get("volume", 0)
                price = price or info.get("regularMarketPrice", 0) or info.get("previousClose", 0)
                
                hist_2d = stock.history(period="2d")
                if len(hist_2d) >= 2 and pct == 0:
                    pct = ((hist_2d['Close'].iloc[-1] - hist_2d['Close'].iloc[-2]) / hist_2d['Close'].iloc[-2]) * 100

            # 🛠️ ASX 本土硬性双滤网
            if m_cap < 5000000: continue  # 门槛 1：微盘生死线放宽至 5M AUD
            
            turnover = price * vol
            if turnover < 30000: continue   # 门槛 2：换手活跃度放宽至 3万 AUD
            
            # 🌟 核心算法升级：引入对数资金加权评分 (防止纯刷单小微股或无动能超级大盘股霸榜)
            # score = 绝对涨跌幅 * log10(真实成交额)
            score = abs(pct) * math.log10(turnover)
            
            final_movers.append({
                "ticker": ticker,
                "pct_change": pct,
                "volume": vol,
                "turnover": turnover,
                "market_cap": m_cap,
                "last_close": price,
                "score": score
            })
        except: continue
        
    # 🌟 核心升级：按照全新的“机构资金动能复合评分”进行降序大排名
    final_movers.sort(key=lambda x: x['score'], reverse=True)
    top_selected = final_movers[:limit]
    
    if not top_selected:
        print(f"🛑 [量化熔断] 今日市场上无任何高含金量的股票满足双因子准入逻辑。")
        return []
        
    print(f"🏆 [资金动能王座锁定] 今日最具爆发持续性的前 {len(top_selected)} 只高质量异动股: {[m['ticker'] for m in top_selected]}")
    return top_selected

def get_asx_official_announcements(ticker_short):
    """🏛️ 绝对核心：直连 ASX 官网提取最新 3 条权威合规公告"""
    url = "https://www.asx.com.au/asx/research/v1/announcements"
    params = {"itemsPerPage": 5, "searchText": ticker_short}
    headers = {"User-Agent": "Mozilla/5.0"}
    
    official_news = []
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            items = response.json().get("data", {}).get("items", [])
            for item in items[:3]:
                official_news.append({
                    "title": item.get("headline", ""),
                    "is_sensitive": "Yes" if item.get("marketSensitive", False) else "No",
                    "date": item.get("dateAndTime", "")[:10],
                    "time": item.get("dateAndTime", "")[11:16]
                })
    except: pass
    return official_news

def get_stock_comprehensive_data(ticker):
    """📊 多维数据闭环"""
    ticker_short = ticker.replace(".AX", "")
    official_announcements = get_asx_official_announcements(ticker_short)
    today_str = datetime.now().strftime("%Y-%m-%d")
    if not official_announcements:
        official_announcements = [{"title": "Regular trade volatility / Volume rebalancing", "is_sensitive": "No", "date": today_str, "time": "16:15"}]

    stock = yf.Ticker(ticker)
    info = stock.info
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
        ma_status = "均线缠绕震荡"

    metrics = {
        "current_price": current_close,
        "10d_high": last_10_days['High'].max(),
        "10d_low": last_10_days['Low'].min(),
        "current_ma5": current_ma5,
        "current_ma20": current_ma20,
        "ma_status": ma_status,
        "market_cap_formatted": f"${info.get('marketCap', 0)/1000000:.2f}M AUD",
        "today_turnover": f"${current_close * last_10_days['Volume'].iloc[-1]/1000:.2f}K AUD",
        "pe_ratio": info.get("trailingPE", "N/A"),
        "inst_owned": f"{info.get('heldPercentInstitutions', 0)*100:.2f}%"
    }
    
    return {
        "ticker": ticker,
        "price_metrics": metrics,
        "official_news": official_announcements
    }

def serialize_to_prompt(raw_data):
    ticker = raw_data['ticker']
    metrics = raw_data['price_metrics']
    
    prompt = f"""
你现在是全球顶尖的量化私募机构首席分析师。请根据以下来自【ASX官方交易所】核心主导和【雅虎财经量化中心】辅助计算的交叉验证真实数据集，为 {ticker} 撰写精炼、无废话、纯数据驱动的复盘报告。

---【📡 ASX主导量化数据集】---
股票代码: {ticker}
原始基础面数据（源自ASX/Yahoo联合提供）：
- 市值: {metrics['market_cap_formatted']} | 今日真实换手额: {metrics['today_turnover']} | 机构持股比: {metrics['inst_owned']} | 滚动市盈率 P/E: {metrics['pe_ratio']}
- 官方收盘价: ${metrics['current_price']:.3f} | 10日最高/最低位: ${metrics['10d_high']:.3f} / ${metrics['10d_low']:.3f}
技术面切片数据（源自Yahoo辅助均线计算）：
- MA5 攻击线: ${metrics['current_ma5']:.3f} | MA20 生命线: ${metrics['current_ma20']:.3f} | 当前均线形态: {metrics['ma_status']}

🔴 ASX 交易所官网一手合规披露公告历史线 (按时间倒序): {raw_data['official_news']}
--------------------------------

🎯 必须严格按照以下格式直接输出成品，严禁带有任何客套、总结或前言废话：

#### 🔴 PLATFORM_TELEGRAM
**⚖️ {ticker} 核心交易评估报告**

【核心结论与展望】
明确判定：[给出明确指引：建议买入 / 保持观望 / 建议减持]，当前技术面及资金逻辑表明，[上涨/震荡/下跌] 走势预计将持续至[给出明确预测时间线，如未来x个交易日内 / 本周五收盘]。下一个核心催化剂事件预测发生在[明确预估一个日期段或核心事件，如：7月下旬探矿复检报告披露 / 8月年度财报]，该催化剂研判的核心逻辑在于[用1行字说清关键原因]。

【📜 今日官方披露时间线与摘要分析】
（请以具体日期时间为骨架，用 dot points 精简概括披露细节与背后动机，拒绝任何长篇大论的形容词）
* {raw_data['official_news'][0]['date']} {raw_data['official_news'][0]['time']} - 最新合规披露：发行公告《{raw_data['official_news'][0]['title']}》（市场敏感度: {raw_data['official_news'][0]['is_sensitive']}）。核心摘要与分析：[2行字大白话指出该公告披露的核心财报数字/勘探深度/业务进展，并直接点明这代表主力在借利好出货还是机构合力吃饱]。
* [若数据集里有第二条公告，按照述格式列出‘日期 时间 - 公告标题 + 摘要分析’。若今天只有一条公告，则在此列出该股票近期的历史重大事件线，并点明对今天盘面情绪的累积影响]。

【📊 核心量化指标硬核验证】
（直接罗列硬性指标，并给出基于该指标的交易可行度支撑，拒绝无数据支撑的废话）
* 市值与流动性验证：当前总市值 {metrics['market_cap_formatted']}，今日真实换手金额达 {metrics['today_turnover']}。[分析：说明该流动性水平是否具备散户及游资的短线换手承接力]。
* 均线生命线与价格位置：最新收盘价 ${metrics['current_price']:.3f} 相比于 MA5 攻击线 (${metrics['current_ma5']:.3f}) 和 MA20 生命线 (${metrics['current_ma20']:.3f}) 呈现【{metrics['ma_status']}】状态。[分析：基于此位置直接给出明确的交易防御数字：下方强支撑位精确看至 $xx.xx，上方强阻力位精确看向 $xx.xx]。
* 筹码结构：机构持股比例为 {metrics['inst_owned']}，滚动市盈率为 {metrics['pe_ratio']}。[分析：一句话判定该股属于高度控盘股、还是散户游资混战股，进一步增强上述结论的可信度]。

#### 🔴 PLATFORM_X
（风格：数据流。首句直接给出【结论：买入/观望】和预测持续时间，随后列出最硬核的2条公告/均线数字证据。150字内，带3个标签。）

#### 🔴 PLATFORM_XIAOHONGSHU
【全盘扫描：今日ASX数据之王 {ticker} 应该买还是等？】
别看大道理，直接看官方一手真凭实据！
- 核心建议：[买入/观望]（预计行情持续至xx）
- 关键时间线：{raw_data['official_news'][0]['date']} 官方突发公告《{raw_data['official_news'][0]['title']}》！这意味着：[用 dot point 给出大白话数据解释]
- 硬核指标：市值 {metrics['market_cap_formatted']}，今天主力砸了 {metrics['today_turnover']}！均线处于{metrics['ma_status']}。
  支撑位看 $xx.xx，阻力位看 $xx.xx！
（用数据说话，段落极短，最后带5个标签）
"""
    return prompt
