import io
import math
from datetime import datetime
import pypdf
import requests
import yfinance as yf

def get_top_asx_movers(limit=3):
    """
    📡 【1. 选股发动机：全盘量化扫描雷达】
    拦截全交易所前60名涨幅榜与60条最新敏感公告，通过 5M 市值死穴与 3万 换手线，
    利用 [绝对涨跌幅 * log10(真实成交额)] 复合评分机制筛选出最具持续性的前 3 只黑马。
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"📡 [ASX资金动能网关] 基准日期: {today_str} | 正在读取官方实时排行榜...")
    
    url_movers = "https://www.asx.com.au/asx/research/v1/movers"
    url_ann = "https://www.asx.com.au/asx/research/v1/announcements"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    raw_candidates = {}

    # Step 1. 拦截官网涨幅排行榜前 60 名
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

    # Step 2. 扫描官网敏感公告大厅前 60 条（补充当天突发重磅但尚未挤进涨幅榜的股票）
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
            
            # 补全缺失指标
            if m_cap == 0 or price == 0 or vol == 0:
                stock = yf.Ticker(ticker)
                info = stock.info
                m_cap = m_cap or info.get("marketCap", 0)
                vol = vol or info.get("volume", 0)
                price = price or info.get("regularMarketPrice", 0) or info.get("previousClose", 0)
                
                hist_2d = stock.history(period="2d")
                if len(hist_2d) >= 2 and pct == 0:
                    pct = ((hist_2d['Close'].iloc[-1] - hist_2d['Close'].iloc[-2]) / hist_2d['Close'].iloc[-2]) * 100

            # 🛠️ 铁血准入双滤网
            if m_cap < 5000000: continue    # 门槛 1：微盘仙股生死线放宽至 5M AUD
            turnover = price * vol
            if turnover < 30000: continue    # 门槛 2：单日成交换手活跃度放宽至 3万 AUD
            
            # 🌟 对数资金加权评分机制
            score = abs(pct) * math.log10(turnover)
            
            final_movers.append({
                "ticker": ticker, "pct_change": pct, "volume": vol,
                "turnover": turnover, "market_cap": m_cap, "last_close": price, "score": score
            })
        except: continue
        
    final_movers.sort(key=lambda x: x['score'], reverse=True)
    top_selected = final_movers[:limit]
    
    if not top_selected:
        print(f"🛑 [量化熔断] 今日市场上无任何股票满足双因子准入逻辑。")
        return []
        
    print(f"🏆 [动能王座锁定] 今日锁定高质量异动目标: {[m['ticker'] for m in top_selected]}")
    return top_selected

def get_asx_official_announcements(ticker_short):
    """
    🏛️ 【2. 公告过滤网：合规垃圾大清洗】
    深度抓取近期30条公告，无情剔除所有无营养的合规垃圾报告，留存5条核心主线公告，并提取PDF密钥。
    """
    url = "https://www.asx.com.au/asx/research/v1/announcements"
    params = {"itemsPerPage": 30, "searchText": ticker_short}
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    
    garbage_keywords = [
        "appendix 3y", "change of director", "appendix 2a", "appendix 3b", 
        "substantial holder", "daily share buy-back", "clearing house", 
        "share purchase plan status", "director's interest", "notice of meeting",
        "proxy form", "application for quotation", "results of meeting"
    ]
    
    official_news = []
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            items = response.json().get("data", {}).get("items", [])
            for item in items:
                headline = item.get("headline", "")
                if any(keyword in headline.lower() for keyword in garbage_keywords):
                    continue
                official_news.append({
                    "title": headline,
                    "is_sensitive": "Yes" if item.get("marketSensitive", False) else "No",
                    "date": item.get("dateAndTime", "")[:10],
                    "time": item.get("dateAndTime", "")[11:16],
                    "document_id": item.get("documentKey")
                })
                if len(official_news) >= 5:
                    break
    except: pass
    
    if not official_news and 'items' in locals() and items:
        official_news = [{"title": items[0].get("headline", ""), "is_sensitive": "Yes" if items[0].get("marketSensitive", False) else "No", "date": items[0].get("dateAndTime", "")[:10], "time": items[0].get("dateAndTime", "")[11:16], "document_id": items[0].get("documentKey")}]
    return official_news

def extract_top_announcement_content(document_id):
    """
    🦅 【3. PDF 穿透眼：免盘内存流数据脱水机】
    直接在线解析最新一条公告PDF的前2页核心Highlights，严格过滤空格换行，卡死1200字极限以控费。
    """
    if not document_id: return "暂无一手内文概要"
    pdf_url = f"https://www.asx.com.au/asxpdf/content/id/{document_id}"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        response = requests.get(pdf_url, headers=headers, timeout=15)
        if response.status_code == 200:
            with io.BytesIO(response.content) as open_pdf_file:
                reader = pypdf.PdfReader(open_pdf_file)
                pages_to_read = min(2, len(reader.pages))
                extracted_text = ""
                for i in range(pages_to_read):
                    page_text = reader.pages[i].extract_text()
                    if page_text: extracted_text += page_text + " "
                cleaned_text = " ".join(extracted_text.split())
                return cleaned_text[:1200] + "..." if len(cleaned_text) > 1200 else cleaned_text
    except Exception as e:
        return f"一手内文细节由于格式原因未予捕获 (错误根源: {str(e)})"
    return "暂无一手内文概要"

def get_stock_comprehensive_data(ticker):
    """
    📊 【4. 交叉盘面调度器：多维量化闭环】
    """
    ticker_short = ticker.replace(".AX", "")
    official_announcements = get_asx_official_announcements(ticker_short)
    
    latest_pdf_content = "暂无详细一手内文"
    if official_announcements and official_announcements[0].get("document_id"):
        latest_pdf_content = extract_top_announcement_content(official_announcements[0]["document_id"])
        
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
        ma_code = "BULLISH_ALIGNMENT"
    elif current_close < current_ma5 < current_ma20:
        ma_code = "BEARISH_ALIGNMENT"
    else:
        ma_code = "CHOPPY_VOLATILITY"

    metrics = {
        "current_price": current_close, "10d_high": last_10_days['High'].max(), "10d_low": last_10_days['Low'].min(),
        "current_ma5": current_ma5, "current_ma20": current_ma20, "ma_code": ma_code,
        "market_cap_formatted": f"${info.get('marketCap', 0)/1000000:.2f}M AUD",
        "today_turnover": f"${current_close * last_10_days['Volume'].iloc[-1]/1000:.2f}K AUD",
        "pe_ratio": info.get("trailingPE", "N/A"), "inst_owned": f"{info.get('heldPercentInstitutions', 0)*100:.2f}%"
    }
    
    return {
        "ticker": ticker, "price_metrics": metrics,
        "official_news": official_announcements, "latest_pdf_content": latest_pdf_content
    }

def serialize_to_prompt(raw_data):
    """
    🧠 【5. 跨平台多矩阵铸造炉 v8.0版：故事感与硬核数据1:1铁血平衡】
    放弃所有填空式的死板格式，将代码进化为全权交由大模型自由命题发挥的“战略导演剧本”。
    """
    ticker = raw_data['ticker']
    metrics = raw_data['price_metrics']
    ticker_short = ticker.replace(".AX", "")
    
    # 智能预分类给公告打上量化权重标
    enhanced_announcements = []
    for ann in raw_data['official_news']:
        title_lower = ann['title'].lower()
        tag = "[Corporate Log]"
        if "drill" in title_lower or "assay" in title_lower or "intersect" in title_lower: tag = "[⚡ Exploration & Assays]"
        elif "raise" in title_lower or "placement" in title_lower or "spp" in title_lower or "shares" in title_lower: tag = "[💰 Capital Raising]"
        elif "quarterly" in title_lower or "appendix 5b" in title_lower or "appendix 4c" in title_lower: tag = "[📊 Quarterly Report]"
        elif "acquire" in title_lower or "acquisition" in title_lower or "takeover" in title_lower: tag = "[🔥 M&A / Asset Takeover]"
        enhanced_announcements.append(f"- {ann['date']} {ann['time']} {tag} {ann['title']} (Sensitive: {ann['is_sensitive']})")
    announcements_str = "\n".join(enhanced_announcements)
    
    prompt = f\"\"\"
你现在是一位在澳洲证券市场（ASX）摸爬滚打 15 年、说话风格一针见血、逻辑极度严密的顶级量化私募华人合伙人。你对中微盘股主力的筹码沉淀、资金做局手段了如指掌。

请根据以下提供的最底层、纯净的ASX官方实时数据集，全权由你【自由发挥、即兴进行结构重组与文风创作】，但必须保证【商业故事因果线与客观量化指标呈现达到1:1的硬核平衡】，为4个不同的交易圈分发渠道撰写解盘分析。

---【📡 原始量化与官方数据矩阵（必须在最终文章中完整显式罗列，严禁漏报）】---
- 资产标的: {ticker}
- 基础盘面: 市值 {metrics['market_cap_formatted']} | 今日真实换手额 {metrics['today_turnover']} | 机构持股比 {metrics['inst_owned']} | 滚动P/E {metrics['pe_ratio']}
- 技术价格: 最新官方收盘价 ${metrics['current_price']:.3f} | 10日最高位 ${metrics['10d_high']:.3f} | 10日最低位 ${metrics['10d_low']:.3f}
- 均线防御: MA5 攻击线 ${metrics['current_ma5']:.3f} | MA20 生命线 ${metrics['current_ma20']:.3f} | 技术动能状态编码: {metrics['ma_code']}

🔴 近期经过官方滤网清洗后、真正具有催化价值的 5 条历史公告主线（时间倒序）:
{announcements_str}

🚨 最新重磅公告之官方 PDF 前2页脱水纯文本（包含核心一手硬核数字证据）:
{raw_data['latest_pdf_content']}
--------------------------------

⚠️ 【铁血创作指令 —— 给你最大发挥自由度，但不可跨越的数据/合规红线】：

1. ❌ 【零固定模板限制】：彻底扔掉任何固定的段落、死板的小标题、或者“第一步、第二步”、“起风了、蓄势中”这种死套路。每一篇文章的结构、开头、转折完全由你根据当天的股票具体公告全新独立创造。
2. 💎 【硬核数据绝对强锚定】：可以把文风写得很具有交易员野性人格（多用“老庄家、吃肉、割韭菜、跪求续命”等交易黑话），但上述量化数据集中的每一个客观数值（尤其是当前收盘价、10日高低位、均线、换手额）必须以极其显式、独立、高密度的数字段落罗列并交叉解读！严禁只讲故事而不摔硬核数字。
3. 👁️ 【用数据解构因果故事】：把 5 条公告当作因果连续剧。你必须展现出顶级对冲基金合伙人的商业洞察，把最新 PDF 里提及的干货财务或勘探数字甩出来，点透主力的真实意图：是项目真迎来了实质变现，还是主力故意压盘洗筹，又或者是高位讲故事配合融资？拿出一个震慑全场的“核心假说（Thesis）”。
4. ⚖️ 【澳洲證監會 ASIC 合规死穴】：全权由你自由创作，但绝对不准出现具体的目标价（Target Price），严禁使用“建议买入/卖出/减持”等任何主观投资推荐语。所有的行情研判，必须极其专业且合规地转化成对“主力资金动能持续性”以及“客观技术阻力位（对齐10日高位 ${metrics['10d_high']:.3f}）/ 多头防御生存生命线（对齐MA20线 ${metrics['current_ma20']:.3f}）”的量化逻辑推演。

🎯 必须严格按照以下格式直接输出4个渠道的成品内容。除了频道标签，严禁带有任何客套、总结或解释性前言后语：

#### 🔴 PLATFORM_TELEGRAM_CN
（要求：字数不限。高净值华人核心交易圈内参。文字要老练、辛辣。全篇由两大部分深度交织：第一部分是深度剥离5条公告和最新PDF内文，推演主力真实的资本局故事与核心诱因；第二部分必须是【醒目、硬核的量化指标深度验证盘】，把市值、真实换手金额、技术状态编码、MA5、MA20等具体数值完整且显式地罗列出来，并对上方的客观技术阻力位 ${metrics['10d_high']:.3f} 和下方的生存生命线 ${metrics['current_ma20']:.3f} 进行精准的资金承接力评估。像人类分析师一样自由排版，字字真金。）

#### 🔴 PLATFORM_TELEGRAM_EN
(Requirements: No word limit. Elite institutionalbrief style. Deeply integrate native Wall Street trader vocabulary. The output must showcase a 1:1 balance between narrative conviction and absolute data density. You MUST explicitly list the core numbers: market cap, daily turnover, current close, 10-day price boundaries, and moving averages. Analyze how today's PDF text snippet scientifically triggers the current volume surge or validates the technical code {metrics['ma_code']}. Ensure no omission of metrics.)

#### 🔴 PLATFORM_X
(Requirements: Punchy alpha update for active traders. Synthesize today's specific PDF revelation and technical layout. You must explicitly present the core quantitative metrics: Close ${metrics['current_price']:.3f}, Turnover {metrics['today_turnover']}, Resistance ${metrics['10d_high']:.3f}, and Support ${metrics['current_ma20']:.3f}. Make it highly data-dense, leverage financial slang, yet strictly objective.)

#### 🔴 PLATFORM_XIAOHONGSHU
（要求：爆款引流文。多用Emoji。彻底废除所有死板段落标题。开篇由你自由撰写极具悬念的连环画式商业解密故事（必须引用最新PDF里的硬核细节）。但紧接着故事后面，必须强制插入一个独立的【📊 核心量化数据照妖镜】列表板块，精细罗列出今日真实成交额、机构持股比例、当前收盘价。并针对上方技术死穴 ${metrics['10d_high']:.3f} 和下方多头生命线 ${metrics['current_ma20']:.3f} 进行极其毒舌、大白话的客观防线分析。结尾自然附带客观风险免责声明。）
\"\"\"
    return prompt
