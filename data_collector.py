import io
import os
from datetime import datetime
import requests
import yfinance as yf
import pypdf  # 💡 确保已运行: pip install pypdf

def get_asx_official_announcements(ticker_short):
    """
    🏛️ 【公告控制塔】30条深度抓取 + 垃圾公告硬核清洗过滤网
    """
    url = "https://www.asx.com.au/asx/research/v1/announcements"
    params = {"itemsPerPage": 30, "searchText": ticker_short}
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    
    # ❌ 铁血黑名单：只要标题包含这些无营养的合规垃圾词，直接就地过滤
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
                headline_lower = headline.lower()
                
                # 执行黑名单洗网
                if any(keyword in headline_lower for keyword in garbage_keywords):
                    continue
                
                official_news.append({
                    "title": headline,
                    "is_sensitive": "Yes" if item.get("marketSensitive", False) else "No",
                    "date": item.get("dateAndTime", "")[:10],
                    "time": item.get("dateAndTime", "")[11:16],
                    "document_id": item.get("documentKey")  # 用于下载PDF的唯一密钥
                })
                
                # 🎯 积攒够 5 条高含金量硬核主线公告即收网，防止塞爆 Prompt
                if len(official_news) >= 5:
                    break
    except Exception as e:
        print(f"[-] ASX公告抓取网络异常: {str(e)}")
    
    # 🪹 兜底机制：如果洗完发现一条硬核的都没有，说明这票近期确实在躺平，直接返回最近的 1 条
    if not official_news and 'items' in locals() and items:
        official_news = [{
            "title": items[0].get("headline", ""),
            "is_sensitive": "Yes" if items[0].get("marketSensitive", False) else "No",
            "date": items[0].get("dateAndTime", "")[:10],
            "time": items[0].get("dateAndTime", "")[11:16],
            "document_id": items[0].get("documentKey")
        }]
        
    return official_news

def extract_top_announcement_content(document_id):
    """
    🦅 【PDF 穿透眼】直接潜入ASX底层，免盘下载最新PDF并强行提取前2页核心Highlights
    """
    if not document_id:
        return "暂无一手内文概要"
        
    pdf_url = f"https://www.asx.com.au/asxpdf/content/id/{document_id}"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    
    try:
        response = requests.get(pdf_url, headers=headers, timeout=15)
        if response.status_code == 200:
            # 💡 直接在内存中以流的形式打开PDF，0磁盘IO，速度极快
            with io.BytesIO(response.content) as open_pdf_file:
                reader = pypdf.PdfReader(open_pdf_file)
                
                # 铁血防御：只读前 2 页（Highlights密集区），卡死 Token 成本
                pages_to_read = min(2, len(reader.pages))
                extracted_text = ""
                for i in range(pages_to_read):
                    page_text = reader.pages[i].extract_text()
                    if page_text:
                        extracted_text += page_text + " "
                
                # 粗暴清洗：压缩无用换行与空格
                cleaned_text = " ".join(extracted_text.split())
                # 最终安全截取前 1200 个字符（约 200 英文单词），双保险控费
                return cleaned_text[:1200] + "..." if len(cleaned_text) > 1200 else cleaned_text
    except Exception as e:
        return f"一手内文细节由于格式或底层网络原因未予捕获 (错误根源: {str(e)})"
    return "暂无一手内文概要"

def get_stock_comprehensive_data(ticker):
    """
    📊 【量化多维闭环】ASX原生数据主导，状态编码化，挂载PDF脱水内文
    """
    ticker_short = ticker.replace(".AX", "")
    
    # 1. 拦截并处理公告数据链
    official_announcements = get_asx_official_announcements(ticker_short)
    
    latest_pdf_content = "暂无详细一手内文"
    if official_announcements and official_announcements[0].get("document_id"):
        # ⚡ 核心质变：穿透提取最新一条核心公告的 PDF 前两页干货内文
        latest_pdf_content = extract_top_announcement_content(official_announcements[0]["document_id"])

    # 2. 调度 Yahoo Finance 补充计算量化因子
    stock = yf.Ticker(ticker)
    info = stock.info
    hist = stock.history(period="6mo")
    
    hist['MA5'] = hist['Close'].rolling(window=5).mean()
    hist['MA20'] = hist['Close'].rolling(window=20).mean()
    
    last_10_days = hist.tail(10)
    current_close = last_10_days['Close'].iloc[-1]
    current_ma5 = last_10_days['MA5'].iloc[-1]
    current_ma20 = last_10_days['MA20'].iloc[-1]
    
    # 技术状态代码化，严禁AI主观判定
    if current_close > current_ma5 > current_ma20:
        ma_code = "BULLISH_ALIGNMENT"
    elif current_close < current_ma5 < current_ma20:
        ma_code = "BEARISH_ALIGNMENT"
    else:
        ma_code = "CHOPPY_VOLATILITY"

    metrics = {
        "current_price": current_close,
        "10d_high": last_10_days['High'].max(),
        "10d_low": last_10_days['Low'].min(),
        "current_ma5": current_ma5,
        "current_ma20": current_ma20,
        "ma_code": ma_code,
        "market_cap_formatted": f"${info.get('marketCap', 0)/1000000:.2f}M AUD",
        "today_turnover": f"${current_close * last_10_days['Volume'].iloc[-1]/1000:.2f}K AUD",
        "pe_ratio": info.get("trailingPE", "N/A"),
        "inst_owned": f"{info.get('heldPercentInstitutions', 0)*100:.2f}%"
    }
    
    return {
        "ticker": ticker,
        "price_metrics": metrics,
        "official_news": official_announcements,
        "latest_pdf_content": latest_pdf_content
    }

def serialize_to_prompt(raw_data):
    """
    🧠 【多矩阵内容铸造炉】精准对接4大终端，注入PDF核心前瞻，铁血防脑补与合规盾牌
    """
    ticker = raw_data['ticker']
    metrics = raw_data['price_metrics']
    ticker_short = ticker.replace(".AX", "")
    
    # 构建带量化预分类标签的公告简易路线图
    enhanced_announcements = []
    for ann in raw_data['official_news']:
        title_lower = ann['title'].lower()
        tag = "【经营动态】"
        if "drill" in title_lower or "assay" in title_lower or "intersect" in title_lower:
            tag = "【⚡ 核心勘探/技术重大进展】"
        elif "raise" in title_lower or "placement" in title_lower or "spp" in title_lower or "shares" in title_lower:
            tag = "【💰 资本运作/定向增发融资】"
        elif "quarterly" in title_lower or "appendix 5b" in title_lower or "appendix 4c" in title_lower:
            tag = "【📊 季度法定财报/现金流汇报】"
        elif "acquire" in title_lower or "acquisition" in title_lower or "takeover" in title_lower:
            tag = "【🔥 战略兼并/重大资产收购】"
            
        enhanced_announcements.append(f"- {ann['date']} {ann['time']} {tag} 《{ann['title']}》(市场敏感度: {ann['is_sensitive']})")
        
    announcements_str = "\n".join(enhanced_announcements)
    
    prompt = f"""
你现在是拥有顶级法律合规与数据严谨意识的跨国量化私募机构首席分析师。请根据以下来自【ASX官方交易所】核心主导的真实硬核数据集，为 {ticker} 撰写4个不同渠道平台的复盘报告。

---【📡 ASX主导量化数据集】---
股票代码: {ticker}
- 市值: {metrics['market_cap_formatted']} | 今日真实换手额: {metrics['today_turnover']} | 机构持股比: {metrics['inst_owned']} | 滚动市盈率 P/E: {metrics['pe_ratio']}
- 官方收盘价: ${metrics['current_price']:.3f} | 10日最高/最低位: ${metrics['10d_high']:.3f} / ${metrics['10d_low']:.3f}
- MA5 攻击线: ${metrics['current_ma5']:.3f} | MA20 生命线: ${metrics['current_ma20']:.3f} | 均线编码: {metrics['ma_code']}

🔴 ASX 交易所官网经过【量化滤网清洗】后、真正具有核心资讯价值的 5 条历史公告主线 (按时间倒序): 
{announcements_str}

🚨【核心突发】最新第一条敏感公告之官方PDF前2页脱水提炼内文（包含关键数字证据，若显示暂无则仅依标题推理）:
{raw_data['latest_pdf_content']}
--------------------------------

⚠️ 铁血防脑补与 ASIC 澳洲证监会合规约束：
1. 🛡️【使用防御性语言】：严禁无端联想因果。对无法百分之百确定的资本动机，必须使用“迹象表明”、“可能旨在”、“通常意味着”等中性专业词汇，严禁编造阴果故事。必须结合 PDF 提炼内文里的真实数据，禁止捏造财务或勘探数字。
2. ❌【法律合规红线】：严禁给出目标价格（Target Price）及具体的推荐买入/卖出（Buy/Sell Recommendation）等主观投资建议。所有价格预测转化为“技术性阻力位”或“数据支撑位”。

🎯 必须严格按照以下格式直接输出成品，严禁带有任何客套、总结或前言废话：

#### 🔴 PLATFORM_TELEGRAM_CN
**⚖️ {ticker} 核心交易评估报告 (中文版)**

【核心动能研判】
资金趋势：[研判当前是 强力多头攻势 / 资金流出防御 / 窄幅区间震荡]，当前技术形态预示此动能有望延续至[给出明确预测时间线，如：未来3个交易日内 / 本周五收盘]。下一个核心催化剂事件预计聚焦于[结合公告预估下一个大事件]，其量化核心传导逻辑在于[用1行字说清关键原因]。

【📜 官方披露时间线与量化演进】
* 历史主线复盘：纵观该股近期的一系列合规披露，其核心脉络由以下节点构成：从早前的《[提及列表中较早的核心公告标题]》展现出的动作，到后续在《[提及中间的关键公告标题]》中的态势发展，最终延伸至今日的最新敏感披露《{raw_data['official_news'][0]['title']}》。这表明在客观层面上，资金近期正在围绕[结合打标分类与PDF提取的Highlights细节，用两句话客观解构这一系列事件是在推进主营业务，还是在进行资本层面的筹码洗牌，严禁瞎编故事]。

【📊 核心量化指标硬核验证】
* 流动性承接：总市值 {metrics['market_cap_formatted']}，今日真实成交换手达 {metrics['today_turnover']}。[客观分析此换手率是否具备游资和机构的短线换手承接力]。
* 均线防御防线：当前官方收盘价为 ${metrics['current_price']:.3f}。上方近期技术阻力位对齐10日高点 **${metrics['10d_high']:.3f}**，下方关键技术支撑位看死 MA20 生命线 **${metrics['current_ma20']:.3f}**。
* 筹码结构：机构持股比例 {metrics['inst_owned']}，滚动市盈率 {metrics['pe_ratio']}。[一句话判定该股属于高度控盘股还是散户游资混战股]。

#### 🔴 PLATFORM_TELEGRAM_EN
**⚖️ {ticker} Quantitative Assessment Report (English Edition)**

[Momentum & Trend Outlook]
Market Stance: [State clearly: Strong Bullish Momentum / Bearish Risk Management / Sideways Consolidation] until [Timeframe]. The next pivotal catalyst is projected around [Core event based on news].

[📜 ASX Official Disclosure Timeline & Narrative Arc]
* Historical Trajectory: Reviewing the sequence of the 5 filtered official disclosures leading up to today's market-sensitive release "{raw_data['official_news'][0]['title']}" combined with its official first-hand text snippet, the company has built a clear operational milestone. This demonstrates a transition from early foundational steps to the current release, revealing that smart money is currently executing a strategic [Explain the macro story: institutional re-rating / operational expansion / near-term capital cycling based on the data provided].

[📊 Quantitative Metrics Verification]
* Liquidity Cap: Market Cap at {metrics['market_cap_formatted']} with today's turnover at {metrics['today_turnover']}.
* Technical Boundaries: Last close at ${metrics['current_price']:.3f}. Resistance is aligned with the 10-day high at **${metrics['10d_high']:.3f}**, while the defensive support rests on the MA20 line at **${metrics['current_ma20']:.3f}**.
* Structure: Institutional ownership stands at {metrics['inst_owned']} with a trailing P/E of {metrics['pe_ratio']}.

#### 🔴 PLATFORM_X
📊 #{ticker_short} Quant Flow Update (Pure English)
Trend: [Strong Bullish / Capital Defense] till [Timeline]
📡 Catalyst Arc: Today's sensitive release "{raw_data['official_news'][0]['title']}" concludes a multi-week operational milestone. 
💰 Turnover: {metrics['today_turnover']} with clear [Institutional Accumulation / Distribution] indicators.
📈 Tech Levels: Close ${metrics['current_price']:.3f}. Objective resistance at ${metrics['10d_high']:.3f} | Support at ${metrics['current_ma20']:.3f}.
#ASX #AusShares #{ticker_short}

#### 🔴 PLATFORM_XIAOHONGSHU
【📌 今日ASX异动暴风眼 {ticker}：串联官方前因后果，看清主力底牌！】

🔥 拒绝单看一天数据断章取义！今天直接用量化滤网清洗掉所有毫无营养的例行公事报告，揪出了 {ticker} 近期真正具有核弹级资讯价值的 5 条官方历史合规公告！更绝的是，我们已经穿透了今天最新公告的官方PDF内文！带你像看连环画一样，过瘾地把这个商业故事链娓娓道来：

📖【回溯：连环拼图拼出真实轨迹】
1️⃣ 起风了：{raw_data['official_news'][-1]['date']} 官方披露《{raw_data['official_news'][-1]['title']}》，项目底层逻辑开始悄悄发生质变。
2️⃣ 蓄势中：随后紧接着跟进动态，整个市场的资金情绪开始大面积发酵。
3️⃣ 进展线：就在今天，官方突发重磅敏感公告《{raw_data['official_news'][0]['title']}》彻底引爆市场！
🤯 这一套行云流水的组合拳打下来，结合官方PDF里透露的硬核细节来看，背后的真实剧本【专业推演】是：[结合公告前面的预打标分类以及最新PDF提炼内文里的真实字眼，用3行字讲出你的独到见解。注意：如果前后公告属于同一个项目，说明这是一次连贯的战略推进；如果属于不同事件，则指出这是多条战线并行的结果。多用“迹象表明/可能意味着”，展现严谨私募大牛的逼格！]

📊【硬核合规数据照妖镜】
- 💰 资金肉搏：今天大资金真真切切砸了 {metrics['today_turnover']}！大资金换手承接力十分瞩目。
- 📈 筹码防御：当前收盘价 ${metrics['current_price']:.3f}，技术面对应编码【{metrics['ma_code']}】。
- 🎯 客观防线：**根据10日动能和均线测算，上方近期技术阻力位看死 **${metrics['10d_high']:.3f}**，下方关键技术支撑防线看死 **${metrics['current_ma20']:.3f}**。**（数据源自官方客观记录，注意防御风险！）

#澳洲股票 #ASX #澳洲搞钱 #量化交易 #商业故事
"""
    return prompt
