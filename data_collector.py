import io
import math
from datetime import datetime
import pypdf
import requests
import yfinance as yf

# =========================
# 🌐 API ABSTRACTION LAYER
# =========================

class APIClient:
    """
    🔒 统一数据入口 + 强制Markit优先 + fallback机制
    """

    MARKIT_BASE = "https://asx.api.markitdigital.com"
    ASX_FALLBACK = "https://www.asx.com.au"

    session = requests.Session()

    @staticmethod
    def _safe_get(url, params=None, timeout=12):
        try:
            r = APIClient.session.get(url, params=params, timeout=timeout)
            return r
        except Exception as e:
            print("API error:", e)
            return None

    # -------------------------
    # 📡 Movers
    # -------------------------
    @staticmethod
    def get_movers():
        endpoints = [
            f"{APIClient.MARKIT_BASE}/asx-research/1.0/markets/movers",
            f"{APIClient.ASX_FALLBACK}/asx/research/v1/movers"
        ]

        for url in endpoints:
            r = APIClient._safe_get(url)

            if not r or r.status_code != 200:
                continue

            try:
                data = r.json()
                items = data.get("data", {}).get("items", [])
                if not items:
                    items = data.get("data", {}).get("results", [])

                return items
            except:
                continue

        return []

    # -------------------------
    # 📢 Announcements
    # -------------------------
    @staticmethod
    def get_announcements(search_text):
        endpoints = [
            f"{APIClient.MARKIT_BASE}/asx-research/1.0/announcements",
            f"{APIClient.ASX_FALLBACK}/asx/research/v1/announcements"
        ]

        params = {
            "itemsPerPage": 30,
            "searchText": search_text
        }

        for url in endpoints:
            r = APIClient._safe_get(url, params=params)

            if not r or r.status_code != 200:
                continue

            try:
                data = r.json()
                items = data.get("data", {}).get("items", [])
                if not items:
                    items = data.get("data", {}).get("results", [])

                return items
            except:
                continue

        return []

    # -------------------------
    # 📄 PDF
    # -------------------------
    @staticmethod
    def get_pdf(document_id):
        if not document_id:
            return None

        url = f"{APIClient.MARKIT_BASE}/asxpdf/content/id/{document_id}"

        r = APIClient._safe_get(url, timeout=15)

        if not r or r.status_code != 200:
            return None

        return r.content


# =========================
# 🧠 UTILITIES
# =========================

def safe_float(x, default=0.0):
    try:
        return float(x) if x else default
    except:
        return default


def safe_int(x, default=0):
    try:
        return int(x) if x else default
    except:
        return default


def sanitize_text(text, limit=1200):
    if not text:
        return ""

    blacklist = ["ignore previous", "system prompt", "act as", "disregard"]

    text = str(text)

    for b in blacklist:
        text = text.replace(b, "")

    return " ".join(text.split())[:limit]


def tag_announcement(title):
    t = title.lower()

    if any(x in t for x in ["drill", "assay", "intersect"]):
        return "EXPLORATION_STRONG"
    if any(x in t for x in ["raise", "placement", "spp"]):
        return "CAPITAL_EVENT"
    if any(x in t for x in ["acquire", "takeover"]):
        return "M&A_EVENT"
    if "quarterly" in t:
        return "FINANCIAL_UPDATE"
    return "OTHER"


# =========================
# 📊 CORE: MOVERS
# =========================

def get_top_asx_movers(limit=3):

    items = APIClient.get_movers()

    raw = {}

    for i in items:
        t = i.get("ticker", "")
        if t and len(t) == 3:
            raw[f"{t}.AX"] = {
                "ticker": f"{t}.AX",
                "price": safe_float(i.get("lastPrice") or i.get("price")),
                "pct": safe_float(i.get("pricePercentChange") or i.get("changePercent")),
                "vol": safe_int(i.get("volume")),
                "mcap": safe_float(i.get("marketCap"))
            }

    results = []

    for ticker, d in raw.items():
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            hist = stock.history(period="2d")
            if hist is None or len(hist) < 2:
                continue

            price = d["price"] or safe_float(info.get("regularMarketPrice"))
            vol = d["vol"] or safe_int(info.get("volume"))
            mcap = d["mcap"] or safe_float(info.get("marketCap"))
            pct = d["pct"]

            if pct == 0:
                pct = ((hist["Close"].iloc[-1] - hist["Close"].iloc[-2]) /
                       hist["Close"].iloc[-2]) * 100

            if mcap < 5_000_000:
                continue

            turnover = price * vol
            if turnover < 30000:
                continue

            score = abs(pct) * math.log10(turnover)

            results.append({
                "ticker": ticker,
                "price": price,
                "pct": pct,
                "vol": vol,
                "mcap": mcap,
                "turnover": turnover,
                "score": score
            })

        except:
            continue

    return sorted(results, key=lambda x: x["score"], reverse=True)[:limit]


# =========================
# 📢 ANNOUNCEMENTS
# =========================

def get_asx_official_announcements(ticker_short):

    items = APIClient.get_announcements(ticker_short)

    out = []

    for i in items:
        title = i.get("headline", "")

        if any(x in title.lower() for x in ["appendix", "notice", "director"]):
            continue

        out.append({
            "title": title,
            "tag": tag_announcement(title),
            "date": i.get("dateAndTime", "")[:10],
            "time": i.get("dateAndTime", "")[11:16],
            "document_id": i.get("documentKey")
        })

        if len(out) >= 5:
            break

    return out


# =========================
# 📄 PDF EXTRACT
# =========================

def extract_pdf(document_id):

    content = APIClient.get_pdf(document_id)

    if not content:
        return ""

    try:
        with io.BytesIO(content) as f:
            pdf = pypdf.PdfReader(f)

            text = ""

            for i in range(min(2, len(pdf.pages))):
                t = pdf.pages[i].extract_text()
                if t:
                    text += t + " "

            return sanitize_text(text)

    except:
        return ""


# =========================
# 📊 DATA BUILDER
# =========================

def get_stock_comprehensive_data(ticker):

    short = ticker.replace(".AX", "")

    news = get_asx_official_announcements(short)

    pdf = ""
    if news and news[0].get("document_id"):
        pdf = extract_pdf(news[0]["document_id"])

    stock = yf.Ticker(ticker)
    info = stock.info or {}

    hist = stock.history(period="6mo")

    if hist is None or len(hist) < 25:
        return {}

    hist["MA5"] = hist["Close"].rolling(5).mean()
    hist["MA20"] = hist["Close"].rolling(20).mean()
    hist = hist.dropna()

    last = hist.tail(20)

    close = safe_float(hist["Close"].iloc[-1])
    ma5 = safe_float(hist["MA5"].iloc[-1])
    ma20 = safe_float(hist["MA20"].iloc[-1])

    if close > ma5 > ma20:
        state = "BULL"
    elif close < ma5 < ma20:
        state = "BEAR"
    else:
        state = "CHOP"

    inst = info.get("heldPercentInstitutions")

    metrics = {
        "current_price": close,
        "10d_high": safe_float(last["High"].max()),
        "10d_low": safe_float(last["Low"].min()),
        "current_ma5": ma5,
        "current_ma20": ma20,
        "ma_code": state,
        "market_cap_formatted": f"${safe_float(info.get('marketCap'))/1e6:.2f}M AUD",
        "today_turnover": f"${safe_float(close * last['Volume'].iloc[-1]):,.0f} AUD",
        "pe_ratio": info.get("trailingPE", "N/A"),
        "inst_owned": f"{(inst*100) if inst else 0:.2f}%"
    }

    return {
        "ticker": ticker,
        "price_metrics": metrics,
        "official_news": news,
        "latest_pdf_content": pdf
    }


# =========================
# 🧠 PROMPT (UNCHANGED STRUCTURE)
# =========================

def serialize_to_prompt(raw_data):

    metrics = raw_data["price_metrics"]

    news = [
        f"- [{n['tag']}] {n['date']} {n['time']} {n['title']}"
        for n in raw_data["official_news"]
    ]

    pdf = sanitize_text(raw_data.get("latest_pdf_content", ""))

    prompt = f"""
你现在是一位在澳洲证券市场（ASX）摸爬滚打 15 年、说话风格一针见血、逻辑极度严密的顶级量化私募华人合伙人。你对中微盘股主力的筹码沉淀、资金做局手段了如指掌。
请根据以下提供的最底层、纯净的ASX官方实时数据集，全权由你【自由发挥、即兴进行结构重组与文风创作】，但必须保证【商业故事因果线与客观量化指标呈现达到1:1的硬核平衡】，为4个不同的交易圈分发渠道撰写解盘分析。
---【📡 原始量化与官方数据矩阵（必须在最终文章中完整显式罗列，严禁漏报）】---
TICKER: {raw_data['ticker']}
PRICE: {metrics['current_price']}
MA5: {metrics['current_ma5']}
MA20: {metrics['current_ma20']}
MC: {metrics['market_cap_formatted']}
TURNOVER: {metrics['today_turnover']}
PDF: {pdf}

NEWS:
{chr(10).join(news)}

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
（要求：爆款引流文。多用Emoji。彻底废除所有死板段落标题。开篇由你自由撰写极极具悬念的连环画式商业解密故事（必须引用最新PDF里的硬核细节）。但紧接着故事后面，必须强制插入一个独立的【📊 核心量化数据照妖镜】列表板块，精细罗列出今日真实成交额、机构持股比例、当前收盘价。并针对上方技术死穴 ${metrics['10d_high']:.3f} 和下方多头生命线 ${metrics['current_ma20']:.3f} 进行极其毒舌、大白话的客观防线分析。结尾自然附带客观风险免责声明。）
"""
    return prompt
