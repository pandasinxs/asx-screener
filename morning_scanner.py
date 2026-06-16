# ============================================================
# FIRST PULLBACK — MORNING SCANNER v5
# 升级点:
#   1. 重试逻辑覆盖所有可重试异常（网络/超时/5xx/429）
#   2. 筛选指标全面强化（价格下限、量能门槛、VWAP距离）
#   3. 三阶段流程：粗筛 → 新闻时间线+历史指标精筛 → Gemini综合分析
#   4. Gemini输出结构化JSON，Telegram格式化呈现
# ============================================================

import os
import re
import io
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import numpy as np
import yfinance as yf
import pandas as pd
import requests
from google import genai

# ── 日志配置 ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("morning_scanner.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# yfinance 会把 "possibly delisted" 这类正常的数据缺失打成 ERROR 级别输出到
# 根 logger，在我们的日志里产生大量噪音。把它压制到 WARNING 以下即可。
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

# ── 环境变量 ─────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

GEMINI_MODEL = "gemini-2.5-flash"
gemini_client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

# 模块级公告缓存：由 run_morning_scan() 注入，供 get_stock_news_timeline() 使用。
# 避免把72小时全市场公告dict作为参数层层传递。
_ann_map_cache: dict = {}

ASX_ANN_URL = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json",
    "Referer": "https://www.asx.com.au",
}

# ── 筛选参数（集中管理，便于调优）───────────────────────────
FILTER = {
    "min_price"         : 0.05,    # 最低股价，过滤仙股
    "max_price"         : 20.0,    # 最高股价，过滤大盘价股（流动性差）
    "min_change_pct"    : 10.0,    # 最低涨幅%
    "max_change_pct"    : 60.0,    # 最高涨幅%：>60%的通常已无追入空间
    "min_vol_ratio"     : 1.5,     # 今日量 / 20日均量：必须明显放量
    "min_dollar_volume" : 500_000, # 最低日换手金额（流动性门槛，原300k偏低）
    "max_vwap_dist_pct" : 5.0,     # 当前价距VWAP的最大距离%（追高过度则过滤）
    "min_history_days"  : 20,      # 日线最少需要多少天数据
}


# ============================================================
# 通用重试装饰器（覆盖所有可重试异常）
# ============================================================

# 可重试的异常类型（网络、超时、服务端错误）
RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ConnectionResetError,
    TimeoutError,
)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    安全地从 DataFrame 中提取一列并确保返回 pd.Series。

    问题根因：yfinance 在批量下载后，如果 batch 内只有 1 只股票，
    raw[ticker][col].squeeze() 会把单行 DataFrame 挤压成 numpy 标量，
    导致后续的 .iloc / .mean() / .sum() 等调用全部 AttributeError。

    解决方案：统一通过此函数提取列，保证返回类型始终是 pd.Series。
    """
    s = df[col]
    # 如果已经是 Series，直接返回
    if isinstance(s, pd.Series):
        return s
    # DataFrame（多列同名的极端情况），取第一列
    if isinstance(s, pd.DataFrame):
        return s.iloc[:, 0]
    # 标量（单行被 squeeze 的情况）：包装成长度为1的 Series
    return pd.Series([s])


def gemini_call_with_retry(
    prompt: str,
    max_retries: int = 10,
    retry_interval: int = 30,
) -> str:
    """
    调用Gemini，对所有可重试异常（429限速/网络中断/服务端错误）
    执行重试逻辑：每次间隔retry_interval秒，最多max_retries次。
    成功立即返回，不会无限循环。
    """
    if not gemini_client:
        return ""

    for attempt in range(1, max_retries + 1):
        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )
            return response.text.strip()

        except Exception as e:
            err_str = str(e).lower()

            # 判断是否属于可重试类型
            is_rate_limit  = "429" in err_str or "resource_exhausted" in err_str
            is_server_err  = any(str(code) in err_str for code in [500, 502, 503, 504])
            is_network_err = any(
                keyword in err_str
                for keyword in ["connection", "timeout", "reset", "broken pipe", "eof"]
            )
            is_retryable = is_rate_limit or is_server_err or is_network_err

            if is_retryable and attempt < max_retries:
                reason = (
                    "限速(429)" if is_rate_limit
                    else "服务端错误" if is_server_err
                    else "网络异常"
                )
                log.warning(
                    f"Gemini {reason}，第{attempt}/{max_retries}次重试，"
                    f"{retry_interval}秒后继续... 错误: {str(e)[:80]}"
                )
                time.sleep(retry_interval)
            else:
                if attempt >= max_retries:
                    log.error(f"Gemini连续{max_retries}次失败，放弃。最后错误: {e}")
                else:
                    # 不可重试的错误（鉴权失败、prompt违规等），立即放弃
                    log.error(f"Gemini不可重试错误: {e}")
                return ""

    return ""


# ============================================================
# 数据获取层
# ============================================================

# ============================================================
# 新闻情报层 v2 — 设计原则：
#   "标题只是门牌，正文才是房间，事件链才是故事"
#
#   核心升级（对比v1）：
#   1. Google News redirect修复：RSS link是google转跳链接，
#      必须follow redirect才能拿到真实URL，否则fetch到的是google页面
#   2. 新闻质量门控：候选股必须有≤2天内的新闻/公告，否则drop
#   3. 事件链标注：对历史新闻自动分类
#      trigger（直接催化剂）/ followup（后续发酵）/
#      background（背景积累）/ analyst（机构解读）
#   4. 标题去噪：过滤与股票无关的通用市场新闻
# ============================================================

# PDF下载地址模板（documentKey来自公告API）
PDF_DL_BASE = (
    "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0"
    "/file/{doc_key}?access_token=83ff96335c2d45a094df02a206a39ff4"
)

# PDF关键词：命中这些词的段落才提取
_PDF_KEY_TERMS = [
    "revenue", "production", "guidance", "result", "profit", "loss",
    "cash", "ebitda", "npat", "highlights", "outlook", "summary",
    "drill", "resource", "reserve", "acquisition", "contract",
    "milestone", "update", "completion", "approval", "forecast",
    "quarter", "annual", "growth", "decline", "increase", "decrease",
    "record", "significant", "material",
]

# 财务数字正则：提取 "$X.XM/B"、"X%"、"X tonnes" 等关键数字作为key_facts
_FACT_PATTERN = re.compile(
    r"""
    (?:
        \$[\d,]+(?:\.\d+)?(?:\s?[MBKmb](?:illion)?)?  # 金额: $1.2M / $500K
        | [\d,]+(?:\.\d+)?%                             # 百分比: 45.3%
        | [\d,]+(?:\.\d+)?\s?(?:t(?:onnes?)?|oz|lb|barrel|bbl|MW|GW|kW)  # 资源/能源单位
        | (?:revenue|profit|loss|cash|production|grade|resource)\s+of\s+[\d,.]+  # 关键财务句式
        | (?:up|down|increased?|decreased?|grew?|fell?|rose?)\s+[\d.]+%  # 涨跌幅描述
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# 网页正文抓取：User-Agent伪装成浏览器（防反爬）
_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# 噪音域名：抓不到正文或质量差，跳过fetch
_SKIP_FETCH_DOMAINS = {
    "accounts.google.com", "google.com", "twitter.com", "x.com",
    "facebook.com", "linkedin.com", "youtube.com",
}

# Google News RSS域名：link字段需要follow redirect才能得到真实URL
_GOOGLE_NEWS_DOMAINS = {"news.google.com"}

# 通用市场新闻噪音词：标题中含这些词但不含stock code则过滤
_GENERIC_NOISE_TITLES = [
    "asx 200", "asx200", "market wrap", "market update", "market open",
    "morning bell", "afternoon wrap", "commodities", "wall street",
    "fed rate", "rba rate", "cpi data", "gdp data", "iron ore price",
    "gold price today", "oil price", "crypto", "bitcoin",
]

# 事件链分类关键词
_CHAIN_KEYWORDS = {
    "trigger": [
        "announces", "confirmed", "signed", "awarded", "completed",
        "results show", "maiden", "discovery", "breakthrough",
        "quarterly report", "annual report", "placement",
    ],
    "followup": [
        "continues", "update on", "progress", "milestone",
        "following", "subsequent", "after", "next steps",
    ],
    "analyst": [
        "analyst", "broker", "target price", "rating", "upgrade",
        "downgrade", "buy rating", "sell rating", "hold rating",
        "research note", "initiates coverage",
    ],
}

# 正文提取：噪音HTML标签
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE    = re.compile(r"\s{2,}")


def _extract_key_facts(text: str) -> list[str]:
    """从正文中提取关键数字和财务事实，最多10条，去重。"""
    matches = _FACT_PATTERN.findall(text)
    seen, facts = set(), []
    for m in matches:
        clean = m.strip()
        if clean.lower() not in seen and len(clean) > 2:
            seen.add(clean.lower())
            facts.append(clean)
        if len(facts) >= 10:
            break
    return facts


def _clean_html_to_text(html: str) -> str:
    """简单HTML清洗：去标签、解码实体、合并空白。"""
    text = _HTML_TAG_RE.sub(" ", html)
    for entity, char in [
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&nbsp;", " "), ("&#39;", "'"), ("&quot;", '"'),
    ]:
        text = text.replace(entity, char)
    return _SPACE_RE.sub(" ", text).strip()


def _resolve_google_news_url(google_url: str) -> str:
    """
    Google News RSS的<link>是转跳链接（news.google.com/rss/articles/...）。
    直接fetch只能得到google的JS重定向页面，正文为空。
    解决方案：follow redirect，拿到真实目标URL。
    超时或失败时返回原始URL（降级为只有标题）。
    """
    try:
        resp = requests.get(
            google_url,
            headers=_FETCH_HEADERS,
            timeout=8,
            allow_redirects=True,
            stream=True,  # 不下载body，只要最终URL
        )
        final_url = resp.url
        resp.close()
        # 如果最终URL还是google域，说明redirect失败
        if "google.com" in final_url:
            return ""
        return final_url
    except Exception:
        return ""


def _fetch_article_body(url: str, max_chars: int = 1500) -> str:
    """
    抓取新闻URL的正文内容。
    v2修复：Google News URL需先resolve redirect再fetch。
    策略：取<article>或<main>标签内容，若不存在则取最长<p>段落集合。
    失败时静默返回空字符串（正文是加分项，不是必须项）。
    """
    if not url:
        return ""
    try:
        domain = url.split("/")[2] if "/" in url else ""

        # Google News转跳链接处理
        if any(gd in domain for gd in _GOOGLE_NEWS_DOMAINS):
            url = _resolve_google_news_url(url)
            if not url:
                return ""
            domain = url.split("/")[2] if "/" in url else ""

        if any(skip in domain for skip in _SKIP_FETCH_DOMAINS):
            return ""

        resp = requests.get(url, headers=_FETCH_HEADERS, timeout=10, stream=True)
        resp.raise_for_status()

        # 只读前300KB，避免大页面拖慢速度
        content = b""
        for chunk in resp.iter_content(8192):
            content += chunk
            if len(content) > 300_000:
                break

        html = content.decode("utf-8", errors="ignore")

        # 优先提取<article>或<main>块
        body = ""
        for tag in ["<article", "<main", '<div class="article', '<div id="article',
                    '<div class="content', '<div class="story']:
            start = html.lower().find(tag)
            if start != -1:
                # 找结束标签（容错：找不到就取8000字符）
                tag_name = tag.strip("<").split()[0]
                end_tag  = f"</{tag_name}>"
                end      = html.lower().find(end_tag, start)
                if end == -1:
                    end = start + 8000
                body = _clean_html_to_text(html[start:end])
                if len(body) > 150:  # 有实质内容才停止
                    break

        # 降级：收集所有<p>标签内容（过滤太短的导航/按钮文本）
        if not body or len(body) < 150:
            paras = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL | re.IGNORECASE)
            body  = " ".join(
                _clean_html_to_text(p) for p in paras
                if len(_clean_html_to_text(p)) > 60
            )

        body = _SPACE_RE.sub(" ", body).strip()
        if len(body) > max_chars:
            cut  = body[:max_chars].rfind(". ")
            body = body[:cut + 1] if cut > max_chars * 0.7 else body[:max_chars] + "..."

        return body

    except requests.Timeout:
        log.debug(f"正文抓取超时: {url[:60]}")
    except requests.RequestException as e:
        log.debug(f"正文抓取失败: {url[:60]} — {e}")
    except Exception as e:
        log.debug(f"正文解析异常: {url[:60]} — {e}")
    return ""


def _classify_event_chain(title: str, body: str,
                           days_ago: int, is_asx_ann: bool) -> str:
    """
    对新闻/公告自动分类，标注其在事件链中的角色。

    返回值：
      "trigger"    — 直接催化剂（公告/首发事件）
      "followup"   — 后续进展（跟踪报道/项目更新）
      "analyst"    — 分析师/机构解读
      "background" — 背景信息（行业动态/宏观）

    分类逻辑：
      - ASX官方公告且≤2天 → trigger（最高优先级）
      - 标题/正文含trigger关键词 → trigger
      - 含analyst关键词 → analyst
      - 含followup关键词 → followup
      - 其余 → background
    """
    combined = (title + " " + body[:200]).lower()

    if is_asx_ann and days_ago <= 2:
        return "trigger"

    for role, keywords in _CHAIN_KEYWORDS.items():
        if any(kw in combined for kw in keywords):
            return role

    # 规则降级：同一时间窗口内越新越可能是触发事件
    if days_ago <= 1:
        return "trigger"
    if days_ago <= 7:
        return "followup"
    return "background"


def _is_generic_market_noise(title: str, code: str) -> bool:
    """
    判断标题是否为通用市场新闻（与个股无关）。
    如果标题中既没有公司代码，又含有通用市场关键词，则过滤。
    """
    title_lower = title.lower()
    code_lower  = code.lower()
    if code_lower in title_lower:
        return False  # 含股票代码，不是噪音
    return any(noise in title_lower for noise in _GENERIC_NOISE_TITLES)


def _extract_pdf_content(doc_key: str, max_chars: int = 2500) -> tuple[str, list[str]]:
    """
    下载ASX公告PDF，提取关键段落正文和关键数字。
    返回 (body_text, key_facts_list)。
    """
    if not doc_key:
        return "", []

    url = PDF_DL_BASE.format(doc_key=doc_key)
    try:
        import pdfplumber

        resp = requests.get(url, headers=ASX_HEADERS, timeout=20, stream=True)
        resp.raise_for_status()

        ct = resp.headers.get("Content-Type", "")
        if "pdf" not in ct.lower():
            log.debug(f"PDF响应非PDF格式: {ct}")
            return "", []

        pages_text = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:15]:
                t = page.extract_text()
                if t:
                    pages_text.append(t)

        if not pages_text:
            return "", []

        full_text = "\n".join(pages_text)

        # 按段落切割，按关键词命中数排序
        paragraphs = re.split(r"\n{2,}|\r\n\r\n", full_text)
        scored     = []
        for para in paragraphs:
            para = para.strip()
            if len(para) < 40:
                continue
            para_lower = para.lower()
            hits = sum(1 for kw in _PDF_KEY_TERMS if kw in para_lower)
            if hits >= 1:
                scored.append((hits, para))

        scored.sort(key=lambda x: x[0], reverse=True)
        extracted = "\n\n".join(p for _, p in scored[:8])
        extracted = _SPACE_RE.sub(" ", extracted).strip()

        if not extracted:
            extracted = full_text[:1000]

        if len(extracted) > max_chars:
            cut = extracted[:max_chars].rfind(". ")
            extracted = extracted[:cut + 1] if cut > max_chars * 0.7 else extracted[:max_chars] + "..."

        key_facts = _extract_key_facts(full_text)
        log.debug(f"PDF提取成功: {doc_key[:20]} → {len(extracted)}字符, {len(key_facts)}关键数字")
        return extracted, key_facts

    except ImportError:
        log.warning("pdfplumber未安装，跳过PDF提取。运行: pip install pdfplumber")
        return "", []
    except requests.Timeout:
        log.warning(f"PDF下载超时: {doc_key[:20]}")
    except requests.RequestException as e:
        log.warning(f"PDF下载失败: {doc_key[:20]} — {e}")
    except Exception as e:
        log.warning(f"PDF解析异常: {doc_key[:20]} — {e}")
    return "", []


def get_recent_announcements(hours_back: int = 72) -> dict:
    """
    批量拉取ASX近期公告，保存documentKey供后续PDF提取使用。
    返回 {symbol: {headline, sensitive, date, doc_key}}
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=hours_back)
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    result: dict = {}
    page = 0

    while True:
        try:
            r = requests.get(
                ASX_ANN_URL,
                params={"itemsPerPage": 100, "page": page},
                headers=ASX_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            items = r.json().get("data", {}).get("items", [])

            if not items:
                break

            got_old = False
            for item in items:
                if item.get("date", "") < cutoff:
                    got_old = True
                    break
                sym = item.get("symbol", "")
                if sym and sym not in result:
                    result[sym] = {
                        "headline" : item.get("headline", "")[:120],
                        "sensitive": item.get("isPriceSensitive", False),
                        "date"     : item.get("date", "")[:10],
                        "doc_key"  : item.get("documentKey", ""),
                    }

            if got_old or len(items) < 100:
                break

            page += 1
            time.sleep(0.3)

        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in RETRYABLE_STATUS_CODES:
                log.warning(f"公告API HTTP错误({e.response.status_code})，跳过分页{page}")
            else:
                log.error(f"公告API不可重试错误: {e}")
            break
        except Exception as e:
            log.error(f"公告API异常 (page={page}): {e}")
            break

    log.info(f"最近{hours_back}小时公告：{len(result)} 只股票")
    return result


def get_stock_news_timeline(code: str, days_back: int = 90) -> list[dict]:
    """
    构建单只股票的完整新闻情报时间线。

    每条记录包含：
      title       — 标题
      body        — 正文摘要（PDF提取 或 网页正文抓取）
      key_facts   — 关键数字列表（正则提取财务/运营数字）
      date        — 日期
      days_ago    — 距今天数
      source      — 来源
      sensitive   — 是否ASX价格敏感公告
      is_trigger  — 是否为今日触发涨幅的直接催化剂
      chain_role  — 事件链角色: trigger/followup/analyst/background

    v2核心修复：
      - Google News link先resolve redirect拿真实URL再fetch正文
      - 通用市场噪音过滤（非个股新闻剔除）
      - 事件链自动分类（trigger/followup/analyst/background）
      - 新闻质量门控：返回timeline同时返回has_recent标志
    """
    today_dt  = date.today()
    today_str = today_dt.isoformat()
    cutoff    = (today_dt - timedelta(days=days_back)).isoformat()
    timeline  = []

    # ── 来源1：ASX全市场公告缓存（本地查找，零额外API调用）──
    # ASX单股历史公告API已确认404，用全市场72h缓存按code查找。
    ann_entry = _ann_map_cache.get(code)
    if ann_entry:
        ann_date  = ann_entry.get("date", today_str)
        days_ago  = (today_dt - date.fromisoformat(ann_date)).days if ann_date else 999
        sensitive = ann_entry.get("sensitive", False)
        doc_key   = ann_entry.get("doc_key", "")
        headline  = ann_entry.get("headline", "")
        is_recent = days_ago <= 2

        body, key_facts = "", []
        if doc_key and days_ago <= 3:
            log.info(f"  📄 PDF提取 [{code}] [{days_ago}天前]: {headline[:50]}")
            body, key_facts = _extract_pdf_content(doc_key)
            time.sleep(0.5)

        chain_role = _classify_event_chain(headline, body, days_ago, is_asx_ann=True)

        timeline.append({
            "date"       : ann_date,
            "days_ago"   : days_ago,
            "source"     : "ASX官方公告",
            "title"      : headline[:120],
            "body"       : body,
            "key_facts"  : key_facts,
            "sensitive"  : sensitive,
            "is_trigger" : is_recent and sensitive,
            "chain_role" : chain_role,
            "doc_key"    : doc_key,
        })
        log.debug(f"ASX公告命中 [{code}]: [{chain_role}] {headline[:50]}")
    else:
        log.debug(f"ASX公告无记录 [{code}]（72小时内无公告）")

    # ── 来源2：Google News ────────────────────────────────────
    # v2修复：RSS link是google转跳链接，先resolve再fetch正文

    google_queries = [
        f"ASX:{code}",
        f"{code} ASX Australia",
    ]
    seen_titles: set = set()

    for query in google_queries:
        rss_url = (
            f"https://news.google.com/rss/search"
            f"?q={requests.utils.quote(query)}&hl=en-AU&gl=AU&ceid=AU:en"
        )
        try:
            resp = requests.get(
                rss_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            for item in root.findall(".//item")[:10]:
                title    = (item.findtext("title") or "").strip()
                # Google RSS的<link>是转跳链接，直接fetch会拿到google JS页
                glink    = (item.findtext("link") or "").strip()
                pub_raw  = item.findtext("pubDate") or ""
                source   = str(item.findtext("source") or "Google News")[:60]

                try:
                    pub_date = parsedate_to_datetime(pub_raw).strftime("%Y-%m-%d")
                except Exception:
                    pub_date = today_str

                if not title or pub_date < cutoff:
                    continue

                # 过滤通用市场噪音
                if _is_generic_market_noise(title, code):
                    log.debug(f"噪音过滤 [{code}]: {title[:60]}")
                    continue

                title_key = title[:50].lower()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                days_ago  = (today_dt - date.fromisoformat(pub_date)).days
                is_recent = days_ago <= 2

                body = ""
                if is_recent and glink:
                    # 先resolve Google News转跳，拿真实URL
                    real_url = _resolve_google_news_url(glink)
                    if real_url:
                        log.info(f"  🌐 正文抓取 [{code}]: {title[:50]}")
                        body = _fetch_article_body(real_url, max_chars=1500)
                        time.sleep(0.4)
                    else:
                        log.debug(f"  ⚠️ Google redirect失败，仅保留标题: {title[:50]}")

                key_facts  = _extract_key_facts(body) if body else []
                chain_role = _classify_event_chain(title, body, days_ago, is_asx_ann=False)

                timeline.append({
                    "date"       : pub_date,
                    "days_ago"   : days_ago,
                    "source"     : source,
                    "title"      : title[:120],
                    "body"       : body,
                    "key_facts"  : key_facts,
                    "sensitive"  : False,
                    "is_trigger" : False,
                    "chain_role" : chain_role,
                    "url"        : glink,
                })

            time.sleep(0.4)

        except ET.ParseError as e:
            log.warning(f"Google RSS XML解析失败 [{code}]: {e}")
        except requests.RequestException as e:
            log.warning(f"Google RSS请求失败 [{code}]: {e}")
        except Exception as e:
            log.warning(f"Google RSS未知错误 [{code}]: {e}")

    # ── 来源3：yfinance（补充覆盖）──────────────────────────
    try:
        stock = yf.Ticker(f"{code}.AX")
        for n in (stock.news or [])[:12]:
            content  = n.get("content", {})
            title    = content.get("title", "")
            pub      = content.get("pubDate", "")[:10]
            art_url  = content.get("canonicalUrl", {}).get("url", "")
            source   = content.get("provider", {}).get("displayName", "Yahoo Finance")

            if not title or not pub or pub < cutoff:
                continue
            if _is_generic_market_noise(title, code):
                continue

            title_key = title[:50].lower()
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)

            days_ago  = (today_dt - date.fromisoformat(pub)).days if pub else 999
            is_recent = days_ago <= 2

            body = ""
            if is_recent and art_url:
                already = any(e.get("url") == art_url for e in timeline)
                if not already:
                    log.info(f"  🌐 yfinance正文 [{code}]: {title[:50]}")
                    body = _fetch_article_body(art_url, max_chars=1500)
                    time.sleep(0.3)

            key_facts  = _extract_key_facts(body) if body else []
            chain_role = _classify_event_chain(title, body, days_ago, is_asx_ann=False)

            timeline.append({
                "date"       : pub,
                "days_ago"   : days_ago,
                "source"     : source,
                "title"      : title[:120],
                "body"       : body,
                "key_facts"  : key_facts,
                "sensitive"  : False,
                "is_trigger" : False,
                "chain_role" : chain_role,
                "url"        : art_url,
            })

    except Exception as e:
        log.warning(f"yfinance新闻失败 [{code}]: {e}")

    # ── 去重 + 排序 ──────────────────────────────────────────
    # 排序优先级：trigger > 有body > sensitive > 日期新
    seen_dedup: set = set()
    unique: list   = []
    chain_order = {"trigger": 0, "followup": 1, "analyst": 2, "background": 3}

    def _sort_key(item: dict) -> tuple:
        return (
            chain_order.get(item.get("chain_role", "background"), 3),
            -int(bool(item.get("body"))),
            -int(item.get("sensitive", False)),
            item.get("days_ago", 999),
        )

    for item in sorted(timeline, key=_sort_key):
        key = item["title"][:50].lower()
        if key not in seen_dedup:
            seen_dedup.add(key)
            unique.append(item)

    result = unique[:20]

    # 统计日志
    with_body   = sum(1 for i in result if i.get("body"))
    with_facts  = sum(1 for i in result if i.get("key_facts"))
    triggers    = sum(1 for i in result if i.get("chain_role") == "trigger")
    recent_cnt  = sum(1 for i in result if i.get("days_ago", 999) <= 2)
    log.info(
        f"新闻时间线 [{code}]: {len(result)}条 | "
        f"含正文:{with_body} | 含关键数字:{with_facts} | "
        f"trigger:{triggers} | 近2天:{recent_cnt}"
    )
    return result


def has_recent_news(timeline: list[dict], max_days: int = 2) -> bool:
    """
    新闻质量门控：判断时间线中是否有≤max_days天内的新闻/公告。
    Morning scanner 要求必须有近期新闻才值得写文章。

    优先检查：
      1. ASX官方公告（最权威）
      2. trigger分类的新闻
      3. 任意≤2天的新闻
    """
    for item in timeline:
        if item.get("days_ago", 999) <= max_days:
            return True
    return False


def get_asx_universe() -> list[str]:
    """获取ASX全量股票代码"""
    try:
        df  = pd.read_csv(
            "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
            skiprows=1,
            encoding="latin1",
        )
        col = next((c for c in df.columns if "code" in c.lower()), None)
        if col is None:
            log.error("ASX列表CSV格式变更，找不到code列")
            return []
        codes = df[col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r"^[A-Z]{1,5}$")]
        return [f"{c}.AX" for c in valid]
    except Exception as e:
        log.error(f"获取ASX股票列表失败: {e}")
        return []


def batch_daily(tickers: list[str], batch_size: int = 100) -> dict[str, pd.DataFrame]:
    all_data: dict = {}
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="60d", interval="1d", progress=False)
                if not df.empty and len(df) >= FILTER["min_history_days"]:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(
                    batch, period="60d", interval="1d",
                    progress=False, group_by="ticker",
                )
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how="all")
                        if not tdf.empty and len(tdf) >= FILTER["min_history_days"]:
                            all_data[t] = tdf
                    except KeyError:
                        pass
        except Exception as e:
            log.warning(f"日线批量下载失败 (batch {i//batch_size+1}): {e}")
        time.sleep(0.5)
    return all_data


def batch_intraday(tickers: list[str], batch_size: int = 50) -> dict[str, pd.DataFrame]:
    all_data: dict = {}
    total = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        bn    = i // batch_size + 1
        if bn % 5 == 1:
            log.info(f"  盘中数据 {bn}/{total}批...")
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="1d", interval="5m", progress=False)
                if not df.empty:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(
                    batch, period="1d", interval="5m",
                    progress=False, group_by="ticker",
                )
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how="all")
                        if not tdf.empty:
                            all_data[t] = tdf
                    except KeyError:
                        pass
        except Exception as e:
            log.warning(f"盘中批量下载失败 (batch {bn}): {e}")
        time.sleep(0.5)
    return all_data


# ============================================================
# 技术指标计算（精筛用）
# ============================================================

def calc_vwap(df: pd.DataFrame) -> pd.Series:
    c  = safe_series(df, "Close")
    h  = safe_series(df, "High")
    l  = safe_series(df, "Low")
    v  = safe_series(df, "Volume")
    tp = (h + l + c) / 3
    return (tp * v).cumsum() / v.cumsum()


def align_price_events_to_news(
    daily: pd.DataFrame,
    news_timeline: list[dict],
    threshold_pct: float = 5.0,
    window_days: int = 2,
) -> list[dict]:
    """
    价格-事件对齐：把历史大涨大跌节点和新闻/公告日期自动匹配。

    逻辑：
    - 找出过去60日内单日涨跌 >= threshold_pct 的价格事件
    - 对每个价格事件，在 ±window_days 范围内搜索新闻/公告
    - 如果找到匹配，在 news_timeline 中标记 price_move 字段
    - 同时返回"孤立价格事件"（没有对应新闻的大涨大跌，值得注意）

    这让 Gemini 能自动推断"是那条公告导致了那次涨停"的因果链。
    修改 news_timeline（原地更新），同时返回未匹配的孤立事件列表。
    """
    try:
        closes   = safe_series(daily, "Close").dropna()
        pct_ch   = closes.pct_change() * 100
        # 只看最近60日
        recent   = pct_ch.iloc[-60:]

        # 收集价格事件
        price_events = []
        for dt, val in recent.items():
            if abs(val) >= threshold_pct:
                price_events.append({
                    "date"      : str(dt)[:10],
                    "change_pct": round(float(val), 1),
                    "matched"   : False,
                })

        if not price_events:
            return []

        # 对每条新闻，检查是否有对应价格事件
        for news_item in news_timeline:
            news_date = news_item.get("date", "")
            if not news_date:
                continue
            try:
                nd = date.fromisoformat(news_date)
            except ValueError:
                continue

            # 搜索 ±window_days 内的价格事件
            matched_moves = []
            for pe in price_events:
                try:
                    pd_dt = date.fromisoformat(pe["date"])
                except ValueError:
                    continue
                if abs((pd_dt - nd).days) <= window_days:
                    matched_moves.append(pe["change_pct"])
                    pe["matched"] = True

            if matched_moves:
                # 把价格反应嵌入新闻条目（Gemini可直接读取因果关系）
                news_item["price_move"] = matched_moves[0]  # 最近的一次
                news_item["price_move_str"] = (
                    f"{'📈' if matched_moves[0] > 0 else '📉'}"
                    f"公告后{abs(matched_moves[0])}%"
                )

        # 返回无法关联到新闻的孤立价格事件（可能是内幕信息或未公告事件）
        orphan_events = [pe for pe in price_events if not pe["matched"]]
        if orphan_events:
            log.debug(
                f"价格-事件对齐: {len(price_events)}个价格事件，"
                f"{len(price_events)-len(orphan_events)}个已匹配，"
                f"{len(orphan_events)}个无对应新闻"
            )
        return orphan_events

    except Exception as e:
        log.debug(f"价格-事件对齐异常: {e}")
        return []


def compute_historical_metrics(daily: pd.DataFrame) -> dict:
    """
    将180天日线数据压缩为关键指标字典，供Gemini消费。
    避免原始数据直接传入造成token浪费。
    """
    closes  = safe_series(daily, "Close").dropna()
    volumes = safe_series(daily, "Volume").dropna()
    highs   = safe_series(daily, "High").dropna()
    lows    = safe_series(daily, "Low").dropna()

    if len(closes) < 5:
        return {}

    # 价格动量
    ret_5d  = float((closes.iloc[-1] / closes.iloc[-6]  - 1) * 100) if len(closes) > 6  else None
    ret_20d = float((closes.iloc[-1] / closes.iloc[-21] - 1) * 100) if len(closes) > 21 else None
    ret_60d = float((closes.iloc[-1] / closes.iloc[-61] - 1) * 100) if len(closes) > 61 else None

    # 波动率（20日年化）
    daily_ret = closes.pct_change().dropna()
    vol_20d   = float(daily_ret.iloc[-20:].std() * (252 ** 0.5) * 100) if len(daily_ret) >= 20 else None

    # 成交量趋势
    avg_vol_20d = float(volumes.iloc[-20:].mean())
    avg_vol_5d  = float(volumes.iloc[-5:].mean())
    vol_trend   = round(avg_vol_5d / avg_vol_20d, 2) if avg_vol_20d > 0 else None

    # 52周高低位置（用现有数据估算）
    period_high = float(highs.max())
    period_low  = float(lows.min())
    curr_price  = float(closes.iloc[-1])
    pct_from_high = round((curr_price / period_high - 1) * 100, 1) if period_high > 0 else None
    pct_from_low  = round((curr_price / period_low  - 1) * 100, 1) if period_low  > 0 else None

    # 简单趋势：5日均线 vs 20日均线
    ma5  = float(closes.iloc[-5:].mean())  if len(closes) >= 5  else None
    ma20 = float(closes.iloc[-20:].mean()) if len(closes) >= 20 else None
    trend = "上升趋势" if (ma5 and ma20 and ma5 > ma20) else "下降趋势"

    # RSI(14)
    rsi = None
    if len(daily_ret) >= 14:
        gains  = daily_ret.clip(lower=0).iloc[-14:]
        losses = (-daily_ret.clip(upper=0)).iloc[-14:]
        avg_g  = gains.mean()
        avg_l  = losses.mean()
        rsi    = round(100 - 100 / (1 + avg_g / avg_l), 1) if avg_l > 0 else 100.0

    return {
        "current_price"    : round(curr_price, 3),
        "trend"            : trend,
        "ma5"              : round(ma5, 3)  if ma5  else None,
        "ma20"             : round(ma20, 3) if ma20 else None,
        "rsi_14"           : rsi,
        "ret_5d_pct"       : round(ret_5d,  1) if ret_5d  else None,
        "ret_20d_pct"      : round(ret_20d, 1) if ret_20d else None,
        "ret_60d_pct"      : round(ret_60d, 1) if ret_60d else None,
        "vol_annualized_pct": round(vol_20d, 1) if vol_20d else None,
        "vol_trend_5v20"   : vol_trend,
        "pct_from_period_high": pct_from_high,
        "pct_from_period_low" : pct_from_low,
        "avg_daily_vol_20d": int(avg_vol_20d),
    }


# ============================================================
# 核心筛选逻辑（强化版）
# ============================================================

def apply_filters(
    t: str,
    daily: pd.DataFrame,
    intra: pd.DataFrame,
) -> Optional[dict]:
    """
    对单只股票应用完整筛选条件。
    通过返回候选字典，否则返回 None 并记录拒绝原因。

    筛选逻辑说明：
    - 价格区间：过滤仙股（噪音多、点差大）和超高价股（流动性差）
    - 涨幅上限：60%以上通常已是恐慌性追买尾段，风险/回报恶化
    - 量比≥1.5：确保今日异动量是实质性的，不是低迷盘整
    - 换手金额≥50万：确保可以正常进出，避免流动性陷阱
    - VWAP距离≤5%：价格已明显脱离VWAP说明追入成本过高
    - 排除已连涨：避免在多日加速拉升末端追高
    """
    try:
        closes     = safe_series(daily, "Close")
        prev_close = float(closes.iloc[-2])
        curr_price = float(safe_series(intra, "Close").iloc[-1])

        # 1. 价格区间过滤（最基础的仙股过滤）
        if not (FILTER["min_price"] <= curr_price <= FILTER["max_price"]):
            log.debug(f"  SKIP {t}: 价格{curr_price}超出区间")
            return None

        # 2. 涨幅区间
        change_pct = (curr_price - prev_close) / prev_close * 100
        if not (FILTER["min_change_pct"] <= change_pct <= FILTER["max_change_pct"]):
            log.debug(f"  SKIP {t}: 涨幅{change_pct:.1f}%超出区间")
            return None

        # 3. 量比（今日量 / 20日均量）
        today_vol   = float(safe_series(intra, "Volume").sum())
        avg_day_vol = float(safe_series(daily, "Volume").iloc[-20:].mean())
        vol_ratio   = today_vol / avg_day_vol if avg_day_vol > 0 else 0
        if vol_ratio < FILTER["min_vol_ratio"]:
            log.debug(f"  SKIP {t}: 量比{vol_ratio:.2f}不足")
            return None

        # 4. 流动性：今日换手金额
        dollar_volume = today_vol * curr_price
        if dollar_volume < FILTER["min_dollar_volume"]:
            log.debug(f"  SKIP {t}: 日换手额${dollar_volume:,.0f}不足")
            return None

        # 5. VWAP距离（价格不能远离VWAP，避免追高）
        vwap_series = calc_vwap(intra)
        vwap        = float(vwap_series.iloc[-1])
        vwap_dist   = abs(curr_price - vwap) / vwap * 100 if vwap > 0 else 999
        if vwap_dist > FILTER["max_vwap_dist_pct"]:
            log.debug(f"  SKIP {t}: 距VWAP{vwap_dist:.1f}%过远")
            return None

        # 6. 排除已连涨多日（避免追末段）
        if len(closes) >= 4:
            d1, d2, d3 = float(closes.iloc[-2]), float(closes.iloc[-3]), float(closes.iloc[-4])
            if d1 > d2 * 1.05 and d2 > d3 * 1.02:
                log.debug(f"  SKIP {t}: 已连续多日上涨，避免追高")
                return None

        # 7. 计算其他盘中指标
        today_high  = float(safe_series(intra, "High").max())
        today_low   = float(safe_series(intra, "Low").min())
        launch_pt   = float(safe_series(intra, "Low").iloc[0])

        # 是否仍是"一字板"（价格贴近当日最高，无回调空间）
        # 用绝对价差而非百分比，对低价股更准确
        pullback_room = (today_high - curr_price) / today_high * 100 if today_high > 0 else 0
        is_straight   = pullback_room < 2.0

        # 价格相对今日区间的位置（0=最低，100=最高）
        range_size  = today_high - today_low
        price_in_range = (
            (curr_price - today_low) / range_size * 100
            if range_size > 0 else 50
        )

        return {
            "ticker"        : t,
            "price"         : round(curr_price, 3),
            "prev_close"    : round(prev_close, 3),
            "change_pct"    : round(change_pct, 1),
            "vol_ratio"     : round(vol_ratio, 2),
            "dollar_volume" : int(dollar_volume),
            "vwap"          : round(vwap, 3),
            "vwap_dist_pct" : round(vwap_dist, 1),
            "today_high"    : round(today_high, 3),
            "today_low"     : round(today_low, 3),
            "launch_pt"     : round(launch_pt, 3),
            "is_straight"   : is_straight,
            "pullback_room" : round(pullback_room, 1),
            "price_in_range": round(price_in_range, 1),
        }

    except (IndexError, ValueError, KeyError, ZeroDivisionError) as e:
        log.debug(f"  SKIP {t}: 指标计算异常 {e}")
        return None


# ============================================================
# Gemini 综合分析（三阶段流程第三步）
# ============================================================

def build_gemini_batch_prompt(candidates: list[dict]) -> str:
    """
    构建批量分析Prompt——叙事驱动版。

    核心设计原则：
    给Gemini的不是"数据堆"，而是"侦探档案"：
      - 按事件链角色（trigger/followup/analyst/background）分层呈现新闻
      - 触发事件放顶部高亮，含正文摘要和关键数字
      - 历史背景按时间倒序，让Gemini理解故事演进
      - 孤立价格事件（无对应新闻的大涨大跌）单独标注，提示异常

    输出JSON字段含故事三层结构：
      catalyst（今日触发）→ backstory（历史背景）→ story_chain（因果链）
    """

    def _fmt_item(n: dict, body_limit: int = 300) -> str:
        """渲染单条新闻：标题 + 关键数字 + 正文（按重要性截取）。
        定义在函数级别（build_gemini_batch_prompt内），所有候选共用同一个函数对象。
        """
        days   = n.get("days_ago", "?")
        src    = n.get("source", "")
        title  = n.get("title", "")
        body   = n.get("body", "")
        facts  = n.get("key_facts", [])
        move   = n.get("price_move_str", "")
        sens   = "⭐价格敏感 " if n.get("sensitive") else ""
        move_s = f" → {move}" if move else ""

        lines = [f"  [{days}天前] {sens}{src}: {title}{move_s}"]
        if facts:
            lines.append(f"    关键数字: {' | '.join(facts[:5])}")
        if body:
            body_trim = body[:body_limit].rstrip()
            if len(body) > body_limit:
                body_trim += "..."
            lines.append(f"    正文: {body_trim}")
        return "\n".join(lines)

    blocks = []
    for c in candidates:
        metrics  = c.get("hist_metrics", {})
        timeline = c.get("news_timeline", [])
        orphans  = c.get("orphan_price_events", [])

        # ── 按chain_role分组 ──────────────────────────────────
        triggers    = [n for n in timeline if n.get("chain_role") == "trigger"]
        followups   = [n for n in timeline if n.get("chain_role") == "followup"]
        analysts    = [n for n in timeline if n.get("chain_role") == "analyst"]
        backgrounds = [n for n in timeline if n.get("chain_role") == "background"]

        # 触发事件（最重要，正文给足300字）
        trigger_section = ""
        if triggers:
            trigger_section = "\n⚡【直接催化剂 — 今日涨幅的直接触发事件】\n"
            trigger_section += "\n".join(_fmt_item(n, 300) for n in triggers[:2])
        else:
            trigger_section = "\n⚠️【无明确触发事件 — 此股可能是技术性或跟风炒作】"

        # 后续进展（150字）
        followup_section = ""
        if followups:
            followup_section = "\n\n📈【后续进展 — 催化剂发酵轨迹】\n"
            followup_section += "\n".join(_fmt_item(n, 150) for n in followups[:3])

        # 分析师观点（100字）
        analyst_section = ""
        if analysts:
            analyst_section = "\n\n🔍【分析师/机构观点】\n"
            analyst_section += "\n".join(_fmt_item(n, 100) for n in analysts[:2])

        # 历史背景（仅标题，帮助理解公司发展脉络）
        bg_section = ""
        if backgrounds:
            bg_section = "\n\n📚【历史背景 — 公司近期发展脉络】\n"
            bg_lines   = [
                f"  [{n.get('days_ago','?')}天前] {n.get('source','')}: {n.get('title','')}"
                for n in backgrounds[:4]
            ]
            bg_section += "\n".join(bg_lines)

        # 孤立价格事件（无新闻的大涨大跌）
        orphan_section = ""
        if orphans:
            orphan_section = "\n\n❓【孤立价格异动 — 大涨大跌但无对应公告，需核查】\n"
            orphan_section += "\n".join(
                f"  {e['date']}: 单日{'+' if e['change_pct']>0 else ''}{e['change_pct']}%"
                for e in orphans[:4]
            )

        pullback_label = "⚠️一字板" if c["is_straight"] else f"回调空间{c['pullback_room']}%"
        block = (
            f"\n{'━'*48}\n"
            f"【{c['ticker']}】\n"
            f"今日: 涨{c['change_pct']}% | 量比{c['vol_ratio']}x | 换手${c['dollar_volume']:,}\n"
            f"价格: ${c['price']} | VWAP${c['vwap']}(距{c['vwap_dist_pct']}%)"
            f" | {pullback_label}\n"
            f"历史: {metrics.get('trend','?')} | RSI={metrics.get('rsi_14','?')}"
            f" | 5日{metrics.get('ret_5d_pct','?')}% | 60日{metrics.get('ret_60d_pct','?')}%"
            f" | 距高点{metrics.get('pct_from_period_high','?')}%"
            f"{trigger_section}"
            f"{followup_section}"
            f"{analyst_section}"
            f"{bg_section}"
            f"{orphan_section}"
        )
        blocks.append(block)

    stocks_section = "\n".join(blocks)

    return f"""你是专业的ASX短线量化分析师，今日为{date.today().isoformat()}。
以下股票已通过量化初筛（涨≥10%、量比≥1.5x、VWAP偏离≤5%）且有≤2天内公告/新闻。

{stocks_section}

请对每只股票输出结构化分析。严格按照以下JSON格式，不要输出任何JSON以外的内容：

{{
  "TICKER.AX": {{
    "verdict": "买入" | "观望" | "回避",
    "confidence": "高" | "中" | "低",
    "catalyst": "1句：今日涨幅的直接触发事件（必须引用具体公告/新闻标题或关键数字，禁止泛化）",
    "backstory": "2-3句：这个催化剂的历史背景——公司此前铺垫了什么，市场原有预期是什么，今日是否超预期",
    "story_chain": "1-2句：从时间线中找出因果链——哪个早期事件为今日涨幅埋下伏笔（如：3个月前拿到XX合同，今日季报确认收入兑现）",
    "short_term_view": "上行情景（1句）+ 下行风险（1句）",
    "entry_note": "具体价位或条件：回踩VWAP $X.XX建仓 / 一字板等待回调至$X.XX / 等放量确认"
  }}
}}

分析规则：
- catalyst/backstory/story_chain 三个字段合起来必须构成完整的"为什么今天涨"故事
- 如果触发事件是"⚠️无明确触发事件"，verdict只能是"回避"
- 孤立价格异动（无对应新闻）说明存在信息不对称，在short_term_view中必须提及
- 无正文或关键数字的字段，明确写"公告正文未获取，需核查原文"，禁止编造
- 每个字段严格控制在规定句数内"""


def analyze_candidates_batch(candidates: list[dict]) -> dict[str, dict]:
    """执行批量Gemini分析，返回 {ticker: analysis_dict}"""
    if not gemini_client or not candidates:
        return {}

    prompt  = build_gemini_batch_prompt(candidates)
    log.info(f"批量Gemini分析，共{len(candidates)}只股票，1次API调用...")

    raw = gemini_call_with_retry(prompt)
    if not raw:
        return {}

    try:
        clean = raw.strip()
        # 剥离 ```json ... ``` 包装
        if clean.startswith("```"):
            lines = clean.split("\n")
            clean = "\n".join(lines[1:])
            if clean.strip().endswith("```"):
                clean = clean.strip()[:-3]
        result = json.loads(clean)
        log.info(f"Gemini批量分析成功，获得{len(result)}条结果")
        return result
    except json.JSONDecodeError as e:
        log.error(f"Gemini JSON解析失败: {e}\n原始输出(前500字):\n{raw[:500]}")
        return {}


# ============================================================
# 通知
# ============================================================

def send_telegram(msg: str) -> None:
    """
    发送 Telegram 消息，自动按4000字节分段。
    叙事卡片版每只股票内容较多，单条可能超限，按段落切割保持完整性。
    """
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过通知")
        return

    url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # 按段落切割（遇到连续换行），尽量保持每张卡片完整
    chunks: list[str] = []
    current = ""
    for paragraph in msg.split("\n\n"):
        candidate = (current + "\n\n" + paragraph).strip() if current else paragraph
        if len(candidate) > 3800:
            if current:
                chunks.append(current.strip())
            current = paragraph
        else:
            current = candidate
    if current:
        chunks.append(current.strip())

    for chunk in chunks:
        try:
            resp = requests.post(
                url,
                json={
                    "chat_id"                  : CHAT_ID,
                    "text"                     : chunk,
                    "parse_mode"               : "HTML",
                    "disable_web_page_preview" : True,
                },
                timeout=10,
            )
            if not resp.ok:
                log.error(f"Telegram发送失败: {resp.status_code} {resp.text[:100]}")
        except requests.RequestException as e:
            log.error(f"Telegram请求异常: {e}")
        time.sleep(0.5)


def format_telegram_message(candidates: list[dict], ai_results: dict, today: str) -> str:
    """
    格式化主报告 Telegram 消息——叙事卡片版。

    设计原则：
    - 每只股票是一张独立的"故事卡片"，从催化剂讲到入场建议
    - 新增字段全部利用：backstory（背景）/ story_chain（因果链）
    - 新闻正文摘要和关键数字直接在卡片里呈现，不只是标题
    - 孤立价格事件（无对应新闻的大涨大跌）在卡片末尾警示
    - 发送逻辑：header + 每只股票各一条消息（避免单条超4096字节）
    """
    verdict_order = {"买入": 0, "观望": 1, "回避": 2}
    candidates.sort(
        key=lambda c: (
            verdict_order.get(
                ai_results.get(c["ticker"], {}).get("verdict", "观望"), 1
            ),
            -c["change_pct"],
        )
    )

    # 统计摘要
    buy_cnt   = sum(1 for c in candidates
                    if ai_results.get(c["ticker"], {}).get("verdict") == "买入")
    watch_cnt = sum(1 for c in candidates
                    if ai_results.get(c["ticker"], {}).get("verdict") == "观望")
    avoid_cnt = sum(1 for c in candidates
                    if ai_results.get(c["ticker"], {}).get("verdict") == "回避")

    header = (
        f"⚡ <b>First Pullback 候选 {today}</b>\n"
        f"共 {len(candidates)} 只 | "
        f"🟢买入:{buy_cnt}  🟡观望:{watch_cnt}  🔴回避:{avoid_cnt}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    cards = [header]

    for idx, c in enumerate(candidates, 1):
        ai      = ai_results.get(c["ticker"], {})
        verdict = ai.get("verdict", "—")
        conf    = ai.get("confidence", "—")
        verdict_emoji = {"买入": "🟢", "观望": "🟡", "回避": "🔴"}.get(verdict, "⚪")

        # ── 行情数据行 ────────────────────────────────────────
        sl_flag  = "⚠️一字板" if c["is_straight"] else f"↩回调{c['pullback_room']}%"
        src_flag = "📋ASX" if c.get("ann_source") == "asx" else "📰新闻"
        sen_flag = "⭐" if c.get("ann_sensitive") else ""

        card_lines = [
            f"\n{verdict_emoji} <b>#{idx} {c['ticker']}</b>  "
            f"{verdict}（{conf}信心）  +{c['change_pct']}%  量{c['vol_ratio']}x",

            f"💰 ${c['price']} | VWAP${c['vwap']}(偏{c['vwap_dist_pct']}%) | {sl_flag}",
            f"{src_flag}{sen_flag} <i>{c.get('ann_headline', '')[:80]}</i>",
        ]

        # ── 触发催化剂（今日涨幅的直接原因）────────────────────
        if ai.get("catalyst"):
            card_lines.append(f"\n🔥 <b>催化剂</b>: {ai['catalyst']}")

        # ── 历史背景（铺垫这个催化剂的前情）────────────────────
        if ai.get("backstory"):
            card_lines.append(f"📖 <b>背景</b>: {ai['backstory']}")

        # ── 因果链（从早期埋伏到今日引爆）──────────────────────
        if ai.get("story_chain"):
            card_lines.append(f"🔗 <b>因果链</b>: {ai['story_chain']}")

        # ── 触发新闻正文摘要（最多1条trigger，最多150字）────────
        # 这是关键：让读者拿到真实内容而不只是标题
        timeline = c.get("news_timeline", [])
        trigger_items = [n for n in timeline if n.get("chain_role") == "trigger"]
        if trigger_items:
            t_item = trigger_items[0]
            body   = t_item.get("body", "")
            facts  = t_item.get("key_facts", [])
            if body:
                body_short = body[:160].rstrip()
                if len(body) > 160:
                    body_short += "..."
                card_lines.append(f"📄 <b>公告摘要</b>: {body_short}")
            if facts:
                card_lines.append(f"📊 <b>关键数字</b>: {' | '.join(facts[:4])}")

        # ── 孤立价格事件警告（大涨大跌无对应新闻）──────────────
        orphans = c.get("orphan_price_events", [])
        if orphans:
            orphan_str = "、".join(
                f"{e['date']}单日{'+' if e['change_pct']>0 else ''}{e['change_pct']}%"
                for e in orphans[:2]
            )
            card_lines.append(f"❓ <b>异动警示</b>: {orphan_str}（无对应公告，需核查）")

        # ── 短期展望 + 入场建议 ──────────────────────────────────
        if ai.get("short_term_view"):
            card_lines.append(f"\n📈 <b>短期</b>: {ai['short_term_view']}")
        if ai.get("entry_note"):
            card_lines.append(f"🎯 <b>入场</b>: {ai['entry_note']}")

        card_lines.append("─" * 22)
        cards.append("\n".join(card_lines))

    # 尾部固定止盈止损提示
    cards.append(
        "⚠️ <b>止损</b>: 跌破启动低点或 -8%\n"
        "💰 <b>止盈</b>: +10% 锁半仓，+20% 清仓\n"
        "本内容仅供参考，不构成投资建议。"
    )

    return "\n".join(cards)


# ============================================================
# 主扫描流程
# ============================================================

def run_morning_scan() -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    log.info(f"{'='*50}")
    log.info(f"First Pullback 早盘扫描开始 [{today}]")
    log.info(f"{'='*50}")

    # ── 阶段一：批量数据 + 粗筛 ──────────────────────────────
    log.info("【阶段一】拉取公告 & 批量数据...")

    ann_map  = get_recent_announcements()

    # 注入模块级缓存，供 get_stock_news_timeline() 使用（避免参数透传）
    global _ann_map_cache
    _ann_map_cache = ann_map
    universe = get_asx_universe()
    if not universe:
        log.error("无法获取股票池，终止")
        return
    log.info(f"股票池：{len(universe)} 只")

    daily_data = batch_daily(universe, batch_size=100)

    # 流动性预过滤（换手金额门槛，使用日线均值估算）
    liquid: list[str] = []
    for t, df in daily_data.items():
        try:
            avg_vol    = float(safe_series(df, "Volume").iloc[-20:].mean())
            last_close = float(safe_series(df, "Close").iloc[-1])
            if avg_vol * last_close >= FILTER["min_dollar_volume"]:
                liquid.append(t)
        except (IndexError, ValueError, KeyError):
            pass
    log.info(f"流动性预过滤后：{len(liquid)} 只")

    intra_data = batch_intraday(liquid, batch_size=50)

    # 应用强化筛选条件
    pre_candidates: list[dict] = []
    for t in liquid:
        daily = daily_data.get(t)
        intra = intra_data.get(t)
        if daily is None or intra is None or intra.empty:
            continue
        result = apply_filters(t, daily, intra)
        if result:
            pre_candidates.append(result)

    log.info(f"量化条件通过：{len(pre_candidates)} 只，验证公告...")

    # ── 公告验证 ─────────────────────────────────────────────
    stage1_pass: list[dict] = []
    for c in pre_candidates:
        code     = c["ticker"].replace(".AX", "")
        ann_info = ann_map.get(code)

        if ann_info is None:
            news       = []
            try:
                stock = yf.Ticker(f"{code}.AX")
                today_str = date.today().isoformat()
                for n in (stock.news or [])[:8]:
                    content = n.get("content", {})
                    title   = content.get("title", "")
                    pub     = content.get("pubDate", "")[:10]
                    if title and pub == today_str:
                        news.append({"title": title, "sensitive": False})
            except Exception:
                pass

            if not news:
                log.info(f"  ❌ {c['ticker']}: 无今日公告/新闻，跳过")
                continue
            ann_info        = {"headline": news[0]["title"], "sensitive": False}
            c["ann_source"] = "yfinance"
        else:
            c["ann_source"] = "asx"

        c["ann_headline"]  = ann_info["headline"]
        c["ann_sensitive"] = ann_info["sensitive"]
        stage1_pass.append(c)
        flag = "✅" if c["ann_source"] == "asx" else "⚠️"
        log.info(f"  {flag} {c['ticker']}: +{c['change_pct']}% 量:{c['vol_ratio']}x")

    log.info(f"阶段一完成，{len(stage1_pass)} 只通过")

    if not stage1_pass:
        send_telegram(
            f"📋 <b>First Pullback 早盘扫描 {today}</b>\n\n今日无候选股票。"
        )
        return

    # ── 阶段二：精筛数据采集（历史指标 + 新闻时间线 + 新闻门控）──
    log.info("【阶段二】采集历史指标和新闻时间线（含新闻质量门控）...")

    final_pass: list[dict] = []
    for c in stage1_pass:
        code  = c["ticker"].replace(".AX", "")
        daily = daily_data.get(c["ticker"])

        # 历史指标（复用已有日线，无额外API调用）
        c["hist_metrics"] = compute_historical_metrics(daily) if daily is not None else {}

        # 新闻时间线（触发PDF下载 + 网页正文抓取）
        log.info(f"  📰 {c['ticker']}: 获取新闻时间线...")
        timeline = get_stock_news_timeline(code, days_back=90)

        # ── 硬性门控：无≤2天内新闻/公告则直接DROP ──────────
        # 这是Morning Scanner的核心哲学：没有催化剂就没有故事，
        # 没有故事就没有文章，没有文章就不应该出现在候选列表里。
        if not has_recent_news(timeline, max_days=2):
            log.info(
                f"  🚫 {c['ticker']}: 新闻门控DROP — "
                f"无≤2天内公告/新闻（最新: {timeline[0]['days_ago'] if timeline else 'N/A'}天前）"
            )
            continue

        # 价格-事件对齐：把历史涨跌节点和对应新闻自动关联
        # 让Gemini能直接读取"那条公告导致了那次+8%"的因果关系
        if daily is not None:
            orphan_events = align_price_events_to_news(daily, timeline)
            if orphan_events:
                # 孤立价格事件（无对应新闻的大涨大跌）也存入候选，
                # 可能是内幕消息提前反应，Gemini需要知道
                c["orphan_price_events"] = orphan_events
                log.info(
                    f"  ⚠️ {c['ticker']}: {len(orphan_events)}个孤立价格事件"
                    f"（大涨大跌无对应新闻，提示Gemini核查）"
                )

        c["news_timeline"] = timeline
        final_pass.append(c)
        time.sleep(0.5)

    dropped = len(stage1_pass) - len(final_pass)
    log.info(
        f"阶段二完成：{len(final_pass)} 只通过新闻门控"
        f"（{dropped} 只因无近期新闻DROP）"
    )

    if not final_pass:
        send_telegram(
            f"📋 <b>First Pullback 早盘扫描 {today}</b>\n\n"
            f"技术筛选通过 {len(stage1_pass)} 只，但均无近2天内新闻/公告。\n"
            f"无故事可讲，今日不发候选。"
        )
        return

    # 用通过门控的列表替换（后续阶段统一用 final_pass）
    stage1_pass = final_pass

    # ── 阶段三：Gemini综合分析 ──────────────────────────────
    log.info("【阶段三】Gemini综合分析...")
    ai_results = analyze_candidates_batch(stage1_pass)

    # ── 保存 & 发送主报告 ─────────────────────────────────────
    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(
                {"date": today, "stocks": stage1_pass, "ai": ai_results},
                f, indent=2, default=str,
            )
        with open(ALERTED_FILE, "w") as f:
            json.dump({"date": today, "alerted": []}, f)
    except OSError as e:
        log.error(f"保存文件失败: {e}")

    msg = format_telegram_message(stage1_pass, ai_results, today)
    send_telegram(msg)
    log.info(f"✅ 主报告已发送，{len(stage1_pass)} 个候选")

    # ── 阶段四：日报Prompt生成（零额外API调用，复用已有数据）──
    log.info("【阶段四】生成日报Prompt（Telegram / X / 小红书）...")
    run_report_prompt(stage1_pass, ai_results, today)
    log.info("✅ 扫描全部完成")


# ============================================================
# 日报Prompt生成模块
# Morning版本 vs EOD版本的核心区别：
#   EOD：趋势积累 + 中期逻辑 + 基本面支撑
#   Morning：催化剂突破 + 盘中动量 + 短线入场窗口
# 所有数据来自已完成的扫描阶段，零额外API调用。
# ============================================================

def _build_morning_stock_block(c: dict, ai: dict, rank: int) -> str:
    """
    构建单只股票的完整文案素材包。

    数据分层结构（文案AI可按层取用）：
      Section A — 今日行情快照（量价结构）
      Section B — 事件链时间线（按chain_role分组）
                  🔥 trigger   直接催化剂（含正文+关键数字，这是故事核心）
                  📈 followup  后续进展（催化剂发酵轨迹）
                  🔍 analyst   分析师/机构观点
                  📚 background 历史背景（公司发展脉络）
      Section C — 孤立价格异动（大涨大跌无对应新闻，信息不对称警示）
      Section D — 历史技术摘要
      Section E — AI量化结论（三层故事结构）

    设计原则：正文和关键数字优先展示，标题降级为辅助信息。
    """
    ai      = ai or {}
    metrics = c.get("hist_metrics", {})
    timeline = c.get("news_timeline", [])

    # ── Section A：今日行情快照 ────────────────────────────────
    sl_flag  = "⚠️ 一字拉升（暂不追入）" if c["is_straight"] else f"✅ 回调空间{c['pullback_room']}%"
    src_flag = "ASX官方公告" if c.get("ann_source") == "asx" else "新闻来源"
    sen_flag = "（⭐价格敏感公告）" if c.get("ann_sensitive") else ""

    section_a = (
        f"【今日行情快照】\n"
        f"  涨幅 +{c['change_pct']}% | 量比 {c['vol_ratio']}x"
        f" | 日换手 ${c['dollar_volume']:,}\n"
        f"  价格 ${c['price']} | VWAP ${c['vwap']}（偏离{c['vwap_dist_pct']}%）"
        f" | {sl_flag}\n"
        f"  今日高点 ${c['today_high']} | 启动低点 ${c['launch_pt']}\n"
        f"  催化剂来源: {src_flag}{sen_flag}\n"
        f"  公告标题: {c.get('ann_headline', '无')}"
    )

    # ── Section B：事件链时间线（按chain_role分层）─────────────
    chain_groups: dict[str, list] = {
        "trigger"   : [],
        "followup"  : [],
        "analyst"   : [],
        "background": [],
    }
    for n in timeline:
        role = n.get("chain_role", "background")
        if role in chain_groups:
            chain_groups[role].append(n)

    def _render_news_item(n: dict, body_limit: int, show_body: bool = True) -> str:
        """渲染单条新闻：标题 + 关键数字 + 正文（按体量截取）"""
        days     = n.get("days_ago", "?")
        src      = n.get("source", "未知")
        title    = n.get("title", "")
        body     = n.get("body", "")
        facts    = n.get("key_facts", [])
        move_str = n.get("price_move_str", "")
        sens     = "⭐" if n.get("sensitive") else ""

        move_label = f"  → 股价反应: {move_str}" if move_str else ""
        lines      = [f"  [{days}天前] {sens}{src}: {title}{move_label}"]

        # 关键数字是讲故事的"弹药"，始终展示
        if facts:
            lines.append(f"  📊 关键数字: {' | '.join(facts[:6])}")

        # 正文按重要性决定给多少字
        if show_body and body:
            body_trim = body[:body_limit].rstrip()
            if len(body) > body_limit:
                body_trim += "..."
            lines.append(f"  📝 正文: {body_trim}")

        return "\n".join(lines)

    timeline_sections = []

    # trigger：最重要，正文给足400字，最多2条
    if chain_groups["trigger"]:
        timeline_sections.append("🔥【直接催化剂 — 今日涨幅的触发事件（最重要）】")
        for n in chain_groups["trigger"][:2]:
            timeline_sections.append(_render_news_item(n, body_limit=400, show_body=True))

    # followup：后续进展，正文200字，最多3条
    if chain_groups["followup"]:
        timeline_sections.append("\n📈【后续进展 — 催化剂发酵轨迹】")
        for n in chain_groups["followup"][:3]:
            timeline_sections.append(_render_news_item(n, body_limit=200, show_body=True))

    # analyst：机构观点，正文150字，最多2条
    if chain_groups["analyst"]:
        timeline_sections.append("\n🔍【分析师/机构观点】")
        for n in chain_groups["analyst"][:2]:
            timeline_sections.append(_render_news_item(n, body_limit=150, show_body=True))

    # background：历史背景，只显示标题+关键数字（不给正文，节省token）
    if chain_groups["background"]:
        timeline_sections.append("\n📚【历史背景 — 理解公司发展脉络，帮助判断今日催化剂是否超预期】")
        for n in chain_groups["background"][:4]:
            timeline_sections.append(_render_news_item(n, body_limit=0, show_body=False))

    section_b = (
        "【事件链时间线（按催化剂角色分层）】\n"
        + ("\n".join(timeline_sections) if timeline_sections else "  暂无新闻/公告数据")
    )

    # ── Section C：孤立价格事件（大涨大跌无对应新闻）───────────
    orphans     = c.get("orphan_price_events", [])
    section_c   = ""
    if orphans:
        orphan_lines = [
            f"  {e['date']}: 单日{'+' if e['change_pct']>0 else ''}{e['change_pct']}%"
            f"（无对应公告）"
            for e in orphans[:4]
        ]
        section_c = (
            "\n❓【孤立价格异动 — 大涨大跌但无对应公告，可能存在信息不对称】\n"
            "（写作时请提示读者核查是否有未公开信息）\n"
            + "\n".join(orphan_lines)
        )

    # ── Section D：历史技术摘要 ────────────────────────────────
    section_d = (
        "【历史技术摘要（近6个月）】\n"
        f"  趋势: {metrics.get('trend','?')} | "
        f"RSI: {metrics.get('rsi_14','?')} | "
        f"5日涨: {metrics.get('ret_5d_pct','?')}% | "
        f"20日涨: {metrics.get('ret_20d_pct','?')}% | "
        f"60日涨: {metrics.get('ret_60d_pct','?')}%\n"
        f"  年化波动: {metrics.get('vol_annualized_pct','?')}% | "
        f"距历史高点: {metrics.get('pct_from_period_high','?')}% | "
        f"量能趋势(近5日/20日均量): {metrics.get('vol_trend_5v20','?')}x"
    )

    # ── Section E：AI量化结论（三层故事结构）────────────────────
    ai_lines = []
    verdict  = ai.get("verdict", "")
    conf     = ai.get("confidence", "")
    if verdict:
        ai_lines.append(f"  量化判断: {verdict}（{conf}信心）")
    if ai.get("catalyst"):
        ai_lines.append(f"  🔥 今日触发: {ai['catalyst']}")
    if ai.get("backstory"):
        ai_lines.append(f"  📖 历史背景: {ai['backstory']}")
    if ai.get("story_chain"):
        ai_lines.append(f"  🔗 因果链: {ai['story_chain']}")
    if ai.get("short_term_view"):
        ai_lines.append(f"  📈 短期展望: {ai['short_term_view']}")
    if ai.get("entry_note"):
        ai_lines.append(f"  🎯 入场参考: {ai['entry_note']}")

    section_e = (
        "【AI量化初步结论（三层故事结构，供文案参考）】\n"
        + ("\n".join(ai_lines) if ai_lines else "  暂无AI分析")
    )

    # ── 拼装完整数据块 ────────────────────────────────────────
    parts = [
        f"\n{'='*56}",
        f"#{rank} {c['ticker']}",
        f"{'='*56}",
        section_a,
        "",
        section_b,
    ]
    if section_c:
        parts.append(section_c)
    parts += ["", section_d, "", section_e]

    return "\n".join(parts)


def _build_morning_prompt(platform: str, stocks_block: str,
                          today: str, n_candidates: int) -> str:
    """
    生成各平台的最终Prompt文本。

    数据包说明注入策略：
    - 在数据包引言里明确告诉文案AI各字段含义
    - 强制三层叙事结构（触发→背景→因果链）
    - 明确指出chain_role的含义，避免AI忽略事件分类
    - 孤立价格异动单独提示，要求文案AI处理信息不对称问题
    """
    # 数据包说明（三个平台共用，避免重复写）
    data_guide = (
        "【数据包说明 — 请务必阅读后再生成内容】\n"
        "每只股票的数据按以下结构组织：\n"
        "  🔥 直接催化剂(trigger)：今日涨幅的直接触发事件，含正文摘要和关键数字\n"
        "     → 这是故事的核心，必须具体引用，不能泛化\n"
        "  📈 后续进展(followup)：催化剂发酵的中间节点，说明市场如何消化这个事件\n"
        "  🔍 分析师观点(analyst)：机构如何解读，帮助判断市场共识\n"
        "  📚 历史背景(background)：公司此前的铺垫，解释为什么今天这件事重要\n"
        "  ❓ 孤立价格异动：大涨大跌但无对应公告，写作时须提示读者核查\n"
        "  AI量化结论：已提供catalyst/backstory/story_chain三层分析，可直接引用\n"
        "数据优先级：公告PDF正文 > ASX公告标题 > 新闻正文 > 技术指标"
    )

    instructions = {
        "seo": """You are a short-term trading research analyst specializing in US equities.

Your task is to generate a high-value intraday market analysis report based strictly on real-time scanner data captured within the first 30 minutes after market open (NOT EOD / closing data).

⸻

Core Objective

* Identify early-session momentum and abnormal market behavior
* Extract actionable trading signals
* Maintain strict separation from end-of-day analysis logic

⸻

Critical Constraint (Mandatory)

You MUST explicitly state at the beginning:

This analysis is based on intraday scanner data captured within the first 30 minutes after market open (not end-of-day data).

This is required to avoid misinterpretation as EOD analysis.

⸻

Output Requirement (STRICT)

You must output TWO versions of the report:

1. English version
2. Chinese version

Each version must be placed in a separate code block.

⸻

Content Requirements (Flexible Structure, Not Template-Bound)

1. Title (CTR optimized)

* Must include:
    * “Today / Early Session”
    * Price movement or % change if available
    * Catalyst or driver keyword (earnings / placement / momentum / breakout)
* Avoid repetitive phrasing across outputs

⸻

2. Opening Context (Mandatory)

Must include:

* Market tone (risk-on / risk-off / rotation)
* Key abnormal early-session behavior
* Scanner time reference (early session only)

⸻

3. Catalyst & Interpretation Layer

Explain:

* What triggered the move (event / flow / technical breakout)
* Why the market is reacting now
* Why this is meaningful in an intraday context

Avoid listing facts only — must include interpretation.

⸻

4. Intraday Trading Signal Module (Core Differentiator)

For each key opportunity, define:

* Trigger Condition
    * Specific price/volume/structure condition
* Entry Logic
    * Breakout / pullback / continuation behavior
* Invalidation Condition
    * What would invalidate the setup
* Optional Risk Context
    * Support / resistance / VWAP interaction

All logic must be based on real-time observable conditions, NOT closing price confirmation.

⸻

5. Market Structure Overview

Briefly describe:

* Sector leadership (if relevant)
* Capital rotation behavior
* Index influence (if any)

⸻

6. FAQ Section (Long-tail SEO + clarity)

Must include at least 4 questions:

* Why is this stock moving today?
* Why use intraday scanner data instead of EOD?
* Is this move sustainable during the session?
* What key levels matter for traders?

Answers must be concise and professional.

⸻

Style Rules

* Avoid repetitive sentence structures across reports
* Do NOT use rigid templates
* Prioritize interpretation over raw data listing
* Write in a trader’s analytical voice, not a news reporter tone

⸻

Hard Prohibitions

* No EOD / closing price framing
* No hindsight bias language (“closed above”, “at close”)
* No pure data dumping without interpretation
* No repetitive identical phrasing across outputs

⸻

Subtle Professional Requirement

Naturally incorporate:

* Intraday-only validity of signals
* Distinction between live trading vs post-market analysis
* Conditional logic rather than deterministic prediction""",

        "twitter": """Generate an English X (Twitter) thread — ASX morning catalyst report ({today}).

MANDATORY NARRATIVE RULE — Each stock tweet answers THREE questions:
  Q1 What happened? (trigger event — cite specific data/number from the announcement)
  Q2 Why does this matter? (background — what was expected, did this beat it?)
  Q3 What's the story arc? (story_chain — 'X months ago → milestone → today confirmed')

FORMAT:
Tweet 1 (hook ≤250 chars): Biggest move of the day + catalyst theme
Tweets 2–{n_candidates + 1}: One per stock:
  Line 1: $ASX:TICKER +X% | [Trigger: specific announcement + key number]
  Line 2: [Why it matters + story arc in 1 line] | Buy/Watch/Avoid
  Note: Flag 'unconfirmed' for any orphan price move without announcement
Final tweet: Key risk + #ASX #Catalyst #AustralianStocks + disclaimer

Rules: Convert numbers to judgment language. ≤280 chars/tweet. Separator: ---TWEET---""",

        "xiaohongshu": """生成**中文**小红书投资笔记（今日{today}，共{n_candidates}只早盘异动股）。

【叙事定位】：不是数据播报员，是"投资侦探"——
你发现了今日涨停背后的完整故事，从几个月前的铺垫写到今天的爆发，
让读者恍然大悟"原来这家公司早就埋了这颗种子"。

【每只股票必须包含三层故事】：
  ① 今天的引爆点（trigger事件，必须有具体内容，引用关键数字）
  ② 为什么今天这件事让股价爆了（background，逻辑解释）
  ③ 时间线上的伏笔（story_chain，"X个月前→...→今日兑现"）

【输出格式】：
标题（≤20字，钩子式，含情绪词，例如"这家公司埋了X个月的局，今天爆了"）
开头钩子（2句，引发好奇心，不剧透答案）
每只股票：
  🔍 公司是谁（1句行业+主营）
  💥 今天发生了什么（trigger，含具体数字）
  🏗️ 背景故事（2-3句，为什么重要）
  🔗 时间线伏笔（1-2句因果链）
  ❓ 如有孤立价格异动，提示读者"X日曾有不明原因大涨，需核查"
  📊 技术信号（1句，口语化，不堆数字）
  🎯 我的判断（买入/观望/回避 + 1句理由）
结尾：今日投资启示（1句心得）+ 风险提示
话题：#澳股 #ASX投资 #股票分析 #早盘异动（可加行业标签）
写作风格：专业但亲切，像朋友分享干货；不确定写"待核查"；末尾免责声明。""",
    }

    instruction = instructions.get(platform, instructions["seo"])

    return (
        f"📋 <b>ASX早盘文案Prompt — {platform.upper()} — {today}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👇 复制以下全部内容给AI生成文章</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"你是一位专注澳大利亚股市(ASX)的资深短线分析师，"
        f"擅长用催化剂驱动的叙事方式分析个股异动。\n\n"
        f"{data_guide}\n\n"
        f"=== 今日早盘数据包"
        f"（{n_candidates}只候选，均已通过新闻门控：≤2天内有公告/新闻）===\n"
        f"{stocks_block}\n\n"
        f"=== 输出任务 ===\n"
        f"{instruction}\n\n"
        f"=== 全局规则（优先级最高，任何平台均适用）===\n"
        f"1. 数据优先级：公告PDF正文 > ASX公告标题 > 新闻正文 > 技术指标\n"
        f"2. 禁止罗列原始数字，必须转化为投资判断语言\n"
        f"3. 无正文的字段写'正文未获取，建议查看ASX原文'\n"
        f"4. 孤立价格异动（❓标注）必须在文中提示读者核查\n"
        f"5. 三层叙事（触发→背景→因果链）缺一不可，不得压缩成一句话\n"
        f"6. 所有内容不构成投资建议，末尾必须加免责声明\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )


def run_report_prompt(
    candidates: list[dict],
    ai_results: dict[str, dict],
    today: str,
) -> None:
    """
    日报Prompt生成主函数。
    输入：stage1_pass（已含news_timeline、hist_metrics、orphan_price_events）+ ai_results
    输出：向Telegram发送三个平台的Prompt文本，用户复制给AI生成文案。
    不调用Gemini，不产生额外API费用。

    发送策略：
    - 每个平台Prompt之间等待3秒，避免Telegram限速
    - 每个Prompt发送前先发一条简短header，标注平台和股票数
    - send_telegram() 内部已按段落自动分段，无需手动切割
    """
    if not candidates:
        log.warning("run_report_prompt：无候选股，跳过")
        return

    # 按verdict排序：买入优先（文案聚焦最强信号）
    verdict_order = {"买入": 0, "观望": 1, "回避": 2}
    sorted_candidates = sorted(
        candidates,
        key=lambda c: (
            verdict_order.get(
                ai_results.get(c["ticker"], {}).get("verdict", "观望"), 1
            ),
            -c["change_pct"],
        ),
    )
    n_candidates = len(sorted_candidates)

    # 构建股票数据块（所有平台共用同一份数据包）
    log.info(f"构建{n_candidates}只股票的文案数据包...")
    stock_blocks: list[str] = []
    for rank, c in enumerate(sorted_candidates, 1):
        ai    = ai_results.get(c["ticker"], {})
        block = _build_morning_stock_block(c, ai, rank)
        stock_blocks.append(block)

        # 统计各层内容丰富度（便于debug）
        timeline   = c.get("news_timeline", [])
        has_body   = sum(1 for n in timeline if n.get("body"))
        has_facts  = sum(1 for n in timeline if n.get("key_facts"))
        has_orphan = len(c.get("orphan_price_events", []))
        log.info(
            f"  #{rank} {c['ticker']}: "
            f"新闻{len(timeline)}条 | 含正文{has_body} | "
            f"含关键数字{has_facts} | 孤立事件{has_orphan}"
        )

    stocks_block = "\n".join(stock_blocks)

    # 发送三个平台Prompt（每个平台之间3秒间隔）
    platform_labels = {
        "seo"         : "SEO 网页文章",
        "twitter"     : "X/Twitter 英文Thread",
        "xiaohongshu" : "小红书 投资笔记",
    }
    for platform in ["seo", "twitter", "xiaohongshu"]:
        label = platform_labels[platform]
        log.info(f"  发送 [{label}] Prompt...")

        # 先发一条简短header，用户知道接下来是什么
        send_telegram(
            f"📝 <b>文案Prompt — {label}</b>\n"
            f"共{n_candidates}只股票 | 复制以下内容给AI生成文章 👇"
        )
        time.sleep(1.0)

        prompt_text = _build_morning_prompt(
            platform, stocks_block, today, n_candidates
        )
        send_telegram(prompt_text)
        log.info(f"  ✅ [{label}] Prompt已发送（{len(prompt_text)}字符）")
        time.sleep(3.0)  # 平台间间隔，避免Telegram限速

    log.info(
        f"日报Prompt全部发送完成 | "
        f"{n_candidates}只股票 × 3平台 | 今日{today}"
    )


if __name__ == "__main__":
    run_morning_scan()
