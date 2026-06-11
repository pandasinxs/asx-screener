# ============================================================
# FIRST PULLBACK — MORNING SCANNER v5
# 升级点:
#   1. 重试逻辑覆盖所有可重试异常（网络/超时/5xx/429）
#   2. 筛选指标全面强化（价格下限、量能门槛、VWAP距离）
#   3. 三阶段流程：粗筛 → 新闻时间线+历史指标精筛 → Gemini综合分析
#   4. Gemini输出结构化JSON，Telegram格式化呈现
# ============================================================

import os
import json
import time
import logging
from datetime import datetime, date, timezone, timedelta
from typing import Optional

import yfinance as yf
import numpy as np
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

# ── 环境变量 ─────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

GEMINI_MODEL = "gemini-2.5-flash-preview-05-14"
gemini_client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

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

def get_recent_announcements(hours_back: int = 72) -> dict:
    """批量拉取ASX近期公告"""
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
    获取单只股票的历史新闻，构建时间线。
    同时抓取ASX公告API（按symbol）+ yfinance新闻。
    返回按日期降序排列的新闻列表，最多20条。
    """
    today_dt = date.today()
    cutoff   = (today_dt - timedelta(days=days_back)).isoformat()
    timeline = []

    # 1) ASX官方公告（按symbol查询）
    try:
        r = requests.get(
            f"https://asx.api.markitdigital.com/asx-research/1.0/company/{code}/announcements",
            params={"count": 20},
            headers=ASX_HEADERS,
            timeout=15,
        )
        if r.ok:
            for item in r.json().get("data", {}).get("announcements", []):
                ann_date = item.get("date", "")[:10]
                if ann_date >= cutoff:
                    days_ago = (today_dt - date.fromisoformat(ann_date)).days
                    timeline.append({
                        "date"     : ann_date,
                        "days_ago" : days_ago,
                        "source"   : "ASX公告",
                        "title"    : item.get("headline", "")[:100],
                        "sensitive": item.get("isPriceSensitive", False),
                    })
    except Exception as e:
        log.debug(f"ASX公告时间线失败 [{code}]: {e}")

    # 2) yfinance新闻（补充覆盖）
    try:
        stock = yf.Ticker(f"{code}.AX")
        for n in (stock.news or [])[:15]:
            content  = n.get("content", {})
            title    = content.get("title", "")
            pub      = content.get("pubDate", "")[:10]
            if title and pub >= cutoff:
                days_ago = (today_dt - date.fromisoformat(pub)).days if pub else 999
                timeline.append({
                    "date"     : pub,
                    "days_ago" : days_ago,
                    "source"   : content.get("provider", {}).get("displayName", "新闻"),
                    "title"    : title[:100],
                    "sensitive": False,
                })
    except Exception as e:
        log.debug(f"yfinance新闻失败 [{code}]: {e}")

    # 去重（按标题前40字）+ 时间降序
    seen   = set()
    unique = []
    for item in sorted(timeline, key=lambda x: x["date"], reverse=True):
        key = item["title"][:40]
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique[:20]


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
    c  = df["Close"].squeeze()
    h  = df["High"].squeeze()
    l  = df["Low"].squeeze()
    v  = df["Volume"].squeeze()
    tp = (h + l + c) / 3
    return (tp * v).cumsum() / v.cumsum()


def compute_historical_metrics(daily: pd.DataFrame) -> dict:
    """
    将180天日线数据压缩为关键指标字典，供Gemini消费。
    避免原始数据直接传入造成token浪费。
    """
    closes  = daily["Close"].squeeze().dropna()
    volumes = daily["Volume"].squeeze().dropna()
    highs   = daily["High"].squeeze().dropna()
    lows    = daily["Low"].squeeze().dropna()

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
        closes     = daily["Close"].squeeze()
        prev_close = float(closes.iloc[-2])
        curr_price = float(intra["Close"].squeeze().iloc[-1])

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
        today_vol   = float(intra["Volume"].squeeze().sum())
        avg_day_vol = float(daily["Volume"].squeeze().iloc[-20:].mean())
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
        today_high  = float(intra["High"].squeeze().max())
        today_low   = float(intra["Low"].squeeze().min())
        launch_pt   = float(intra["Low"].squeeze().iloc[0])

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
    构建批量分析Prompt。
    设计原则：
    - 结构化输入（减少Gemini理解歧义）
    - 明确JSON输出schema（方便解析）
    - 限定每只股票分析字数（节省token，避免废话）
    """
    blocks = []
    for c in candidates:
        code    = c["ticker"].replace(".AX", "")
        metrics = c.get("hist_metrics", {})
        news    = c.get("news_timeline", [])

        # 新闻时间线压缩（最多7条，显示距今天数）
        news_lines = []
        for n in news[:7]:
            days  = n.get("days_ago", "?")
            src   = n.get("source", "")
            title = n.get("title", "")
            flag  = "⭐" if n.get("sensitive") else ""
            news_lines.append(f"  [{days}天前] {flag}{src}: {title}")
        news_text = "\n".join(news_lines) if news_lines else "  无近期公告/新闻"

        block = f"""
--- {c['ticker']} ---
今日: 涨{c['change_pct']}% | 量比{c['vol_ratio']}x | 换手${c['dollar_volume']:,}
价格: 现${c['price']} | VWAP${c['vwap']} | 距VWAP{c['vwap_dist_pct']}% | {"⚠️一字拉升" if c['is_straight'] else f"回调空间{c['pullback_room']}%"}
技术: {metrics.get('trend','?')} | RSI={metrics.get('rsi_14','?')} | 5日涨{metrics.get('ret_5d_pct','?')}% | 20日涨{metrics.get('ret_20d_pct','?')}%
波动率: 年化{metrics.get('vol_annualized_pct','?')}% | 距高点{metrics.get('pct_from_period_high','?')}%
近期新闻/公告:
{news_text}"""
        blocks.append(block)

    stocks_section = "\n".join(blocks)

    prompt = f"""你是专业的ASX短线量化分析师，今日为{date.today().isoformat()}。
以下股票均已通过量化初筛（涨幅≥10%、量比≥1.5x、价格在VWAP 5%以内）。

{stocks_section}

请对每只股票输出结构化分析，严格按照以下JSON格式，不要输出任何其他内容：

{{
  "TICKER.AX": {{
    "verdict": "买入" | "观望" | "回避",
    "confidence": "高" | "中" | "低",
    "catalyst": "1句话概括催化剂逻辑（新闻/公告驱动是否成立）",
    "news_summary": "1-2句话概括新闻时间线的关键信息和趋势",
    "short_term_view": "1-2句话预测未来1-3日短期走势及主要风险",
    "entry_note": "具体入场参考（如：回踩VWAP $X.XX附近建仓 / 一字板暂不追入 / 等待放量确认）"
  }}
}}

分析要求：
- verdict=买入：需要催化剂明确 + 技术结构健康 + 非一字拉升
- verdict=回避：一字板 / 纯炒作无实质公告 / 已严重超买（RSI>80且距高点<5%）
- 所有分析必须基于提供的数据，不确定的用"需核查"
- 每个字段严格控制在规定句数内"""

    return prompt


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
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过通知")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        if not resp.ok:
            log.error(f"Telegram发送失败: {resp.status_code} {resp.text[:100]}")
    except requests.RequestException as e:
        log.error(f"Telegram请求异常: {e}")


def format_telegram_message(candidates: list[dict], ai_results: dict, today: str) -> str:
    """格式化最终Telegram消息"""
    # 按verdict优先级排序：买入 > 观望 > 回避
    verdict_order = {"买入": 0, "观望": 1, "回避": 2}
    candidates.sort(
        key=lambda c: (
            verdict_order.get(
                ai_results.get(c["ticker"], {}).get("verdict", "观望"), 1
            ),
            -c["change_pct"],
        )
    )

    lines = [f"⚡ <b>First Pullback 候选 {today}</b>\n"]

    for c in candidates:
        ai  = ai_results.get(c["ticker"], {})
        verdict = ai.get("verdict", "—")
        conf    = ai.get("confidence", "—")
        verdict_emoji = {"买入": "🟢", "观望": "🟡", "回避": "🔴"}.get(verdict, "⚪")

        src_flag = "📋 ASX" if c.get("ann_source") == "asx" else "📰 新闻"
        sen_flag = "⭐" if c.get("ann_sensitive") else ""
        sl_flag  = "⚠️ 一字" if c["is_straight"] else f"↩ 回调{c['pullback_room']}%"

        lines.append(
            f"{verdict_emoji} <b>{c['ticker']}</b>  {verdict}({conf})  "
            f"+{c['change_pct']}%  量:{c['vol_ratio']}x\n"
            f"   💰 ${c['price']} | VWAP ${c['vwap']}(±{c['vwap_dist_pct']}%) | {sl_flag}\n"
            f"   {src_flag}{sen_flag} {c.get('ann_headline','')}\n"
        )

        if ai.get("catalyst"):
            lines.append(f"   📌 催化: {ai['catalyst']}\n")
        if ai.get("news_summary"):
            lines.append(f"   📰 新闻: {ai['news_summary']}\n")
        if ai.get("short_term_view"):
            lines.append(f"   📈 短期: {ai['short_term_view']}\n")
        if ai.get("entry_note"):
            lines.append(f"   🎯 入场: {ai['entry_note']}\n")

    lines.append(
        "\n─────────────────\n"
        "⚠️ 止损: 跌破启动低点或 -8%\n"
        "💰 止盈: +10%锁半仓，+20%清仓"
    )
    return "\n".join(lines)


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
            avg_vol    = float(df["Volume"].iloc[-20:].mean())
            last_close = float(df["Close"].squeeze().iloc[-1])
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

    # ── 阶段二：精筛数据采集（历史指标 + 新闻时间线）──────────
    log.info("【阶段二】采集历史指标和新闻时间线...")

    for c in stage1_pass:
        code = c["ticker"].replace(".AX", "")

        # 历史指标（从已有日线数据计算，无需额外API调用）
        daily = daily_data.get(c["ticker"])
        c["hist_metrics"] = compute_historical_metrics(daily) if daily is not None else {}

        # 新闻时间线（这里会额外调用ASX+yfinance）
        log.info(f"  📰 {c['ticker']}: 获取新闻时间线...")
        c["news_timeline"] = get_stock_news_timeline(code, days_back=90)
        time.sleep(0.5)  # 避免ASX API限速

    # ── 阶段三：Gemini综合分析 ──────────────────────────────
    log.info("【阶段三】Gemini综合分析...")
    ai_results = analyze_candidates_batch(stage1_pass)

    # ── 保存 & 发送 ──────────────────────────────────────────
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
    log.info(f"✅ 扫描完成，{len(stage1_pass)} 个候选，报告已发送")


if __name__ == "__main__":
    run_morning_scan()