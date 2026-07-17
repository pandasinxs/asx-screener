# ============================================================
# ASX SYSTEM — screener.py  v18.3
#
# 流程一：EOD选股
#   全市场K线 → T1-T4筛选 → Top3加权评分 → 新闻/公告时间线
#   → Gemini分析 → Telegram → 解析JSON标签字段 → signals.json → GitHub推送
#
# 流程二：SEO文章逐只生成（Top1/Top2/Top3各自独立调用Gemini）
#   → 英文+中文文章 → 校验 → 写入 → 独立commit推送GitHub
#   → 任一失败：Telegram告警 + 该股票专属.txt Prompt人工兜底
#
# 流程三：每日日报Prompt（Twitter / 小红书，人工使用，不调用Gemini）
#
# v15/v16/v17/v18/v18.1/v18.2 changelog见历史版本，此处从v18.3开始记录：
#
# v18.3新增（本轮，用户反馈）：
#   1) 删除试运行模式：SEO_DRY_RUN环境变量、TEST_OUTPUT_DIR_EN/ZH、
#      run_seo_article_flow的dry_run参数、_write_seo_article_files的
#      dir_en/dir_zh参数全部删除，每次运行都是正式模式。
#   2) 保留用户自己修复的bug：_validate_seo_article_fields()不再检查
#      "faq"关键词是否出现在正文（中文文章不会出现英文单词"FAQ"，
#      这条校验此前对中文文章必然误判）。
#   3) 所有喂给Gemini的"data block"（技术数据/时间线/市场快照）
#      全部改写为纯英文、不带主观定性描述，尽量给原始数值：
#        - build_tech_summary()新增vwap_slope原始斜率值
#        - get_market_snapshot()新增dev_from_ma50_pct原始偏离度
#        - build_timeline_text()/_build_report_stock_block()/
#          _build_screener_prompt()内tech_block/market_block
#          全部改英文，去掉"量能无持续性"这类定性描述和情绪化emoji，
#          改为陈述原始事实+中性定义说明
#   4) 审计并补齐API调用的重试机制：
#        - _get()/get_asx_universe()/get_market_snapshot()/
#          _extract_pdf_keywords()/fetch_news()/send_telegram()/
#          send_document()/push_to_github()新增重试+退避
#        - ask_gemini()的可重试错误类型扩展到识别通用网络异常
#          （此前只匹配限速相关字符串，网络超时会被判定为不可重试）
#        - screener.log路径从相对路径改为绝对路径（锚定脚本目录），
#          与announcements.db/tier_validation.log保持一致
# ============================================================

import os, io, re, sys, time, logging, json, subprocess
import xml.etree.ElementTree as ET
import requests
import yfinance as yf
import pandas as pd
import pdfplumber
import watchlist_db as wdb
from datetime import datetime, date, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional
from google import genai

# ════════════════════════════════════════════════════════════
# 0. 日志 & 环境变量
# ════════════════════════════════════════════════════════════

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH   = os.path.join(_SCRIPT_DIR, "screener.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
gemini_client  = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

# ════════════════════════════════════════════════════════════
# 1. 常量 & 配置
# ════════════════════════════════════════════════════════════

GEMINI_MODEL    = "gemini-2.5-flash"
GEMINI_CFG_DEEP = {"thinking_config": {"thinking_budget": 512}}

GEMINI_CFG_SEO_ARTICLE = {
    "thinking_config": {"thinking_budget": 1024},
    "max_output_tokens": 65535,
}

RETRY_MAX       = 30
RETRY_WAIT      = 30
TIMEOUT         = 15
TOP_N           = 3

NET_RETRY_MAX  = 10
NET_RETRY_WAIT = 2.0

ASXBOX_REPO  = os.path.expanduser("~/asxbox")
SIGNALS_DIR  = os.path.join(ASXBOX_REPO, "src", "data", "signals")

BLOG_CONTENT_DIR_EN = os.path.join(ASXBOX_REPO, "src", "content", "blog", "en")
BLOG_CONTENT_DIR_ZH = os.path.join(ASXBOX_REPO, "src", "content", "blog", "zh")

SEO_ARTICLE_MIN_CHARS = 600

ASX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept":     "application/json",
    "Referer":    "https://www.asx.com.au",
}
ASX_ANN_ALL  = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
PDF_DL_BASE  = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/{doc_key}?access_token=83ff96335c2d45a094df02a206a39ff4"
GOOGLE_RSS   = "https://news.google.com/rss/search?q={q}&hl=en-AU&gl=AU&ceid=AU:en"

PDF_MAX_CHARS     = 2000
PDF_MAX_PER_STOCK = 2
NEWS_MAX          = 5

BT_STOP_ATR_MULT   = 2
BT_TARGET_ATR_MULT = 4
BT_TIMEOUT_DAYS    = 20

ANN_WHITELIST = {
    "Quarterly Activities Report", "Quarterly Cashflow Report",
    "Half Yearly Report", "Preliminary Final Report", "Annual Report",
    "Full Year Results", "Half Year Results",
    "Appendix 4C", "Appendix 4D", "Appendix 4E",
    "Quarterly Production Report", "Resource/Reserve Update",
    "Exploration Results", "Drilling Results", "Mining Results",
    "Results of Operations", "Merger/Acquisition", "Takeover",
    "Scheme of Arrangement", "Strategic Review",
    "Major Contract", "Material Contract",
    "Capital Raising", "Placement", "Rights Issue", "Share Purchase Plan",
    "CEO/Chairman Change", "Director Change",
    "Suspension", "Trading Halt", "Trading Halt Lifted",
    "Guidance", "Market Update", "Business Update",
    "Investor Presentation", "Progress Report", "Project Update",
}

ANN_NOISE_KEYWORDS = [
    "appendix 3", "change of address", "change of registered",
    "notice of meeting", "proxy form", "lodge", "constitution",
    "cleansing statement", "reinstatement", "transfer of interest",
    "share registry", "cease to be", "becoming substantial",
    "shareholder", "top 20", "section 708",
]

PDF_KEY_TERMS = [
    "revenue", "production", "guidance", "result", "profit", "loss",
    "cash", "ebitda", "npat", "highlights", "outlook", "summary",
    "drill", "resource", "reserve", "acquisition", "contract",
    "milestone", "update", "completion", "approval", "forecast",
]

SCORE_WEIGHTS = {
    "trend_strength": 0.50,
    "persistence"    : 0.20,
    "catalyst"       : 0.15,
    "price_pct_1y"   : 0.15,
}

TIERS = [
    {
        "level": "T1", "label": "🔴 精英",
        "vol_mult": 2.0, "close_pos": 0.88, "consol": 0.12,
        "rsi_lo": 45, "rsi_hi": 65, "adx_min": 28, "di_cross": True,
        "vwap_above": True, "rs_min": 1.05, "vol_decline": True,
        "near_52w_hi": True,
        "note": "最高质量：ADX趋势成形，跑赢大盘，量价配合",
    },
    {
        "level": "T2", "label": "🟠 优质",
        "vol_mult": 1.5, "close_pos": 0.75, "consol": 0.15,
        "rsi_lo": 42, "rsi_hi": 68, "adx_min": 25, "di_cross": True,
        "vwap_above": True, "rs_min": 1.02, "vol_decline": True,
        "near_52w_hi": True,
        "note": "高质量信号，趋势明确",
    },
    {
        "level": "T3", "label": "🟡 标准",
        "vol_mult": 1.2, "close_pos": 0.60, "consol": 0.20,
        "rsi_lo": 38, "rsi_hi": 72, "adx_min": 20, "di_cross": True,
        "vwap_above": True, "rs_min": 1.0, "vol_decline": True,
        "near_52w_hi": False,
        "note": "标准质量，趋势初步形成",
    },
    {
        "level": "T4", "label": "🟢 放宽",
        "vol_mult": 1.0, "close_pos": 0.50, "consol": 0.25,
        "rsi_lo": 35, "rsi_hi": 75, "adx_min": 15, "di_cross": False,
        "vwap_above": True, "rs_min": 0.98, "vol_decline": False,
        "near_52w_hi": False,
        "note": "参考信号，需结合基本面判断",
    },
]

# ════════════════════════════════════════════════════════════
# 2. 技术指标
# ════════════════════════════════════════════════════════════

def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = -delta.clip(upper=0).rolling(period).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))


def calc_adx(high: pd.Series, low: pd.Series,
             close: pd.Series, period: int = 14) -> tuple:
    prev_c = close.shift(1)
    tr = pd.concat([
        high - low, (high - prev_c).abs(), (low - prev_c).abs()
    ], axis=1).max(axis=1)
    up, down = high - high.shift(1), low.shift(1) - low
    pdm = up.where((up > down) & (up > 0), 0.0)
    mdm = down.where((down > up) & (down > 0), 0.0)
    atr = tr.rolling(period).mean()
    pdi = 100 * (pdm.rolling(period).mean() / atr.replace(0, 1e-10))
    mdi = 100 * (mdm.rolling(period).mean() / atr.replace(0, 1e-10))
    dx  = 100 * ((pdi - mdi).abs() / (pdi + mdi).replace(0, 1e-10))
    return dx.rolling(period).mean(), pdi, mdi


def calc_vwap_slope(close: pd.Series, volume: pd.Series,
                    window: int = 20) -> tuple:
    vwap  = (close * volume).rolling(window).sum() / volume.rolling(window).sum()
    slope = float(vwap.iloc[-1]) - float(vwap.iloc[-6])
    return float(vwap.iloc[-1]), slope


def calc_rs(close: pd.Series, bench: pd.Series, period: int = 20) -> float:
    try:
        sr = float(close.iloc[-1]) / float(close.iloc[-period]) - 1
        br = float(bench.iloc[-1]) / float(bench.iloc[-period]) - 1
        return round((1 + sr) / (1 + br), 3) if abs(br) > 1e-6 else 1.0
    except Exception:
        return 1.0


def calc_price_events(close: pd.Series, threshold_pct: float = 5.0) -> list:
    pct    = close.pct_change() * 100
    recent = pct.iloc[-126:]
    events = []
    for dt, val in recent.items():
        if abs(val) >= threshold_pct:
            events.append({"date": str(dt)[:10], "change_pct": round(float(val), 1)})
    return sorted(events, key=lambda x: x["date"], reverse=True)[:10]


def build_tech_summary(df: pd.DataFrame,
                       xjo: Optional[pd.Series] = None) -> dict:
    close  = df["Close"].squeeze()
    high   = df["High"].squeeze()
    low    = df["Low"].squeeze()
    volume = df["Volume"].squeeze()

    lc   = float(close.iloc[-1])
    lh   = float(high.iloc[-1])
    ll   = float(low.iloc[-1])
    lv   = float(volume.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) >= 2 else lc
    vm20 = float(volume.rolling(20).mean().iloc[-1])

    adx_s, pdi_s, mdi_s = calc_adx(high, low, close)
    vwap_val, vwap_slope = calc_vwap_slope(close, volume)
    rsi_s                = calc_rsi(close)
    ma20                 = close.rolling(20).mean()
    ma50                 = close.rolling(50).mean()
    ma200                = close.rolling(200).mean()
    prev_c               = close.shift(1)
    tr                   = pd.concat([
        high - low, (high - prev_c).abs(), (low - prev_c).abs()
    ], axis=1).max(axis=1)
    atr14  = float(tr.rolling(14).mean().iloc[-1])
    w52_hi = float(high.rolling(min(252, len(high))).max().iloc[-1])
    w52_lo = float(low.rolling(min(252, len(low))).min().iloc[-1])

    roll_max  = close.rolling(126).max()
    max_dd    = float(((close - roll_max) / roll_max * 100).min())
    day_range = lh - ll
    close_pos = (lc - ll) / day_range if day_range > 0 else 0.5

    hist_close   = close.iloc[-252:] if len(close) >= 252 else close
    price_pct_1y = round(float((hist_close <= lc).sum() / len(hist_close) * 100), 1)

    vol5 = volume.iloc[-5:]
    vol_consistency = bool(all(
        vol5.iloc[i] <= vol5.iloc[i + 1] for i in range(len(vol5) - 1)
    ))

    price_events = calc_price_events(close)

    return {
        "price"          : round(lc, 3),
        "change_pct"     : round((lc / prev - 1) * 100, 2),
        "volume"         : round(lv),
        "vol_ratio"      : round(lv / vm20, 2) if vm20 > 0 else 1.0,
        "close_pos_pct"  : round(close_pos * 100, 1),
        "rsi14"          : round(float(rsi_s.iloc[-1]), 1),
        "adx14"          : round(float(adx_s.iloc[-1]), 1),
        "plus_di"        : round(float(pdi_s.iloc[-1]), 1),
        "minus_di"       : round(float(mdi_s.iloc[-1]), 1),
        "vwap20"         : round(vwap_val, 3),
        "vwap_up"        : vwap_slope > 0,
        "vwap_slope"     : round(vwap_slope, 4),
        "rs_vs_xjo"      : calc_rs(close, xjo) if xjo is not None else 1.0,
        "ma20"           : round(float(ma20.iloc[-1]), 3),
        "ma50"           : round(float(ma50.iloc[-1]), 3),
        "ma50_up"        : float(ma50.iloc[-1]) > float(ma50.iloc[-11]),
        "ma200"          : round(float(ma200.iloc[-1]), 3) if len(close) >= 200 else None,
        "atr14_pct"      : round(atr14 / lc * 100, 2),
        "w52_hi"         : round(w52_hi, 3),
        "w52_lo"         : round(w52_lo, 3),
        "dist_52w_hi_pct": round((lc / w52_hi - 1) * 100, 1),
        "max_dd_6m_pct"  : round(max_dd, 1),
        "price_pct_1y"   : price_pct_1y,
        "vol_consistency": vol_consistency,
        "price_events"   : price_events,
        "_close"  : close,
        "_high"   : high,
        "_low"    : low,
        "_volume" : volume,
        "_adx_s"  : adx_s,
        "_pdi_s"  : pdi_s,
        "_mdi_s"  : mdi_s,
    }

TIER_BONUS = {"T1": 0.15, "T2": 0.10, "T3": 0.05, "T4": 0.0}

def calc_composite_score(tech: dict) -> float:
    def norm(val, lo, hi):
        return max(0.0, min(1.0, (val - lo) / (hi - lo))) if hi > lo else 0.0

    scores = {
        "trend_strength": tech.get("trend_strength_score", 0.0),
        "persistence"   : tech.get("persistence_score", 0.0),
        "catalyst"      : tech.get("catalyst", 0.0),
        "price_pct_1y"  : norm(tech.get("price_pct_1y", 50), 50, 100),
    }
    base = sum(SCORE_WEIGHTS[k] * v for k, v in scores.items())
    bonus = TIER_BONUS.get(tech.get("tier_level", ""), 0.0)
    return round(base + bonus, 4)

def calc_confidence(tech: dict, tier_level: str) -> float:
    base_map  = {"T1": 0.85, "T2": 0.75, "T3": 0.65, "T4": 0.55}
    base      = base_map.get(tier_level, 0.60)
    adx       = tech.get("adx14", 20)
    rs        = tech.get("rs_vs_xjo", 1.0)
    adx_bonus = min(0.05, max(0.0, (adx - 25) / (40 - 25) * 0.05))
    rs_bonus  = min(0.05, max(0.0, (rs - 1.0) / 0.2 * 0.05))
    vol_bonus = 0.02 if tech.get("vol_consistency") else 0.0
    dist      = abs(tech.get("dist_52w_hi_pct", -20))
    dist_pen  = min(0.05, dist / 20 * 0.05)
    return round(min(0.92, max(0.50, base + adx_bonus + rs_bonus + vol_bonus - dist_pen)), 2)


def _check_volume_quality(volume_s: pd.Series) -> bool:
    if len(volume_s) < 20:
        return False

    vol_recent = volume_s.iloc[-10:]
    vol_prior  = volume_s.iloc[-20:-10]
    vol_last3  = volume_s.iloc[-3:]
    vol_last5  = volume_s.iloc[-5:]

    recent_mean = float(vol_recent.mean())
    prior_mean  = float(vol_prior.mean())

    if prior_mean <= 0 or recent_mean <= 0:
        return False

    ratio = recent_mean / prior_mean

    if ratio < 1.0:
        return True

    if ratio > 1.8:
        log.debug(f"_check_volume_quality: 量能增幅过大({ratio:.2f}x)，拒绝")
        return False

    max_single = float(vol_last3.max())
    if recent_mean > 0 and max_single > recent_mean * 3.0:
        log.debug(f"_check_volume_quality: 单日脉冲检测触发({max_single:.0f} > {recent_mean*3:.0f})，拒绝")
        return False

    vol5   = vol_last5.values.astype(float)
    x_vals = list(range(5))
    mean_x = 2.0
    mean_y = float(sum(vol5) / 5)

    if mean_y <= 0:
        return False

    numerator   = sum((x_vals[i] - mean_x) * (vol5[i] - mean_y) for i in range(5))
    denominator = sum((x_vals[i] - mean_x) ** 2 for i in range(5))

    if denominator == 0:
        return False

    slope     = numerator / denominator
    slope_pct = slope / mean_y

    if slope <= 0 or slope_pct <= 0.01:
        log.debug(f"_check_volume_quality: 量能方向性不足(slope_pct={slope_pct:.3f})，拒绝")
        return False

    return True


def _check_trend_persistence(close: pd.Series,
                              adx_s: pd.Series,
                              pdi_s: pd.Series,
                              mdi_s: pd.Series) -> float:
    score = 0.0

    try:
        adx_10 = adx_s.iloc[-10:].dropna()
        if len(adx_10) >= 5:
            adx_persistence = float((adx_10 > 20).sum()) / len(adx_10)
            score += adx_persistence * 0.40
    except Exception:
        pass

    try:
        pdi_10 = pdi_s.iloc[-10:].dropna()
        mdi_10 = mdi_s.iloc[-10:].dropna()
        min_len = min(len(pdi_10), len(mdi_10))
        if min_len >= 5:
            di_persistence = float(
                sum(1 for i in range(min_len)
                    if float(pdi_10.iloc[-(min_len - i)]) >
                       float(mdi_10.iloc[-(min_len - i)]))
            ) / min_len
            score += di_persistence * 0.40
    except Exception:
        pass

    try:
        ma50        = close.rolling(50).mean()
        ma50_recent = ma50.iloc[-20:].dropna()
        if len(ma50_recent) >= 10:
            vals   = ma50_recent.values.astype(float)
            n      = len(vals)
            mean_x = (n - 1) / 2.0
            mean_y = float(vals.mean())
            if mean_y > 0:
                num = sum((i - mean_x) * (vals[i] - mean_y) for i in range(n))
                den = sum((i - mean_x) ** 2 for i in range(n))
                if den > 0:
                    slope     = num / den
                    slope_pct = slope / mean_y
                    if slope > 0 and slope_pct > 0.0005:
                        score += 0.20
    except Exception:
        pass

    return round(min(1.0, score), 3)


def _check_higher_highs_lows(high: pd.Series,
                              low: pd.Series,
                              lookback: int = 40) -> bool:
    if len(high) < lookback or len(low) < lookback:
        return False

    mid = lookback // 2

    recent_high = high.iloc[-mid:]
    prior_high  = high.iloc[-lookback:-mid]
    recent_low  = low.iloc[-mid:]
    prior_low   = low.iloc[-lookback:-mid]

    higher_high = float(recent_high.max()) > float(prior_high.max())
    higher_low  = float(recent_low.min())  > float(prior_low.min())

    result = higher_high and higher_low
    if not result:
        log.debug(
            f"_check_higher_highs_lows: 结构不满足 "
            f"HH={higher_high}(recent_hi={recent_high.max():.3f} vs prior_hi={prior_high.max():.3f}) "
            f"HL={higher_low}(recent_lo={recent_low.min():.3f} vs prior_lo={prior_low.min():.3f})"
        )
    return result


def _check_ma_alignment(tech: dict, tier_level: str) -> bool:
    ma20  = tech.get("ma20",  0.0)
    ma50  = tech.get("ma50",  0.0)
    ma200 = tech.get("ma200")

    if ma20 <= ma50:
        log.debug(f"_check_ma_alignment: MA20({ma20:.3f}) <= MA50({ma50:.3f})，拒绝")
        return False

    if tier_level in ("T1", "T2") and ma200 is not None:
        if ma50 <= ma200:
            log.debug(f"_check_ma_alignment [{tier_level}]: MA50({ma50:.3f}) <= MA200({ma200:.3f})，拒绝")
            return False

    return True

def _norm(val: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


def calc_trend_strength_score(tech: dict, tier: dict) -> dict:
    lc     = tech["price"]
    w52_hi = tech["w52_hi"]

    scores = {}

    vol_anchor = tier["vol_mult"]
    scores["volume_multiple"] = _norm(
        tech.get("vol_ratio", 1.0), vol_anchor * 0.5, vol_anchor * 1.5
    )

    dist_ratio = lc / w52_hi if w52_hi > 0 else 0.7
    near_hi_anchor = 0.90 if tier["near_52w_hi"] else 0.75
    scores["near_52w_hi"] = _norm(dist_ratio, near_hi_anchor - 0.15, near_hi_anchor + 0.10)

    ma20 = tech.get("ma20", 0)
    ma50 = tech.get("ma50", 1)
    ma_premium = (ma20 / ma50 - 1) if ma50 > 0 else 0
    ma_anchor  = 0.02 if tier["level"] in ("T1", "T2") else 0.0
    scores["ma_alignment"] = _norm(ma_premium, ma_anchor - 0.03, ma_anchor + 0.05)

    rs_anchor = tier["rs_min"]
    scores["relative_strength"] = _norm(tech.get("rs_vs_xjo", 1.0), rs_anchor - 0.10, rs_anchor + 0.15)

    high_s = tech["_high"]
    low_s  = tech["_low"]
    hh_anchor = 0.02 if tier["level"] in ("T1", "T2") else -0.02
    if len(high_s) >= 40:
        recent_hi = float(high_s.iloc[-20:].max())
        prior_hi  = float(high_s.iloc[-40:-20].max())
        hh_ratio  = (recent_hi / prior_hi - 1) if prior_hi > 0 else 0
        scores["hh_hl_structure"] = _norm(hh_ratio, hh_anchor - 0.05, hh_anchor + 0.10)
    else:
        scores["hh_hl_structure"] = 0.0

    close_s = tech["_close"]
    slope_anchor = (tier["adx_min"] - 15) / 100
    if len(close_s) >= 61:
        ma50_now  = float(close_s.rolling(50).mean().iloc[-1])
        ma50_prev = float(close_s.rolling(50).mean().iloc[-11])
        ma50_chg  = (ma50_now / ma50_prev - 1) if ma50_prev > 0 else 0
        scores["ma50_trend"] = _norm(ma50_chg, slope_anchor - 0.02, slope_anchor + 0.05)
    else:
        scores["ma50_trend"] = 0.0

    vwap20 = tech.get("vwap20", lc)
    vwap_premium = (lc / vwap20 - 1) if vwap20 > 0 else 0
    scores["vwap_position"] = _norm(vwap_premium, -0.03, 0.05)

    weights = {
        "volume_multiple":   0.20,
        "near_52w_hi":       0.15,
        "ma_alignment":      0.15,
        "ma50_trend":        0.15,
        "hh_hl_structure":   0.15,
        "relative_strength": 0.10,
        "vwap_position":     0.10,
    }

    trend_strength_score = sum(scores[k] * weights[k] for k in weights)

    return {
        "trend_strength_score": round(trend_strength_score, 4),
        "sub_scores": {k: round(v, 3) for k, v in scores.items()},
    }

# ════════════════════════════════════════════════════════════
# signals.json生成 & GitHub推送
# ════════════════════════════════════════════════════════════

def _parse_gemini_json_fields(text: str) -> dict:
    patterns = {
        "tag_en":       r"【JSON_TAG_EN】(.+)",
        "tag_zh":       r"【JSON_TAG_ZH】(.+)",
        "one_liner_zh": r"【JSON_ONE_LINER_ZH】(.+)",
        "one_liner_en": r"【JSON_ONE_LINER_EN】(.+)",
    }
    result = {}
    for key, pat in patterns.items():
        m = re.search(pat, text)
        result[key] = m.group(1).strip() if m else ""
        if not result[key]:
            log.warning(f"_parse_gemini_json_fields: [{key}] 未找到")
    return result


def generate_signals_json(signals: list) -> bool:
    if not signals:
        log.info("generate_signals_json: 无信号，跳过")
        return False
    today_str  = date.today().isoformat()
    en_payload = {
        "date": today_str,
        "signals": [{"symbol": s["ticker"], "tag": s.get("_json_tag_en", ""),
                      "confidence": s.get("confidence", 0.60),
                      "one_liner": s.get("_json_one_liner_en", "")} for s in signals]
    }
    zh_payload = {
        "date": today_str,
        "signals": [{"symbol": s["ticker"], "tag": s.get("_json_tag_zh", ""),
                      "confidence": s.get("confidence", 0.60),
                      "one_liner": s.get("_json_one_liner_zh", "")} for s in signals]
    }
    try:
        os.makedirs(SIGNALS_DIR, exist_ok=True)
        for path, payload in [
            (os.path.join(SIGNALS_DIR, "en.json"), en_payload),
            (os.path.join(SIGNALS_DIR, "zh.json"), zh_payload),
        ]:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        log.info(f"signals.json写入成功：{len(signals)} 个信号")
        return True
    except Exception as e:
        log.error(f"generate_signals_json失败: {e}")
        return False


def push_to_github(files: list, commit_message: str) -> bool:
    if not files:
        log.info("push_to_github: 文件列表为空，跳过")
        return True
    try:
        r = subprocess.run(
            ["git", "-C", ASXBOX_REPO, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=10
        )
        branch = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else "main"
        log.info(f"push_to_github: branch={branch}，文件={files}")

        def _run_with_retry(cmd, label):
            result = None
            for attempt in range(1, NET_RETRY_MAX + 1):
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    if attempt > 1:
                        log.info(f"{label}: 第{attempt}次重试成功")
                    return result
                if "nothing to commit" in result.stdout + result.stderr:
                    return result
                if attempt < NET_RETRY_MAX:
                    wait = NET_RETRY_WAIT * attempt
                    log.warning(f"{label}: 第{attempt}次失败，{wait:.0f}s后重试...\n"
                                f"stdout:{result.stdout}\nstderr:{result.stderr}")
                    time.sleep(wait)
                else:
                    log.error(f"{label}: 达到{NET_RETRY_MAX}次重试上限\n"
                              f"stdout:{result.stdout}\nstderr:{result.stderr}")
            return result

        pull_result = _run_with_retry(
            ["git", "-C", ASXBOX_REPO, "pull", "--no-rebase", "origin", branch], "git pull"
        )
        if pull_result.returncode != 0 and "nothing to commit" not in (pull_result.stdout + pull_result.stderr):
            return False

        add_result = subprocess.run(
            ["git", "-C", ASXBOX_REPO, "add"] + files,
            capture_output=True, text=True, timeout=30
        )
        if add_result.returncode != 0:
            log.error(f"push_to_github: git add失败\nstdout:{add_result.stdout}\nstderr:{add_result.stderr}")
            return False
        log.info("git: add → OK")

        commit_result = subprocess.run(
            ["git", "-C", ASXBOX_REPO, "commit", "-m", commit_message],
            capture_output=True, text=True, timeout=30
        )
        if commit_result.returncode != 0:
            if "nothing to commit" in commit_result.stdout + commit_result.stderr:
                log.info("push_to_github: 无变更，跳过commit")
                return True
            log.error(f"push_to_github: git commit失败\n"
                      f"stdout:{commit_result.stdout}\nstderr:{commit_result.stderr}")
            return False
        log.info("git: commit → OK")

        push_result = _run_with_retry(
            ["git", "-C", ASXBOX_REPO, "push", "origin", branch], "git push"
        )
        if push_result.returncode != 0:
            return False
        log.info("git: push → OK")

        log.info(f"push_to_github: 推送成功 branch={branch}")
        return True
    except subprocess.TimeoutExpired:
        log.error("push_to_github: git超时")
        return False
    except Exception as e:
        log.error(f"push_to_github异常: {e}")
        return False

# ════════════════════════════════════════════════════════════
# 3. 数据获取
# ════════════════════════════════════════════════════════════

def _get(url: str, params: dict = None, label: str = "") -> Optional[dict]:
    for attempt in range(1, NET_RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, headers=ASX_HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            log.error(f"HTTP错误 [{label}] {url}: {e}")
            return None
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < NET_RETRY_MAX:
                wait = NET_RETRY_WAIT * attempt
                log.warning(f"网络异常 [{label}] 第{attempt}次: {e}，{wait:.0f}s后重试...")
                time.sleep(wait)
            else:
                log.error(f"网络异常 [{label}] 达到{NET_RETRY_MAX}次重试上限: {e}")
        except Exception as e:
            log.error(f"请求异常 [{label}] {url}: {e}")
            return None
    return None


def get_asx_universe() -> list:
    for attempt in range(1, NET_RETRY_MAX + 1):
        try:
            df  = pd.read_csv(
                "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
                skiprows=1, encoding="latin1",
            )
            col = next((c for c in df.columns if "code" in c.lower()), None)
            if not col:
                log.error("ASX列表列名未找到")
                return []
            codes  = df[col].dropna().astype(str).str.strip()
            valid  = codes[codes.str.match(r"^[A-Z]{1,5}$")]
            result = [f"{c}.AX" for c in valid]
            log.info(f"ASX股票池：{len(result)} 只")
            return result
        except Exception as e:
            if attempt < NET_RETRY_MAX:
                wait = NET_RETRY_WAIT * attempt
                log.warning(f"get_asx_universe第{attempt}次失败: {e}，{wait:.0f}s后重试...")
                time.sleep(wait)
            else:
                log.error(f"get_asx_universe达到{NET_RETRY_MAX}次重试上限: {e}")
    return []


def download_ohlcv(tickers: list, period: str = "1y",
                   batch_size: int = 50) -> dict:
    all_data  = {}
    n_batches = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch     = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        if batch_num % 5 == 0 or batch_num == 1:
            log.info(f"  下载 {batch_num}/{n_batches} 批...")
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period=period, interval="1d", progress=False)
                if not df.empty and len(df) >= 60:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(batch, period=period, interval="1d",
                                  progress=False, group_by="ticker")
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how="all")
                        if not tdf.empty and len(tdf) >= 60:
                            all_data[t] = tdf
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"批次下载失败，降级单只: {e}")
            for t in batch:
                try:
                    df = yf.download(t, period=period, interval="1d", progress=False)
                    if not df.empty and len(df) >= 60:
                        all_data[t] = df
                except Exception as e2:
                    log.debug(f"单只下载失败 [{t}]: {e2}")
        time.sleep(0.5)
    log.info(f"  K线完成：{len(all_data)}/{len(tickers)} 只有效")
    return all_data


def get_market_snapshot() -> dict:
    snap = {
        "date": date.today().isoformat(),
        "xjo_close": 0.0, "xjo_change_pct": 0.0,
        "market_status": "normal",
        "dev_from_ma50_pct": None,
        "sector_leaders": [],
        "xjo_series": None,
    }
    for attempt in range(1, NET_RETRY_MAX + 1):
        try:
            xjo = yf.download("^AXJO", period="1y", interval="1d", progress=False)
            if not xjo.empty and len(xjo) >= 50:
                close = xjo["Close"].squeeze()
                snap["xjo_series"]     = close
                snap["xjo_close"]      = round(float(close.iloc[-1]), 2)
                pct = (float(close.iloc[-1]) / float(close.iloc[-2]) - 1) * 100
                snap["xjo_change_pct"] = round(pct, 2)
                ma50 = close.rolling(50).mean()
                dev  = (float(close.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1])
                snap["dev_from_ma50_pct"] = round(dev * 100, 2)
                drop = (close.resample("W").last().pct_change().iloc[-2:] < -0.05).any()
                if dev < -0.03 or drop:
                    snap["market_status"] = "red"
                elif dev < 0:
                    snap["market_status"] = "yellow"
                elif pct > 1.0:
                    snap["market_status"] = "bullish"
            break
        except Exception as e:
            if attempt < NET_RETRY_MAX:
                wait = NET_RETRY_WAIT * attempt
                log.warning(f"大盘XJO第{attempt}次失败: {e}，{wait:.0f}s后重试...")
                time.sleep(wait)
            else:
                log.error(f"大盘XJO达到{NET_RETRY_MAX}次重试上限: {e}")

    sector_map = {
        "金融": "^AXFJ", "资源": "^AXMJ", "医疗": "^AXHJ",
        "科技": "^AXIJ", "能源": "^AXEJ", "消费": "^AXSJ",
    }
    changes = []
    for name, sym in sector_map.items():
        for attempt in range(1, NET_RETRY_MAX + 1):
            try:
                df_s = yf.download(sym, period="5d", interval="1d", progress=False)
                if not df_s.empty and len(df_s) >= 2:
                    c = df_s["Close"].squeeze()
                    changes.append((name, round((float(c.iloc[-1]) / float(c.iloc[-2]) - 1) * 100, 2)))
                break
            except Exception as e:
                if attempt < NET_RETRY_MAX:
                    time.sleep(NET_RETRY_WAIT * attempt)
                else:
                    log.warning(f"板块数据失败 [{name}] 达到重试上限: {e}")
        time.sleep(0.1)
    snap["sector_leaders"] = sorted(changes, key=lambda x: x[1], reverse=True)[:3]
    log.info(f"大盘: XJO {snap['xjo_change_pct']:+.2f}% 状态:{snap['market_status']} "
             f"偏离MA50:{snap['dev_from_ma50_pct']}%")
    return snap


def get_top_movers(all_data: dict, top_n: int = TOP_N) -> list:
    changes = {}
    for ticker, df in all_data.items():
        try:
            close = df["Close"].squeeze()
            vol   = df["Volume"].squeeze()
            if len(close) < 2:
                continue
            lc, prev, lv = float(close.iloc[-1]), float(close.iloc[-2]), float(vol.iloc[-1])
            if lv * lc < 500_000:
                continue
            changes[ticker] = (lc / prev - 1) * 100
        except Exception:
            pass
    if not changes:
        log.warning("get_top_movers：无有效数据")
        return []
    top = sorted(changes.items(), key=lambda x: x[1], reverse=True)[:top_n]
    log.info(f"Top {top_n} Movers: {[(t, f'{c:.1f}%') for t, c in top]}")
    return [t for t, _ in top]

def fetch_fundamentals(ticker: str, retries: int = 3) -> dict:
    for attempt in range(1, retries + 1):
        try:
            info = yf.Ticker(ticker).info
            market_cap = info.get("marketCap", 0)

            if not market_cap:
                if attempt < retries:
                    log.warning(f"fetch_fundamentals [{ticker}] 第{attempt}次: "
                               f"marketCap为空，重试")
                    time.sleep(1.5 * attempt)
                    continue
                else:
                    log.error(f"fetch_fundamentals [{ticker}] 达到{retries}次仍无有效市值")
                    break

            return {
                "company_name": info.get("longName", ticker),
                "sector":       info.get("sector", "未知"),
                "industry":     info.get("industry", "未知"),
                "market_cap_m": round(market_cap / 1_000_000, 1),
            }
        except Exception as e:
            if attempt < retries:
                log.warning(f"fetch_fundamentals失败 [{ticker}] 第{attempt}次: {e}，重试")
                time.sleep(1.5 * attempt)
            else:
                log.error(f"fetch_fundamentals最终失败 [{ticker}]: {e}")

    return {"company_name": ticker, "sector": "未知",
            "industry": "未知", "market_cap_m": 0.0}


def _ann_significance(headline: str, sensitive: bool,
                      doc_type: str, pdf_text: str, pub_date: str) -> int:
    score = 0
    if sensitive:
        score += 4
    if any(w.lower() in doc_type.lower() for w in ANN_WHITELIST):
        score += 3
    if pdf_text:
        score += 2
    if pub_date >= (date.today() - timedelta(days=7)).isoformat():
        score += 1
    return score


def _is_noise_announcement(doc_type: str, headline: str) -> bool:
    combined = (doc_type + " " + headline).lower()
    return any(kw in combined for kw in ANN_NOISE_KEYWORDS)


ANN_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "announcements.db")
_today_ann_cache: dict = {}


def _init_ann_db() -> None:
    """初始化SQLite（announcements + signals_history 两张表）"""
    import sqlite3
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS announcements (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol       TEXT    NOT NULL,
                    date         TEXT    NOT NULL,
                    headline     TEXT,
                    sensitive    INTEGER DEFAULT 0,
                    doc_type     TEXT,
                    doc_key      TEXT,
                    pdf_text     TEXT,
                    significance INTEGER DEFAULT 0,
                    UNIQUE(symbol, date, headline)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_date "
                "ON announcements(symbol, date)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals_history (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker          TEXT    NOT NULL,
                    signal_date     TEXT    NOT NULL,
                    tier_level      TEXT,
                    composite_score REAL,
                    catalyst        REAL,
                    has_today_ann   INTEGER DEFAULT 0,
                    ann_sensitive   INTEGER DEFAULT 0,
                    rs_vs_xjo       REAL,
                    adx14           REAL,
                    vol_consistency INTEGER DEFAULT 0,
                    price_pct_1y    REAL,
                    dist_52w_hi_pct REAL,
                    market_status   TEXT,
                    xjo_change_pct  REAL,
                    sector          TEXT,
                    entry_price     REAL,
                    stop_loss_atr   REAL,
                    take_profit_atr REAL,
                    is_selected     INTEGER DEFAULT 0,
                    outcome         TEXT    DEFAULT 'PENDING',
                    outcome_date    TEXT,
                    outcome_price   REAL,
                    outcome_pct     REAL,
                    holding_days    INTEGER,
                    max_gain_pct    REAL,
                    max_loss_pct    REAL,
                    UNIQUE(ticker, signal_date)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sh_date "
                "ON signals_history(signal_date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sh_outcome "
                "ON signals_history(outcome)"
            )
            conn.commit()
        log.info(f"公告数据库就绪：{ANN_DB_PATH}")
    except Exception as e:
        log.error(f"公告数据库初始化失败: {e}")


def _save_announcements_to_db(ann_dict: dict) -> None:
    import sqlite3
    if not ann_dict:
        return
    today = date.today().isoformat()
    rows  = []
    for sym, a in ann_dict.items():
        rows.append((
            sym, today,
            a.get("headline", ""),
            1 if a.get("sensitive") else 0,
            a.get("doc_type", ""),
            a.get("documentKey", ""),
            a.get("pdf_text", ""),
            a.get("significance", 0),
        ))
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO announcements
                    (symbol, date, headline, sensitive, doc_type, doc_key, pdf_text, significance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
        log.info(f"公告写入DB：{len(rows)} 条")
    except Exception as e:
        log.error(f"公告写入DB失败: {e}")


def _load_announcements_from_db(code: str, days: int = 180) -> list:
    import sqlite3
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT date, headline, sensitive, doc_type, pdf_text, significance
                FROM   announcements
                WHERE  symbol = ? AND date >= ?
                ORDER  BY significance DESC, date DESC
                LIMIT  20
            """, (code, cutoff)).fetchall()
        result = [
            {"date": r[0], "headline": r[1], "sensitive": bool(r[2]),
             "doc_type": r[3], "pdf_text": r[4] or "", "significance": r[5]}
            for r in rows
        ]
        log.info(f"公告DB [{code}]: {len(result)} 条历史记录（近{days}天）")
        return result
    except Exception as e:
        log.error(f"公告DB读取失败 [{code}]: {e}")
        return []


def save_signal_to_history(signal: dict, market_snap: dict,
                           is_selected: bool) -> None:
    import sqlite3
    try:
        lc        = signal.get("price", 0)
        atr       = lc * signal.get("atr14_pct", 2.0) / 100
        sl        = round(lc - BT_STOP_ATR_MULT * atr, 4)
        tp        = round(lc + BT_TARGET_ATR_MULT * atr, 4)
        code      = signal.get("ticker", "").replace(".AX", "")
        today_ann = _today_ann_cache.get(date.today().isoformat(), {})
        ann       = today_ann.get(code, {})

        with sqlite3.connect(ANN_DB_PATH) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO signals_history (
                    ticker, signal_date, tier_level, composite_score,
                    catalyst, has_today_ann, ann_sensitive,
                    rs_vs_xjo, adx14, vol_consistency,
                    price_pct_1y, dist_52w_hi_pct,
                    market_status, xjo_change_pct, sector,
                    entry_price, stop_loss_atr, take_profit_atr,
                    is_selected
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                signal.get("ticker"),
                date.today().isoformat(),
                signal.get("tier_level", ""),
                signal.get("composite_score", 0),
                signal.get("catalyst", 0),
                1 if ann else 0,
                1 if ann.get("sensitive") else 0,
                signal.get("rs_vs_xjo", 0),
                signal.get("adx14", 0),
                1 if signal.get("vol_consistency") else 0,
                signal.get("price_pct_1y", 0),
                signal.get("dist_52w_hi_pct", 0),
                market_snap.get("market_status", ""),
                market_snap.get("xjo_change_pct", 0),
                signal.get("sector", ""),
                lc, sl, tp,
                1 if is_selected else 0,
            ))
            conn.commit()
    except Exception as e:
        log.error(f"save_signal_to_history失败 [{signal.get('ticker')}]: {e}")


def update_signal_outcomes(all_data: dict) -> None:
    import sqlite3
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            pending = conn.execute("""
                SELECT id, ticker, signal_date, entry_price,
                       stop_loss_atr, take_profit_atr
                FROM   signals_history
                WHERE  outcome = 'PENDING'
            """).fetchall()
    except Exception as e:
        log.error(f"update_signal_outcomes读取失败: {e}")
        return

    if not pending:
        log.info("update_signal_outcomes: 无PENDING记录")
        return

    today_str = date.today().isoformat()
    updates   = []

    for row in pending:
        sid, ticker, sig_date, entry, sl, tp = row
        df = all_data.get(ticker)
        if df is None:
            continue
        try:
            close = df["Close"].squeeze()
            high  = df["High"].squeeze()
            low   = df["Low"].squeeze()

            idx       = close.index.astype(str).str[:10]
            close_idx = pd.Series(close.values, index=idx)
            high_idx  = pd.Series(high.values,  index=idx)
            low_idx   = pd.Series(low.values,   index=idx)

            after_close = close_idx[close_idx.index > sig_date]
            after_high  = high_idx[high_idx.index   > sig_date]
            after_low   = low_idx[low_idx.index     > sig_date]

            if after_close.empty:
                continue

            holding  = len(after_close)
            max_gain = round((float(after_high.max()) / entry - 1) * 100, 2)
            max_loss = round((float(after_low.min())  / entry - 1) * 100, 2)
            latest   = float(after_close.iloc[-1])

            outcome   = None
            out_date  = None
            out_price = None

            for dt in after_close.index:
                l = float(after_low.get(dt,  entry))
                h = float(after_high.get(dt, entry))
                if l <= sl:
                    outcome, out_date, out_price = "LOSS", dt, sl
                    break
                if h >= tp:
                    outcome, out_date, out_price = "WIN",  dt, tp
                    break

            if outcome is None:
                if holding >= BT_TIMEOUT_DAYS:
                    outcome, out_date, out_price = "TIMEOUT", today_str, latest

            if outcome:
                out_pct = round((out_price / entry - 1) * 100, 2)
                updates.append((
                    outcome, out_date, out_price, out_pct,
                    holding, max_gain, max_loss, sid
                ))
        except Exception as e:
            log.debug(f"update_signal_outcomes处理失败 [{ticker}]: {e}")

    if updates:
        try:
            with sqlite3.connect(ANN_DB_PATH) as conn:
                conn.executemany("""
                    UPDATE signals_history
                    SET outcome=?, outcome_date=?, outcome_price=?,
                        outcome_pct=?, holding_days=?,
                        max_gain_pct=?, max_loss_pct=?
                    WHERE id=?
                """, updates)
                conn.commit()
            log.info(f"update_signal_outcomes: 更新 {len(updates)} 条结果")
        except Exception as e:
            log.error(f"update_signal_outcomes写入失败: {e}")


def fetch_today_announcements() -> dict:
    global _today_ann_cache
    today = date.today().isoformat()
    if today in _today_ann_cache:
        log.info(f"今日公告（进程缓存）：{len(_today_ann_cache[today])} 只")
        return _today_ann_cache[today]

    _init_ann_db()
    result, page = {}, 0
    pdf_done = set()

    while True:
        data = _get(ASX_ANN_ALL,
                    params={"itemsPerPage": 100, "page": page},
                    label="今日公告")
        if not data:
            break
        items = data.get("data", {}).get("items", [])
        if not items:
            break
        got_old = False
        for item in items:
            if item.get("date", "")[:10] < today:
                got_old = True
                break
            sym      = item.get("symbol", "")
            headline = item.get("headline", "")[:80]
            doc_type = item.get("documentType", "")
            is_sens  = item.get("isPriceSensitive", False)
            doc_key  = item.get("documentKey", "")
            pdf_txt  = ""
            if not sym:
                continue
            if _is_noise_announcement(doc_type, headline):
                continue
            if is_sens and doc_key and sym not in pdf_done:
                pdf_url = PDF_DL_BASE.format(doc_key=doc_key)
                log.info(f"  PDF提取 [{sym}]: {headline[:40]}...")
                pdf_txt = _extract_pdf_keywords(pdf_url)
                if pdf_txt:
                    pdf_done.add(sym)
                time.sleep(0.2)
            if sym not in result:
                sig = _ann_significance(headline, is_sens, doc_type, pdf_txt, today)
                result[sym] = {
                    "headline": headline, "sensitive": is_sens,
                    "doc_type": doc_type, "documentKey": doc_key,
                    "pdf_text": pdf_txt, "significance": sig,
                }
        if got_old or len(items) < 100:
            break
        page += 1
        time.sleep(0.3)

    _save_announcements_to_db(result)
    _today_ann_cache[today] = result
    log.info(f"今日公告：{len(result)} 只（有效），PDF {len(pdf_done)} 份")
    return result


def fetch_announcements(code: str, today_ann: Optional[dict] = None) -> list:
    history = _load_announcements_from_db(code, days=180)
    if today_ann and code in today_ann:
        today_str = date.today().isoformat()
        already   = any(a["date"] == today_str for a in history)
        if not already:
            ann = today_ann[code]
            history.insert(0, {
                "date": today_str, "headline": ann["headline"],
                "sensitive": ann["sensitive"], "doc_type": ann.get("doc_type", ""),
                "pdf_text": ann.get("pdf_text", ""), "significance": ann.get("significance", 0),
            })
            log.info(f"公告 [{code}]: 今日公告从内存补充（DB未命中）")
    return history


def _extract_pdf_keywords(url: str) -> str:
    for attempt in range(1, NET_RETRY_MAX + 1):
        try:
            resp = requests.get(url, timeout=TIMEOUT, headers=ASX_HEADERS, stream=True)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "pdf" not in ct.lower() and not url.lower().endswith(".pdf"):
                log.warning(f"_extract_pdf_keywords: 非PDF [{url[:60]}] CT:{ct}")
                return ""
            pages_text = []
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                for page in pdf.pages[:12]:
                    t = page.extract_text()
                    if t:
                        pages_text.append(t)
            full_text  = "\n".join(pages_text)
            paragraphs = re.split(r"\n{2,}", full_text)
            key_paras  = []
            for para in paragraphs:
                hits = sum(1 for kw in PDF_KEY_TERMS if kw in para.lower())
                if hits >= 1 and len(para.strip()) > 30:
                    key_paras.append((hits, para.strip()))
            key_paras.sort(key=lambda x: x[0], reverse=True)
            extracted = re.sub(r"[ \t]+", " ",
                               "\n\n".join(p for _, p in key_paras[:8])).strip()
            if len(extracted) > PDF_MAX_CHARS:
                extracted = extracted[:PDF_MAX_CHARS] + "\n...[截断]"
            if not extracted:
                log.debug(f"PDF无关键词命中，返回前500字符 [{url[:60]}]")
                return full_text[:500]
            log.debug(f"PDF提取成功 [{url[:60]}]: {len(extracted)} 字符")
            return extracted
        except requests.HTTPError as e:
            log.error(f"PDF下载HTTP错误 [{url[:60]}]: {e}")
            return ""
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < NET_RETRY_MAX:
                wait = NET_RETRY_WAIT * attempt
                log.warning(f"PDF下载网络异常 [{url[:60]}] 第{attempt}次: {e}，{wait:.0f}s后重试...")
                time.sleep(wait)
            else:
                log.error(f"PDF下载网络异常 [{url[:60]}] 达到{NET_RETRY_MAX}次重试上限: {e}")
        except Exception as e:
            log.error(f"PDF提取失败 [{url[:60]}]: {e}")
            return ""
    return ""


def fetch_news(ticker: str, company_name: str = "") -> list:
    code   = ticker.replace(".AX", "")
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    raw    = []
    for q in [f"ASX:{code}",
               f"{company_name} ASX" if company_name else f"{code} ASX Australia"]:
        for attempt in range(1, NET_RETRY_MAX + 1):
            try:
                url  = GOOGLE_RSS.format(q=requests.utils.quote(q))
                resp = requests.get(url, timeout=TIMEOUT,
                                    headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:8]:
                    title = item.findtext("title", "").strip()
                    pub   = item.findtext("pubDate", "")
                    link  = item.findtext("link", "")
                    src   = item.findtext("source", "Google News")
                    try:
                        pub_date = parsedate_to_datetime(pub).strftime("%Y-%m-%d")
                    except Exception:
                        pub_date = date.today().isoformat()
                    if title and pub_date >= cutoff:
                        raw.append({"title": title[:100], "date": pub_date,
                                    "source": str(src)[:40], "url": link})
                break
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < NET_RETRY_MAX:
                    time.sleep(NET_RETRY_WAIT * attempt)
                else:
                    log.error(f"Google RSS 网络异常达到重试上限 [{q}]: {e}")
            except requests.HTTPError as e:
                log.error(f"Google RSS HTTP错误 [{q}]: {e}")
                break
            except ET.ParseError as e:
                log.error(f"Google RSS XML解析错误 [{q}]: {e}")
                break
            except Exception as e:
                log.error(f"Google RSS 未知错误 [{q}]: {e}")
                break
        time.sleep(0.4)
    try:
        for n in (yf.Ticker(ticker).news or [])[:10]:
            content = n.get("content", {})
            title   = content.get("title", "")
            pub     = content.get("pubDate", "")[:10]
            if title and pub >= cutoff:
                raw.append({
                    "title":  title[:100], "date": pub,
                    "source": content.get("provider", {}).get("displayName", "Yahoo"),
                    "url":    content.get("canonicalUrl", {}).get("url", ""),
                })
    except Exception as e:
        log.error(f"yfinance新闻失败 [{ticker}]: {e}")
    seen, deduped = set(), []
    for n in sorted(raw, key=lambda x: x["date"], reverse=True):
        key = n["title"][:40].lower()
        if key not in seen:
            seen.add(key)
            deduped.append(n)
    result = deduped[:NEWS_MAX]
    log.info(f"新闻 [{ticker}]: {len(result)} 条（原始{len(raw)}条）")
    return result


def build_timeline_text(code: str, announcements: list, news: list,
                        today_ann: Optional[dict] = None) -> str:
    events = []
    for a in announcements:
        sig       = a.get("significance", 0)
        sens_tag  = "[Price-sensitive]" if a["sensitive"] else "[Routine]"
        sig_label = f"[Significance:{sig}/10]" if sig >= 5 else ""
        line      = f"{sens_tag}{sig_label}[Announcement] {a['headline']}"
        pdf_txt   = a.get("pdf_text", "")
        if pdf_txt:
            line += f"\n    [PDF excerpt]: {pdf_txt[:400]}"
        events.append({"date": a["date"], "text": line,
                        "sort_key": (a["date"], sig)})
    for n in news:
        events.append({"date": n["date"],
                        "text": f"[News] {n['title']} (source: {n['source']})",
                        "sort_key": (n["date"], 0)})
    if today_ann and code in today_ann:
        ta       = today_ann[code]
        sens_tag = "[Price-sensitive]" if ta["sensitive"] else "[Routine]"
        events.append({"date": date.today().isoformat(),
                        "text": f"{sens_tag}[Today's announcement] {ta['headline']}",
                        "sort_key": (date.today().isoformat(), 10)})
    seen, lines = set(), []
    for e in sorted(events, key=lambda x: x["sort_key"], reverse=True):
        key = e["date"] + e["text"][:50]
        if key not in seen:
            seen.add(key)
            lines.append(f"{e['date']}  {e['text']}")
    return "\n".join(lines[:20]) if lines else "No announcements or news found in the lookback window."


def _extract_frontmatter_title(md_content: str) -> str:
    m = re.search(r'title:\s*"([^"]*)"', md_content)
    return m.group(1) if m else ""


def _sanitize_slug(raw: str) -> str:
    s = raw.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:60]


# ════════════════════════════════════════════════════════════
# 4. Gemini
# ════════════════════════════════════════════════════════════

def _is_retryable_error(err_text: str, exc: Exception) -> bool:
    rate_limit_markers = ("429", "503", "RESOURCE_EXHAUSTED", "overloaded", "quota")
    if any(k in err_text for k in rate_limit_markers):
        return True
    network_markers = ("timeout", "timed out", "connection", "temporarily unavailable",
                       "reset by peer", "deadline exceeded")
    if any(k in err_text.lower() for k in network_markers):
        return True
    exc_type_name = type(exc).__name__.lower()
    if any(k in exc_type_name for k in ("timeout", "connection")):
        return True
    return False


def ask_gemini(prompt: str, label: str = "", config: Optional[dict] = None,
               return_meta: bool = False):
    empty_result = ("", "", 0, 0) if return_meta else ""
    if not gemini_client:
        log.warning("Gemini未配置")
        return empty_result

    cfg = config if config is not None else GEMINI_CFG_DEEP
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL, contents=prompt, config=cfg
            )
            if attempt > 1:
                log.info(f"Gemini成功 [{label}] 第{attempt}次")

            try:
                text = resp.text.strip() if resp.text else ""
            except Exception as text_e:
                log.warning(f"ask_gemini: resp.text提取失败 [{label}]: {text_e}")
                text = ""

            if not return_meta:
                return text

            finish_reason, thoughts_tokens, output_tokens = "", 0, 0
            try:
                if resp.candidates:
                    finish_reason = str(getattr(resp.candidates[0], "finish_reason", "") or "")
                usage = getattr(resp, "usage_metadata", None)
                if usage:
                    thoughts_tokens = getattr(usage, "thoughts_token_count", 0) or 0
                    output_tokens   = getattr(usage, "candidates_token_count", 0) or 0
            except Exception as meta_e:
                log.debug(f"ask_gemini: 提取finish_reason/usage_metadata失败 [{label}]: {meta_e}")

            if finish_reason and "MAX_TOKENS" in finish_reason.upper():
                log.warning(
                    f"ask_gemini [{label}]: finish_reason=MAX_TOKENS "
                    f"(thinking={thoughts_tokens} output={output_tokens} "
                    f"limit={cfg.get('max_output_tokens', 'N/A')})"
                )
            return text, finish_reason, thoughts_tokens, output_tokens

        except Exception as e:
            err = str(e)
            if _is_retryable_error(err, e):
                if attempt < RETRY_MAX:
                    log.warning(f"Gemini可重试错误 [{label}] {attempt}/{RETRY_MAX}: {err[:200]}，{RETRY_WAIT}s后重试...")
                    time.sleep(RETRY_WAIT)
                else:
                    log.error(f"Gemini [{label}] 达到{RETRY_MAX}次重试上限，放弃: {err[:200]}")
                    return empty_result
            else:
                log.error(f"Gemini不可重试错误 [{label}]: {err}")
                return empty_result
    return empty_result

# ════════════════════════════════════════════════════════════
# 5. Telegram
# ════════════════════════════════════════════════════════════

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置")
        return
    url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        for attempt in range(1, NET_RETRY_MAX + 1):
            try:
                r = requests.post(url, json={
                    "chat_id": CHAT_ID, "text": chunk,
                    "parse_mode": "HTML", "disable_web_page_preview": True,
                }, timeout=10)
                r.raise_for_status()
                break
            except requests.HTTPError as e:
                log.error(f"Telegram HTTP错误（不重试，需人工检查token/chat_id）: {e}")
                break
            except Exception as e:
                if attempt < NET_RETRY_MAX:
                    wait = NET_RETRY_WAIT * attempt
                    log.warning(f"Telegram发送失败 第{attempt}次: {e}，{wait:.0f}s后重试...")
                    time.sleep(wait)
                else:
                    log.error(f"Telegram发送失败，达到{NET_RETRY_MAX}次重试上限: {e}")
        time.sleep(0.5)


def send_document(filename: str, content: str, caption: str = "") -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过send_document")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    for attempt in range(1, NET_RETRY_MAX + 1):
        try:
            r = requests.post(
                url,
                data={"chat_id": CHAT_ID, "caption": caption},
                files={"document": (filename, content.encode("utf-8"), "text/plain")},
                timeout=30,
            )
            r.raise_for_status()
            log.info(f"文件发送成功: {filename} ({len(content)} 字符)")
            return
        except requests.HTTPError as e:
            log.error(f"send_document HTTP错误（不重试）[{filename}]: {e}")
            return
        except Exception as e:
            if attempt < NET_RETRY_MAX:
                wait = NET_RETRY_WAIT * attempt
                log.warning(f"send_document失败 第{attempt}次 [{filename}]: {e}，{wait:.0f}s后重试...")
                time.sleep(wait)
            else:
                log.error(f"send_document失败，达到{NET_RETRY_MAX}次重试上限 [{filename}]: {e}")

# ════════════════════════════════════════════════════════════
# 6. Prompt构建
# ════════════════════════════════════════════════════════════

def _build_screener_prompt(signal: dict, timeline: str, tier_label: str) -> str:
    t = signal
    ma200_line = (f"MA200: {t['ma200']}" if t.get("ma200")
                 else "MA200: N/A (fewer than 200 trading days of price history available)")
    pct_1y_line = f"Current close is at the {t.get('price_pct_1y', 50)}th percentile of trailing 1-year daily closes"
    pe          = t.get("price_events", [])
    pe_str      = "\n".join(
        f"  {e['date']}  single-session move: {'+' if e['change_pct'] > 0 else ''}{e['change_pct']}%"
        for e in pe[:6]
    ) if pe else "  No single-session move of +/-5% or greater in the trailing ~6 months"

    tier_bonus_val = TIER_BONUS.get(t.get("tier_level", ""), 0.0)

    tech_block = (
        f"Price: {t['price']} (daily change: {t['change_pct']:+.2f}%)\n"
        f"{pct_1y_line}\n"
        f"5-day volume sequence monotonically non-decreasing: {t.get('vol_consistency')} "
        f"(definition: true if each of the last 5 daily volumes is >= the prior day's volume)\n"
        f"MA20: {t['ma20']} | MA50: {t['ma50']} | {ma200_line}\n"
        f"RSI(14): {t['rsi14']} | ADX(14): {t['adx14']} | +DI: {t['plus_di']} | -DI: {t['minus_di']}\n"
        f"VWAP(20): {t['vwap20']} | VWAP 5-session slope: {t.get('vwap_slope', 'N/A')} "
        f"(raw slope value; positive = VWAP has risen over the last 5 sessions)\n"
        f"Volume ratio (today's volume / 20-day average volume): {t['vol_ratio']}x\n"
        f"Relative strength vs XJO index, 20-day (ratio of returns): {t['rs_vs_xjo']} "
        f"(1.0 = matched the index's return; >1.0 = outperformed; <1.0 = underperformed)\n"
        f"ATR(14) as % of price: {t['atr14_pct']}%\n"
        f"52-week high: {t['w52_hi']} (current price is {t['dist_52w_hi_pct']}% from this high) | "
        f"52-week low: {t['w52_lo']}\n"
        f"Max drawdown, trailing 6 months: {t['max_dd_6m_pct']}%\n"
        f"Trend strength score: {t.get('trend_strength_score', 'N/A')} "
        f"(0-1 scale, weighted composite of 7 technical sub-factors; this tier's [{tier_label}] "
        f"pass threshold on this metric alone is {TREND_SCORE_THRESHOLD.get(t.get('tier_level',''), 'N/A')})\n"
        f"Trend persistence score: {t.get('persistence_score', 0.0)} "
        f"(0-1 scale, measures how long the current trend has been sustained over the trailing "
        f"10-20 sessions; this is a duration measure, not a strength measure)\n"
        f"Composite score: {t.get('composite_score', 'N/A')} "
        f"(includes a fixed +{tier_bonus_val} bonus for tier {tier_label}; composite scores are "
        f"not directly comparable across different tiers)"
    )

    return f"""你是一位专注ASX市场的资深机构分析师。今天是{date.today().isoformat()}。

===== 分析标的 =====
{t['ticker']} | 筛选等级:{tier_label} | 综合评分:{t.get('composite_score','N/A')}
{t.get('company_name','Unknown')} (Sector: {t.get('sector','Unknown')} / Industry: {t.get('industry','Unknown')}) Market cap: {t.get('market_cap_m',0)}M AUD

===== TECHNICAL DATA (raw, 1-year lookback) =====
{tech_block}

===== SINGLE-SESSION MOVES OF +/-5% OR GREATER, TRAILING ~6 MONTHS =====
{pe_str}

===== NEWS & ANNOUNCEMENTS TIMELINE (noise-filtered, sorted by significance) =====
{timeline}

===== 分析任务 =====
请严格按以下4部分输出，每部分2-3句，语言精炼专业：

【技术形态】结合趋势强度评分和量能连续性，评估当前突破质量和支撑压力位。

【事件驱动分析】对照价格波动节点和时间线，找出最重要的1-2个催化剂事件，
判断市场是否已充分定价。

【催化剂预测】基于公告周期（季报/年报/项目进展规律），
预测未来4-8周最可能的催化剂类型和时间窗口。

【综合结论】给出买入/观望/回避建议，说明止损位（基于ATR或关键支撑），
以及最值得关注的一个上行/下行风险。

规则：不确定内容标注"需进一步核查"，禁止编造数据。趋势强度评分是本次分析的
核心技术依据，请优先参考该指标而非其他辅助字段。

===== 固定输出字段（必须在分析末尾严格按格式输出，不得省略）=====
【JSON_TAG_EN】（英文信号标签，2-4 words，如：Bullish Momentum / Range Break Setup / Overbought Pressure）
【JSON_TAG_ZH】（中文信号标签，2-4个字，如：强势突破 / 区间试探 / 超买压力）
【JSON_ONE_LINER_ZH】（一句中文核心解释，≤25字，描述当前技术或事件驱动的关键状态）
【JSON_ONE_LINER_EN】（One English sentence, ≤20 words, same meaning as ZH above）"""


def _build_report_stock_block(ticker: str, tech: dict, fund: dict,
                               timeline: str, pdf_texts: list, rank: int) -> str:
    ma200_line = (f"MA200: {tech['ma200']}" if tech.get("ma200")
                 else "MA200: N/A (fewer than 200 trading days of price history)")
    pct_1y_line = f"Current close is at the {tech.get('price_pct_1y', 50)}th percentile of trailing 1-year daily closes"
    pe          = tech.get("price_events", [])
    pe_str      = " | ".join(
        f"{e['date']}:{'+' if e['change_pct'] > 0 else ''}{e['change_pct']}%"
        for e in pe[:5]
    ) if pe else "No single-session move of +/-5% or greater in the trailing ~6 months"

    trend_score = tech.get("trend_strength_score")
    trend_score_str = (
        f"{trend_score} (0-1 scale, weighted composite of 7 technical sub-factors)"
        if trend_score is not None
        else "N/A (this stock was sourced from the top-movers-by-% fallback list, not the tier screener; see degraded-mode note below)"
    )

    composite  = tech.get("composite_score")
    tier_level = tech.get("tier_level", "")
    if composite is None:
        composite_str = "N/A"
        bonus_note = ("(Degraded mode: this data came from a red-market day or a day when no tier "
                     "candidates passed, not from the tier screener; there is no valid composite score.)")
    else:
        composite_str  = f"{composite}"
        tier_bonus_val = TIER_BONUS.get(tier_level, 0.0)
        bonus_note = (f"(Includes a fixed +{tier_bonus_val} bonus for tier {tier_level or 'N/A'}; "
                     f"composite scores are not directly comparable across different tiers.)")

    tech_line = (
        f"Price: {tech['price']} (daily change: {tech['change_pct']:+.2f}%)\n"
        f"{pct_1y_line}\n"
        f"5-day volume sequence monotonically non-decreasing: {tech.get('vol_consistency')} "
        f"(definition: true if each of the last 5 daily volumes is >= the prior day's volume)\n"
        f"MA50: {tech['ma50']} | {ma200_line}\n"
        f"RSI(14): {tech['rsi14']} | ADX(14): {tech['adx14']}\n"
        f"Volume ratio (today's volume / 20-day average volume): {tech['vol_ratio']}x\n"
        f"Relative strength vs XJO index, 20-day (ratio of returns): {tech['rs_vs_xjo']} "
        f"(1.0 = matched the index's return; >1.0 = outperformed)\n"
        f"ATR(14) as % of price: {tech['atr14_pct']}%\n"
        f"52-week high: {tech['w52_hi']} (current price is {tech['dist_52w_hi_pct']}% from this high)\n"
        f"Max drawdown, trailing 6 months: {tech['max_dd_6m_pct']}%\n"
        f"Trend strength score: {trend_score_str}\n"
        f"Trend persistence score: {tech.get('persistence_score', 0.0)} "
        f"(0-1 scale, measures how long the current trend has been sustained over the trailing "
        f"10-20 sessions; this is a duration measure, not a strength measure)\n"
        f"Composite score: {composite_str} {bonus_note}\n"
        f"Single-session moves of +/-5% or greater, trailing ~6 months: {pe_str}"
    )

    block = (
        f"\n{'='*50}\n"
        f"#{rank} {ticker} | {fund.get('company_name', ticker)}\n"
        f"Sector: {fund.get('sector','Unknown')} | Market cap: {fund.get('market_cap_m',0)}M AUD\n"
        f"{'='*50}\n"
        f"[TECHNICAL DATA]\n{tech_line}\n\n"
        f"[NEWS & ANNOUNCEMENTS TIMELINE (noise-filtered, sorted by significance)]\n{timeline}"
    )
    for i, txt in enumerate(pdf_texts[:PDF_MAX_PER_STOCK], 1):
        if len(txt) > 400:
            block += f"\n\n[Full text excerpt of price-sensitive announcement #{i}, for citation]\n{txt}"
    return block


def _build_seo_article_prompt(market_snap: dict, stock_package: dict) -> str:
    dev_ma50 = market_snap.get("dev_from_ma50_pct")
    dev_str  = f"{dev_ma50}%" if dev_ma50 is not None else "N/A"
    sector_str = "、".join(
        f"{s}({p:+.1f}%)" for s, p in market_snap.get("sector_leaders", [])
    ) or "No data available"
    market_block = (
        f"Date: {market_snap.get('date', date.today().isoformat())}\n"
        f"ASX200: {market_snap.get('xjo_close',0)} "
        f"(daily change: {market_snap.get('xjo_change_pct',0):+.2f}%)\n"
        f"Market status classification: {market_snap.get('market_status','normal')} "
        f"(internal categorical label; underlying data: ASX200 deviation from its 50-day moving "
        f"average is {dev_str})\n"
        f"Sector leaders today: {sector_str}"
    )

    ticker = stock_package["ticker"]
    data_block = stock_package["data_block"]

    return f"""You are a professional ASX equity research analyst and SEO content engine.

Your task is to generate a high-quality END-OF-DAY (EOD) stock analysis article for the ONE
stock provided below.

This content is designed for:
- SEO indexing (Google search traffic)
- Retail trader education
- Post-market strategy interpretation
- Content automation pipeline feeding a live website

-------------------------------------------------
MARKET CONTEXT
-------------------------------------------------
{market_block}

-------------------------------------------------
STOCK PACKAGE: {ticker}
-------------------------------------------------
{data_block}

-------------------------------------------------
CRITICAL CONTEXT
-------------------------------------------------
This is END-OF-DAY (EOD) data.

You MUST:
- Use full-session price action (NOT intraday signals)
- Focus on closing behavior, not triggers
- Avoid VWAP, entry signals, or intraday mechanics
- Avoid any "real-time execution framing"
- Disregard word count limits. Deliver high-density, comprehensive content without any fluff.

-------------------------------------------------
OUTPUT REQUIREMENT (STRICT)
-------------------------------------------------
For this stock you MUST generate:
1. One English SEO article in Markdown
2. One Chinese SEO article in Markdown (independent narrative, NOT a literal translation of #1)
3. An English URL slug (3–6 words, lowercase, hyphenated, no stock ticker in it, reflecting
   the dominant theme of this article)

-------------------------------------------------
ARTICLE STRUCTURE (SEO + TRADING HYBRID) — required for both EN and ZH versions
-------------------------------------------------

## 1. YAML Front Matter (mandatory)
Include:
- title (SEO optimized, natural language)
- description (1–2 sentences, search oriented)
- pubDate (YYYY-MM-DD)

Ensure all string values are wrapped in double quotes, e.g.:
title: "[Insert Title Here]"
description: "[Insert Description Here]"
pubDate: "{date.today().isoformat()}"

## 2. Market Context Section
- ASX200 performance
- sector leadership
- macro tone (risk-on / risk-off / rotation)

## 3. Stock Overview
- company name
- sector
- market cap (if provided)
- positioning summary (1 paragraph)

## 4. Technical Analysis (EOD-based)
Must include:
- MA50 / MA200 trend structure
- RSI interpretation (not just the value)
- ADX trend strength interpretation
- volume confirmation or lack of it
- proximity to 52-week high/low

IMPORTANT:
- This is NOT a trading signal section
- Do NOT include entry/exit triggers
- Do NOT use VWAP or intraday logic

## 5. Catalyst & Narrative Flow (MOST IMPORTANT)
You must build a STORY, not a list.
Structure: Catalyst → Market reaction → Confirmation → Interpretation
Rules:
- Prioritize narrative continuity
- If no direct catalyst exists, explain macro/sector/flow-driven narrative
- Always explain "why now"

## 6. EOD Outlook
- continuation vs exhaustion vs consolidation
- next session bias (soft directional expectation)
- key resistance/support zones (NOT trigger-based)

## 7. Conclusion
- one paragraph synthesis
- classify stock behavior (e.g. trend continuation / range-bound / breakout attempt)

## 8. FAQ Section (SEO-critical, flexible generation)
Include at least 4 questions. Questions are NOT fixed, but must collectively cover:
1. Driver Explanation Intent — Why did the stock move today?
2. Sustainability Intent — Is the move likely to continue or fade?
3. Market Structure Intent — What key levels or price zones matter?
4. Forward Scenario Intent — What is the most likely next market behavior?

Rules:
- Questions must be natural and not repetitive across articles
- Must adapt to stock-specific narrative (no template reuse)
- Must reflect actual catalyst/structure of the stock
- Must optimize for long-tail search variation

-------------------------------------------------
STYLE RULES
-------------------------------------------------
- No repetitive sentence structures across sections
- No rigid templates or robotic phrasing
- Prioritize interpretation over data dumping
- Maintain analyst tone, not news reporter tone
- Maintain narrative coherence across the full article
- The Chinese version must read as independently written, not translated

-------------------------------------------------
HARD CONSTRAINTS
-------------------------------------------------
- NO intraday mechanics (VWAP, entry trigger, breakout triggers)
- NO real-time trading instructions
- NO deterministic predictions
- NO repeated phrasing across languages
- NO hallucinated data
- If data is missing, explicitly state: "Cannot verify due to missing dataset" (EN) /
  "数据待核实" (ZH)

-------------------------------------------------
OUTPUT FORMAT (STRICT — nothing outside these markers, no preamble, no closing remarks)
-------------------------------------------------

【SEO_SLUG】english-slug-here【/SEO_SLUG】
【SEO_EN】
```markdown
(full English article including frontmatter)
```
【/SEO_EN】
【SEO_ZH】
```markdown
(full Chinese article including frontmatter)
```
【/SEO_ZH】

(Do not add any text before 【SEO_SLUG】 or after the closing 【/SEO_ZH】.)

-------------------------------------------------
BACKTEST BEFORE FINAL OUTPUT (MANDATORY EXECUTION)
-------------------------------------------------
1. First, generate an internal draft (one EN article + one ZH article) for self-backtesting.
2. Verify the draft (one EN article + one ZH article) meets all requirements above,
   complies with SEO best practices, delivers a strong personal perspective,
   no regulatory/legal violations, no hallucinated data.
   If not, draft another one and repeat this step.
3. Verify the text logically coherent and concise, avoiding redundant descriptions.
4. Verify the format meets the criteria of md files and output exactly one EN article + one ZH article + one slug.
5. Verify every marker above is spelled exactly as specified.
6. Skip any explanation of this process — output ONLY the final version, using the exact
   marker format above."""


def _parse_seo_article_response(raw_text: str, ticker: str,
                                 gemini_meta: Optional[dict] = None) -> dict:
    gemini_meta     = gemini_meta or {}
    finish_reason   = gemini_meta.get("finish_reason", "")
    thoughts_tokens = gemini_meta.get("thoughts_tokens", 0)
    output_tokens   = gemini_meta.get("output_tokens", 0)

    def _extract(tag: str) -> str:
        mm = re.search(rf"【{tag}】(.*?)【/{tag}】", raw_text, re.DOTALL)
        return mm.group(1).strip() if mm else ""

    slug       = _extract("SEO_SLUG")
    seo_en_raw = _extract("SEO_EN")
    seo_zh_raw = _extract("SEO_ZH")

    seo_en_raw = re.sub(r"^```(?:markdown)?\s*\n?", "", seo_en_raw)
    seo_en_raw = re.sub(r"\n?```\s*$", "", seo_en_raw).strip()
    seo_zh_raw = re.sub(r"^```(?:markdown)?\s*\n?", "", seo_zh_raw)
    seo_zh_raw = re.sub(r"\n?```\s*$", "", seo_zh_raw).strip()

    valid = bool(slug and seo_en_raw and seo_zh_raw)
    fail_reason = ""
    if not valid:
        missing = [n for n, v in
                   [("SEO_SLUG", slug), ("SEO_EN", seo_en_raw), ("SEO_ZH", seo_zh_raw)]
                   if not v]
        if finish_reason and "MAX_TOKENS" in finish_reason.upper():
            fail_reason = (f"确认被MAX_TOKENS截断（thinking消耗{thoughts_tokens} token，"
                            f"正文消耗{output_tokens} token），缺失字段:{missing}")
        else:
            fail_reason = f"必需marker缺失:{missing}"
        log.warning(f"_parse_seo_article_response: [{ticker}] {fail_reason}")

    return {
        "slug": slug, "seo_en_raw": seo_en_raw, "seo_zh_raw": seo_zh_raw,
        "_valid": valid, "_fail_reason": fail_reason,
    }


def _validate_seo_article_fields(fields: dict) -> tuple:
    if not fields.get("_valid", False):
        return False, fields.get("_fail_reason", "解析失败")

    for key, label in [("seo_en_raw", "英文文章"), ("seo_zh_raw", "中文文章")]:
        content = fields.get(key, "")
        if len(content) < SEO_ARTICLE_MIN_CHARS:
            return False, f"{label}长度不足({len(content)}字符)"
        if not content.strip().startswith("---"):
            return False, f"{label}frontmatter格式错误（未以---开头）"
        if "title:" not in content or "pubdate:" not in content.lower():
            return False, f"{label}缺少frontmatter必需字段(title/pubDate)"
        if content.count("## ") < 5:
            return False, f"{label}二级标题数量不足(<5)，结构可能不完整"

    slug_clean = _sanitize_slug(fields.get("slug", ""))
    if not slug_clean:
        return False, f"slug清洗后为空(原始值:{fields.get('slug','')!r})"
    fields["slug_clean"] = slug_clean

    return True, ""

def _write_seo_article_files(ticker: str, slug: str, content_en: str, content_zh: str) -> tuple:
    filename = f"{date.today().isoformat()}-{ticker}-{slug}.md"
    os.makedirs(BLOG_CONTENT_DIR_EN, exist_ok=True)
    os.makedirs(BLOG_CONTENT_DIR_ZH, exist_ok=True)
    path_en = os.path.join(BLOG_CONTENT_DIR_EN, filename)
    path_zh = os.path.join(BLOG_CONTENT_DIR_ZH, filename)
    with open(path_en, "w", encoding="utf-8") as f:
        f.write(content_en)
    with open(path_zh, "w", encoding="utf-8") as f:
        f.write(content_zh)
    return path_en, path_zh


def serialize_to_prompt(market_snap: dict, stocks_block: str, platform: str) -> str:
    dev_ma50 = market_snap.get("dev_from_ma50_pct")
    dev_str  = f"{dev_ma50}%" if dev_ma50 is not None else "N/A"
    sector_str = "、".join(
        f"{s}({p:+.1f}%)" for s, p in market_snap.get("sector_leaders", [])
    ) or "数据暂缺"
    market_block = (
        f"日期：{market_snap.get('date', date.today().isoformat())}\n"
        f"ASX200：{market_snap.get('xjo_close',0)} "
        f"（当日涨跌幅：{market_snap.get('xjo_change_pct',0):+.2f}%）\n"
        f"大盘状态分类：{market_snap.get('market_status','normal')}"
        f"（内部分类标签；驱动该分类的原始数据：ASX200偏离50日均线 {dev_str}）\n"
        f"今日领涨板块：{sector_str}"
    )

    instructions = {
       "twitter": """You are an event-driven ASX equity trader generating high-signal X (Twitter) content.

INPUT:
- ASX index data
- sector performance
- up to 3 stocks (price, technicals, news timeline)

OBJECTIVE:
Convert stock-specific inputs into dense trading interpretation.
Focus on causality, expectation shifts, positioning, and pricing — not repetition or narrative expansion.

🚨 STOCK ISOLATION EXECUTION RULE (NEW, CRITICAL)

* Treat EACH stock as an independent task unit.
* First, internally separate input into individual stock data packages.
* Then process ONE stock at a time using the full tweet-generation pipeline.
* Do NOT mix information across stocks.
* Do NOT generate combined or cross-stock tweets.

--------------------------------------------------

📦 OUTPUT MODE (STRICT)

- Each stock must contain EXACTLY 3 tweets
- Each TWEET must be wrapped in its own triple backtick code block
- No text outside code blocks
- Clean, copy-ready format

If multiple stocks exist:

* Output stock A (3 tweets/ 3 code blocks)
* then stock B (3 tweets/ 3 code blocks)
* then stock C (3 tweets/ 3 code blocks)

--------------------------------------------------

🧠 TRADER SPEECH RULE

All tweets must sound like real-time trader notes.

STRICT RULES:

- No formal comparisons (Before/After is banned)
- No full causal explanation chains
- No labeled reasoning (catalyst/driver/flow labels are forbidden in output)
- Thoughts must be incomplete or slightly abrupt
- Sentences may "skip logic steps"
- Interpretation must be implied, not declared

--------------------------------------------------

📉 STRUCTURE (FIXED 4 TWEETS ONLY)

TWEET 1 — CATALYST + MARKET INTERPRETATION
- [Ticker] + [price move]
- The immediate trigger (news / announcement / market attention)
- What traders are suddenly pricing in

Goal:
Explain the first reaction

SHARP OPINION RULE：
Tweet 1 must contain a non-obvious trading observation.

Focus on:
- what market participants may be misunderstanding
- where positioning may become uncomfortable
- what could surprise traders

TWEET 2 — POSITIONING + EXPECTATION RESET

- What changed in market perception
- Who is likely buying/selling
- Whether this looks like fresh money, continuation, squeeze, or late chasing

Goal:
Show the hidden battle behind the price.

TWEET 3 — TRADE SETUP + RISK

- Current phase: early / middle / late repricing
- Sustainability of the move
- Short-term and medium-term bias
- What could invalidate the move

Goal:
Give a trader's opinion, not a company summary.

Tweets must read as one continuous trader thought, not a research note.
Avoid numbered reasoning or formal frameworks.

Each tweet must be ≤280 characters; no multi-paragraph or multi-point construction.

--------------------------------------------------

🔧 CRITICAL COMPRESSION RULE (MANDATORY)

Under NO circumstance can tweet count exceed 3.

If content overflows:
→ remove repetition, not analytical depth

--------------------------------------------------

🔥 SECOND-ORDER INTERPRETATION (MANDATORY)

Embed implicitly:

- POSITIONING (who is trapped / who is re-entering)
- FLOW DYNAMICS (new money vs continuation vs squeeze)
- PRICING PHASE (early / mid / late / exhaustion)
- BEHAVIOR SIGNAL (overreaction / underreaction / confirmation)

Do NOT label these explicitly.

--------------------------------------------------

📊 QUALITY RULES

- Each tweet must add NEW inference
- No repetition of same idea in different wording
- Each tweet must escalate insight level
- No restating raw input data

--------------------------------------------------

❌ HARD ANTI-FILLER RULES

- No generic phrases (“interesting”, “market watching”, etc.)
- No “suggests / indicates / therefore / because”
- No essay-style explanations
- No repeated sentence structures
- No macro market commentary

--------------------------------------------------

🧠 HUMAN SIGNALS (GLOBAL REQUIREMENTS)

Across each tweet:

- Use trader intuition naturally
- Include uncertainty only when justified
- Show at least one tension or contradiction in the setup
- Avoid sounding like a research report

Use:

- fragments instead of full sentences
- hesitation allowed ("feels like", "maybe", "not fully clean")
- partial thoughts
- trading intuition tone

Avoid:

- structured analysis language
- academic transitions
- fully closed reasoning

--------------------------------------------------

📊 TONE

- Default: balanced
- Slightly opinionated but probabilistic
- Avoid persistent pessimism
- At least one clear stance per tweet

--------------------------------------------------

📦 OUTPUT FORMAT 

- Each tweet = one code block
- No titles
- No extra text
- No explanations
- if ticker is mentioned in a tweet, ticker format must be for instance“$BHP.AX”

--------------------------------------------------

🔥 X ENGAGEMENT RULE

Each tweet should contain one sharp observation that a trader would want to save or share.
Avoid repeating obvious facts already visible from price/news.

--------------------------------------------------

Backtest Before Final Output (Mandatory)

a. generate an initial draft of the copy for self-backtesting to ensure it meets all prompt requirements. 
   If not, redraft another one and repeat this step.
b. Verify if the draft delivers a strong personal perspective. 
c. double-check that the output formatting is entirely correct. 
D. Do not explain your process—output only the final version of the copy.
""",

        "xiaohongshu": """ASX小红书交易盲盒复盘（叙事流IP版）

你是ASX短线交易者，每天做盯盘记录，不写报告。

━━━━━━━━━━━━━━
核心定义
“盲盒” = 股票（用于隐喻表达，规避平台审查）
全文统一使用“盲盒”，不使用“股票/标的”。
盲盒使用的股票代码，不能包含后缀.AX

━━━━━━━━━━━━━━
目标
用叙事方式记录：
- 每个盲盒：催化剂 + 新闻故事线 + 交易判断

━━━━━━━━━━━━━━
整体原则
- 不写报告结构
- 人话优先，但逻辑必须完整
- 信息密度高，但不分层写
- 内容不能提及股票市场相关字眼以规避平台限制
- 用自然语言像写日记一样表达
━━━━━━━━━━━━━━
一、标题

类似“让我们打开今天的盲盒”的一句话
（仅作参考，类似表达即可，但必须包含盲盒）

━━━━━━━━━━━━━━
二、每个盲盒各写一段点评（核心结构）

--------------------------------
1. 催化剂（必须包含）
- 今天发生了什么（必须带上股票代码）
- 如果有公告必须带一句总结
- 结合数据但不单列数据，用人话解释

--------------------------------
2. 历史背景 + 关注连续性
可以体现：
- 之前发生过什么比较大的催化剂
- 市场之前怎么反应
- 和今天发生的事情有什么关联

要求：

- 参考表达“我上月看它发生哪一件事，因为什么，结果是怎么样，然后今天发生了什么事，有什么变化”

- 像“持续观察者视角”，不是一次性解读

--------------------------------
3. 结构判断 + 趋势分析 （1-2句话）

允许乐观又真实的表达，类似：
-短期/中期/长期趋势向好

不允许悲观表达，类似：
-不追，不买，不看好

━━━━━━━━━━━━━━
备注1、盲盒数量规则
- 最多3个
- 按当天筛选结果（上面的输入数据）决定

━━━━━━━━━━━━━━
备注2、开头，结尾规则
不单独写开头总结段和结尾总结段

━━━━━━━━━━━━━━
备注3、风格要求
- 用人话叙事
- 有情绪
- 像交易日记而不是报告
- 有IP开场锚点
- 有观察者视角连续性
- 不拆分结构
- 信息高密度但自然流动

━━━━━━━━━━━━━━
三、回测再二次输出（必须执行）

1、先出一版文案草稿给自己回测，看是否符合prompt的要求。
2、每一个盲盒（股票）用200字以内的字数，按照备注3的语言风格，以主观的视角复述一次草稿，作为输出的文案。
3、筛选出文案里面所有关于股票和投资和市场和赌博的相关用词，然后替换成合法合规的生活化用词。
4、检查文章是否有强烈个人观点，没有就加上观点。
5、检查文章是否有逻辑错误。
6、检查字数限制是否正确，超过10%就返回步骤2。
7、不用交代过程，只给出最终版文案。

━━━━━━━━━━━━━━

备注4、在每一个股票，的点评，的结尾后加上标签

#ASX #澳洲
⚠️仅个人记录，不构成投资建议

- 标签不算在字数限制以内
- 标签与正文之间空两行
""",
    }

    instruction = instructions.get(platform, instructions["twitter"])

    return f"""请先仔细阅读我现在发的全部输入内容，深度消化，再直接给出最终回复。如果有疑问，请自己解决，我不会再给出更多解释。以下是具体prompt。
    
    Prompt — {platform.upper()} — {market_snap.get('date','')}

你是一位专注澳大利亚股市(ASX)的资深投资分析师。以下是今日为你准备的输入数据。

=== 今日市场数据 ===
{market_block}

=== Top 3 精选股票数据包（含技术面+新闻+公告+PDF关键段落）===
{stocks_block}

=== 输出任务 ===
{instruction}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

# ════════════════════════════════════════════════════════════
# 7. 选股筛选
# ════════════════════════════════════════════════════════════

TREND_SCORE_THRESHOLD = {
    "T1": 0.45,
    "T2": 0.40,
    "T3": 0.35,
    "T4": 0.30,
}

def _passes_tier(tech: dict, tier: dict) -> bool:
    lc        = tech["price"]
    vol_ratio = tech["vol_ratio"]
    close_pos = tech["close_pos_pct"] / 100.0
    volume_s  = tech["_volume"]
    high_s    = tech["_high"]
    low_s     = tech["_low"]

    if float(volume_s.iloc[-20:].mean()) * lc < 300_000:
        return False

    r15 = pd.concat([high_s, low_s], axis=1).iloc[-15:]
    pr  = (float(r15.iloc[:, 0].max()) - float(r15.iloc[:, 1].min())) / lc
    if pr > tier["consol"]:
        return False

    if tier["vol_decline"]:
        if not _check_volume_quality(volume_s):
            return False

    if tier["di_cross"] and tech["plus_di"] <= tech["minus_di"]:
        return False

    if tech["adx14"] < tier["adx_min"]:
        return False

    if not (tier["rsi_lo"] <= tech["rsi14"] <= tier["rsi_hi"]):
        return False

    if close_pos < tier["close_pos"]:
        return False

    trend_result = calc_trend_strength_score(tech, tier)
    tech["trend_strength_score"] = trend_result["trend_strength_score"]
    tech["trend_sub_scores"]     = trend_result["sub_scores"]

    threshold = TREND_SCORE_THRESHOLD.get(tier["level"], 0.35)
    if trend_result["trend_strength_score"] < threshold:
        return False

    return True

def select_top3(all_data: dict, market_snap: dict,
                write_to_db: bool = True) -> tuple:
    xjo_s     = market_snap.get("xjo_series")
    today_ann = fetch_today_announcements()

    log.info("分级筛选（T1-T4全部扫描，合并排序）...")
    seen_tickers = {}

    for tier in TIERS:
        log.info(f"  {tier['level']} ({tier['label']})...")
        count = 0
        for ticker, df in all_data.items():
            if ticker in seen_tickers:
                continue
            try:
                if len(df) < 60:
                    continue
                tech = build_tech_summary(df, xjo_s)
                if _passes_tier(tech, tier):
                    tech["ticker"]     = ticker
                    tech["tier_level"] = tier["level"]
                    tech["tier_label"] = tier["label"]

                    tech["persistence_score"] = _check_trend_persistence(
                        tech["_close"], tech["_adx_s"],
                        tech["_pdi_s"], tech["_mdi_s"],
                    )

                    tech["hh_hl"]      = _check_higher_highs_lows(tech["_high"], tech["_low"])
                    tech["ma_aligned"] = _check_ma_alignment(tech, tier["level"])

                    code      = ticker.replace(".AX", "")
                    ann       = today_ann.get(code, {})
                    ann_date  = ann.get("date", "") if ann else ""
                    today_str = date.today().isoformat()
                    week_ago  = (date.today() - timedelta(days=7)).isoformat()
                    month_ago = (date.today() - timedelta(days=30)).isoformat()
                    if ann.get("sensitive") and ann_date == today_str:
                        tech["catalyst"] = 1.0
                    elif ann.get("sensitive") and ann_date >= week_ago:
                        tech["catalyst"] = 0.7
                    elif ann_date >= month_ago:
                        tech["catalyst"] = 0.3
                    else:
                        tech["catalyst"] = 0.0

                    seen_tickers[ticker] = tech
                    count += 1
            except Exception as e:
                log.debug(f"筛选异常 [{ticker}]: {e}")
        log.info(f"    → 本层新增 {count} 个（累计 {len(seen_tickers)} 个）")

    raw_signals = list(seen_tickers.values())

    if not raw_signals:
        log.info("T1-T4均无信号")
        return [], [], "", "T?"

    for s in raw_signals:
        s["composite_score"] = calc_composite_score(s)
    raw_signals.sort(key=lambda x: x["composite_score"], reverse=True)
    raw_signals = raw_signals[:10]

    filtered_pool = []
    for s in raw_signals:
        fund = fetch_fundamentals(s["ticker"])
        if fund.get("market_cap_m", 0) * 1e6 < 50_000_000:
            log.debug(f"市值过滤 [{s['ticker']}]")
            continue
        s.update(fund)
        s["entry_limit"] = round(s["price"] * 1.02, 3)
        s["stop_loss"]   = round(s["price"] * 0.90, 3)
        s["take_profit"] = round(s["price"] * 1.20, 3)
        filtered_pool.append(s)

    signals = filtered_pool[:TOP_N]

    tier_summary = {}
    for s in signals:
        lv = s.get("tier_level", "T?")
        tier_summary[lv] = tier_summary.get(lv, 0) + 1
    tier_label = " / ".join(f"{lv}×{n}" for lv, n in sorted(tier_summary.items()))
    tier_level = signals[0].get("tier_level", "T?") if signals else "T?"

    if write_to_db:
        selected_tickers = {s["ticker"] for s in signals}
        for s in raw_signals:
            save_signal_to_history(
                s, market_snap,
                is_selected=(s["ticker"] in selected_tickers)
            )
        log.info(f"signals_history写入：{len(raw_signals)} 条候选（Top3已标记）")

        wdb.init_watchlist_db()
        for s in filtered_pool:
            wdb.upsert_watchlist(
                ticker=s["ticker"],
                company_name=s.get("company_name", s["ticker"]),
                tier_level=s.get("tier_level", tier_level),
                tier_label=s.get("tier_label", tier_label),
                composite_score=s["composite_score"],
            )
        log.info(f"watchlist写入：{len(filtered_pool)} 只（Top10全部，不只Top3）")

    return signals, raw_signals, tier_label, tier_level

VALIDATION_LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tier_validation.log"
)

def log_tier_validation(raw_signals: list, signals: list,
                        tier_label: str, market_snap: dict) -> None:
    today = date.today().isoformat()

    tier_counts = {}
    for s in raw_signals:
        lv = s.get("tier_level", "T?")
        tier_counts[lv] = tier_counts.get(lv, 0) + 1

    lines = [
        f"{'='*70}",
        f"验证日期: {today}",
        f"ASX200: {market_snap.get('xjo_change_pct', 0):+.2f}% "
        f"状态: {market_snap.get('market_status', 'normal')}",
        f"{'-'*70}",
        f"【Top10候选池层级分布】（进入候选池的股票，按tier统计）",
    ]
    for lv in ["T1", "T2", "T3", "T4"]:
        count = tier_counts.get(lv, 0)
        lines.append(f"  {lv}: {count}只")

    lines.append(f"{'-'*70}")
    lines.append(f"【Top{len(signals)}最终入选】层级分布: {tier_label or '（无）'}")

    if signals:
        for i, s in enumerate(signals, 1):
            trend_score = s.get("trend_strength_score", "N/A")
            comp_score  = s.get("composite_score", "N/A")
            persist     = s.get("persistence_score", "N/A")
            lines.append(
                f"  #{i} {s['ticker']} [{s.get('tier_level','?')}] "
                f"composite={comp_score} trend_strength={trend_score} "
                f"persistence={persist}"
            )
    else:
        lines.append("  （今日无Top3入选，T1-T4筛选全部为空或市值过滤后不足）")

    lines.append(f"{'-'*70}")
    lines.append(f"【滚动7日T1/T2候选数均值】（避免单日样本误判趋势）")
    try:
        import sqlite3
        cutoff = (date.today() - timedelta(days=7)).isoformat()
        with sqlite3.connect(ANN_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT signal_date, tier_level, COUNT(*) as cnt
                FROM signals_history
                WHERE signal_date >= ? AND tier_level IN ('T1', 'T2')
                GROUP BY signal_date, tier_level
            """, (cutoff,)).fetchall()

        t1_counts = [r[2] for r in rows if r[1] == "T1"]
        t2_counts = [r[2] for r in rows if r[1] == "T2"]
        t1_avg = sum(t1_counts) / len(t1_counts) if t1_counts else 0.0
        t2_avg = sum(t2_counts) / len(t2_counts) if t2_counts else 0.0
        t1_days_with_data = len(t1_counts)
        t2_days_with_data = len(t2_counts)

        lines.append(f"  T1: 过去7天内有{t1_days_with_data}天产生候选，均值{t1_avg:.2f}只/天")
        lines.append(f"  T2: 过去7天内有{t2_days_with_data}天产生候选，均值{t2_avg:.2f}只/天")
    except Exception as e:
        lines.append(f"  滚动统计查询失败: {e}")

    lines.append(f"{'='*70}")
    lines.append("")

    try:
        with open(VALIDATION_LOG_PATH, "a", encoding="utf-8") as f:
            f.write("\n".join(lines))
        log.info(f"验证日志已写入: {VALIDATION_LOG_PATH}")
    except Exception as e:
        log.error(f"验证日志写入失败: {e}")

def _passes_tier_diagnostic(tech: dict, tier: dict) -> dict:
    lc        = tech["price"]
    vol_ratio = tech["vol_ratio"]
    close_pos = tech["close_pos_pct"] / 100.0
    w52_hi    = tech["w52_hi"]
    volume_s  = tech["_volume"]
    high_s    = tech["_high"]
    low_s     = tech["_low"]

    checks = {}

    checks["ma50_trend"] = (lc >= tech["ma50"] and tech["ma50_up"])

    checks["liquidity"] = (float(volume_s.iloc[-20:].mean()) * lc >= 300_000)

    r15 = pd.concat([high_s, low_s], axis=1).iloc[-15:]
    pr  = (float(r15.iloc[:, 0].max()) - float(r15.iloc[:, 1].min())) / lc
    checks["consolidation"] = (pr <= tier["consol"])

    if tier["vol_decline"]:
        checks["volume_quality"] = _check_volume_quality(volume_s)
    else:
        checks["volume_quality"] = True

    checks["ma_alignment"] = _check_ma_alignment(tech, tier["level"])

    if tier["level"] in ("T1", "T2"):
        checks["hh_hl_structure"] = _check_higher_highs_lows(high_s, low_s)
    else:
        checks["hh_hl_structure"] = True

    checks["near_52w_hi"] = (not tier["near_52w_hi"]) or (lc >= w52_hi * 0.90)

    checks["adx_strength"] = (tech["adx14"] >= tier["adx_min"])

    checks["di_direction"] = (not tier["di_cross"]) or (tech["plus_di"] > tech["minus_di"])

    checks["vwap_position"] = (not tier["vwap_above"]) or (lc >= tech["vwap20"] and tech["vwap_up"])

    checks["relative_strength"] = (tech["rs_vs_xjo"] >= tier["rs_min"])

    checks["volume_multiple"] = (vol_ratio >= tier["vol_mult"])

    checks["rsi_range"] = (tier["rsi_lo"] <= tech["rsi14"] <= tier["rsi_hi"])

    checks["close_position"] = (close_pos >= tier["close_pos"])

    checks["_all_passed"] = all(v for k, v in checks.items() if k != "_all_passed")

    return checks

def run_tier_diagnostic(all_data: dict, market_snap: dict, 
                         tier_levels: list = ["T1", "T2"]) -> None:
    xjo_s = market_snap.get("xjo_series")
    tier_map = {t["level"]: t for t in TIERS}
    
    for tier_level in tier_levels:
        tier = tier_map[tier_level]
        log.info(f"=== 诊断 {tier_level} ({tier['label']}) ===")
        
        fail_counts = {}
        total_checked = 0
        total_passed = 0
        
        for ticker, df in all_data.items():
            if len(df) < 60:
                continue
            try:
                tech = build_tech_summary(df, xjo_s)
                result = _passes_tier_diagnostic(tech, tier)
                total_checked += 1
                
                if result["_all_passed"]:
                    total_passed += 1
                
                for check_name, passed in result.items():
                    if check_name == "_all_passed":
                        continue
                    if not passed:
                        fail_counts[check_name] = fail_counts.get(check_name, 0) + 1
            except Exception as e:
                log.debug(f"诊断异常 [{ticker}]: {e}")
        
        log.info(f"{tier_level}: 检查{total_checked}只，通过{total_passed}只 "
                 f"({total_passed/total_checked*100:.2f}%)")
        log.info(f"{tier_level} 各条件失败次数（按失败率降序）：")
        for check_name, count in sorted(fail_counts.items(), 
                                         key=lambda x: x[1], reverse=True):
            pct = count / total_checked * 100
            log.info(f"    {check_name}: 失败{count}只 ({pct:.1f}%)")

def run_threshold_scan(all_data: dict, market_snap: dict,
                       tier_levels: list = None) -> None:
    if tier_levels is None:
        tier_levels = ["T1", "T2", "T3", "T4"]

    xjo_s    = market_snap.get("xjo_series")
    tier_map = {t["level"]: t for t in TIERS}

    for tier_level in tier_levels:
        tier   = tier_map[tier_level]
        scores = []

        for ticker, df in all_data.items():
            if len(df) < 60:
                continue
            try:
                tech = build_tech_summary(df, xjo_s)
                result = calc_trend_strength_score(tech, tier)
                scores.append(result["trend_strength_score"])
            except Exception as e:
                log.debug(f"阈值扫描异常 [{ticker}]: {e}")

        if not scores:
            log.info(f"{tier_level}: 无有效评分数据")
            continue

        scores_series = pd.Series(scores)
        log.info(f"=== {tier_level} trend_strength_score 分布 ===")
        log.info(f"  样本数: {len(scores)}")
        log.info(f"  均值: {scores_series.mean():.4f}  "
                 f"中位数: {scores_series.median():.4f}  "
                 f"标准差: {scores_series.std():.4f}")
        log.info(f"  分位数: P90={scores_series.quantile(0.90):.4f}  "
                 f"P80={scores_series.quantile(0.80):.4f}  "
                 f"P70={scores_series.quantile(0.70):.4f}  "
                 f"P60={scores_series.quantile(0.60):.4f}  "
                 f"P50={scores_series.quantile(0.50):.4f}")
        log.info(f"  不同阈值下的通过率：")
        for threshold in [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
            pass_count = sum(1 for s in scores if s >= threshold)
            pass_pct   = pass_count / len(scores) * 100
            log.info(f"    阈值{threshold}: 通过{pass_count}只 ({pass_pct:.1f}%)")
        log.info("")

def run_screener_flow(all_data: dict, market_snap: dict) -> list:
    today   = date.today().strftime("%Y-%m-%d")
    start   = time.time()
    status  = market_snap.get("market_status", "normal")
    xjo_s   = market_snap.get("xjo_series")
    xjo_pct = market_snap.get("xjo_change_pct", 0)

    market_note  = "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。" if status == "yellow" else ""
    market_label = "⚠️ " if status == "yellow" else ""

    today_ann = fetch_today_announcements()

    signals, raw_signals, tier_label, tier_level = select_top3(all_data, market_snap)

    elapsed_screen = round((time.time() - start) / 60, 1)

    log_tier_validation(raw_signals, signals, tier_label, market_snap)

    if not signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只（T1-T4均无信号）\n"
            f"市场动能不足，建议观望。耗时：{elapsed_screen}分钟{market_note}"
        )
        return []

    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"ASX200：{xjo_pct:+.2f}%  |  扫描：{len(all_data)} 只  |  耗时：{elapsed_screen}分钟\n"
        f"层级分布：{tier_label}  |  精选 Top {len(signals)} 只\n\n"
        + "\n".join(
            f"#{i+1} {s['ticker']} | [{s.get('tier_level','?')}] "
            f"评分:{s['composite_score']} 持续性:{s.get('persistence_score',0)} "
            f"RS:{s['rs_vs_xjo']} ADX:{s['adx14']} 量比:{s['vol_ratio']}x"
            for i, s in enumerate(signals)
        )
        + market_note
    )

    log.info(f"深度分析 Top {len(signals)} 只（Gemini）...")
    for idx, s in enumerate(signals, 1):
        code = s["ticker"].replace(".AX", "")
        log.info(f"  [#{idx}] {s['ticker']} 评分:{s['composite_score']} "
                 f"持续性:{s.get('persistence_score',0)}...")

        ann_hist = fetch_announcements(code, today_ann=today_ann)
        news     = fetch_news(s["ticker"], s.get("company_name", ""))
        timeline = build_timeline_text(code, ann_hist, news, today_ann)

        prompt   = _build_screener_prompt(s, timeline, s.get("tier_label", tier_label))
        analysis = ask_gemini(prompt, label=s["ticker"])
        if not analysis:
            analysis = "⚠️ Gemini分析暂时不可用"

        json_fields             = _parse_gemini_json_fields(analysis)
        s["_json_tag_en"]       = json_fields.get("tag_en", "")
        s["_json_tag_zh"]       = json_fields.get("tag_zh", "")
        s["_json_one_liner_zh"] = json_fields.get("one_liner_zh", "")
        s["_json_one_liner_en"] = json_fields.get("one_liner_en", "")
        s["confidence"]         = calc_confidence(s, s.get("tier_level", tier_level))
        s["_json_valid"]        = bool(s["_json_tag_en"] and s["_json_one_liner_en"])
        log.info(
            f"  JSON字段 [{s['ticker']}]: tag_en={s['_json_tag_en']!r} "
            f"confidence={s['confidence']} valid={s['_json_valid']}"
        )

        ann_info = today_ann.get(code, {})
        ann_line = ""
        if ann_info:
            flag     = "⭐ " if ann_info["sensitive"] else ""
            ann_line = f"\n📋 今日公告：{flag}{ann_info['headline']}"

        ma200_str    = f" MA200:${s['ma200']}" if s.get("ma200") else ""
        vol_c_badge  = " 📈量能连续" if s.get("vol_consistency") else ""
        s_tier_label = s.get("tier_label", tier_label)
        send_telegram(
            f"<b>#{idx} {s_tier_label} {s.get('company_name', s['ticker'])}</b> ({s['ticker']})\n"
            f"📅 {today} | {s.get('sector','未知')} | 市值:${s.get('market_cap_m',0)}M | "
            f"综合评分:{s['composite_score']} | 持续性:{s.get('persistence_score',0)}\n\n"
            f"💰 昨收：${s['price']} ({s['change_pct']:+.2f}%) | "
            f"1年历史分位:{s.get('price_pct_1y',50)}%{vol_c_badge}\n"
            f"🟢 入场上限：${s['entry_limit']}（超过不追）\n"
            f"🎯 止盈：${s['take_profit']}（+20%）\n"
            f"🛑 止损：${s['stop_loss']}（-10%）\n\n"
            f"📊 RSI:{s['rsi14']} ADX:{s['adx14']} +DI:{s['plus_di']}\n"
            f"   量比:{s['vol_ratio']}x 收盘位:{s['close_pos_pct']}%\n"
            f"   RS(vsXJO):{s['rs_vs_xjo']}{ma200_str}"
            f"{ann_line}\n\n"
            f"🤖 <b>深度分析</b>\n{analysis}\n\n"
            f"⚠️ 核对图表再决定入场{market_note}"
        )
        time.sleep(1.0)

    valid_signals = [s for s in signals if s.get("_json_valid")]
    log.info(f"生成signals.json：{len(valid_signals)}/{len(signals)} 只有效JSON字段...")
    written = generate_signals_json(valid_signals)
    if written:
        pushed = push_to_github(
            ["src/data/signals/en.json", "src/data/signals/zh.json"],
            commit_message=f"chore: update signals {today}",
        )
        if pushed:
            send_telegram(
                f"🌐 <b>网站信号已更新</b> {today}\n"
                f"signals.json已推送GitHub，Cloudflare正在重建。\n"
                f"信号数量：{len(valid_signals)} 只（SEO文章随后单独生成）"
            )
        else:
            send_telegram(
                f"⚠️ <b>GitHub推送失败</b> {today}\n"
                "signals.json已生成但未能推送，请查看screener.log手动处理。"
            )
    else:
        log.info("无有效信号，跳过GitHub推送，网站保持昨日数据")

    elapsed = round((time.time() - start) / 60, 1)
    log.info(f"选股完成：{tier_level}，Top{len(signals)}，{elapsed}分钟")
    send_telegram(
        f"✅ <b>选股完成</b> {today} | {tier_label} | Top{len(signals)} | {elapsed}分钟"
    )
    return signals


def run_seo_article_flow(signals: list, market_snap: dict) -> None:
    if not signals:
        log.info("run_seo_article_flow: 无signals，跳过（大盘红灯或T1-T4均无候选）")
        return

    today      = date.today().isoformat()
    today_ann  = fetch_today_announcements()
    log.info("=== SEO文章逐只生成流程启动 ===")

    success_count = 0

    for rank, s in enumerate(signals, 1):
        ticker = s["ticker"]
        code   = ticker.replace(".AX", "")
        log.info(f"  [Top{rank}] {ticker} SEO文章生成中...")

        try:
            ann_hist   = fetch_announcements(code, today_ann=today_ann)
            news       = fetch_news(ticker, s.get("company_name", ""))
            timeline   = build_timeline_text(code, ann_hist, news, today_ann)
            pdf_texts  = [a["pdf_text"] for a in ann_hist if a.get("pdf_text")]
            data_block = _build_report_stock_block(ticker, s, s, timeline, pdf_texts, rank)
        except Exception as e:
            log.error(f"run_seo_article_flow: 数据包构建失败 [{ticker}]: {e}")
            send_telegram(f"⚠️ [Top{rank} {ticker}] SEO文章数据包构建失败: {e}")
            continue

        prompt = _build_seo_article_prompt(market_snap, {"ticker": ticker, "data_block": data_block})
        raw, finish_reason, thoughts_tokens, output_tokens = ask_gemini(
            prompt, label=f"SEO_ARTICLE_{ticker}", config=GEMINI_CFG_SEO_ARTICLE, return_meta=True
        )
        log.info(
            f"  [{ticker}] SEO调用完成: finish_reason={finish_reason or 'N/A'} "
            f"thinking_tokens={thoughts_tokens} output_tokens={output_tokens}"
        )

        if not raw:
            log.error(f"[{ticker}] Gemini无返回 finish_reason={finish_reason}，转人工兜底")
            send_telegram(
                f"⚠️ [Top{rank} {ticker}] SEO文章生成失败：Gemini无响应\n"
                f"finish_reason={finish_reason or '未知'}\n"
                f"已附上Prompt，可手动粘贴给AI生成后手动上传。"
            )
            send_document(
                f"seo_prompt_fallback_{ticker}_{today}.txt", prompt,
                caption=f"📋 [{ticker}] SEO Prompt人工兜底（Gemini无响应）— {today}"
            )
            continue

        gemini_meta = {
            "finish_reason": finish_reason,
            "thoughts_tokens": thoughts_tokens,
            "output_tokens": output_tokens,
        }
        fields     = _parse_seo_article_response(raw, ticker, gemini_meta=gemini_meta)
        ok, reason = _validate_seo_article_fields(fields)

        if not ok:
            log.warning(f"[{ticker}] 校验失败-{reason}，转人工兜底")
            send_telegram(
                f"⚠️ [Top{rank} {ticker}] SEO文章校验失败: {reason}\n"
                f"已附上Prompt，可手动粘贴给AI生成后手动上传。"
            )
            send_document(
                f"seo_prompt_fallback_{ticker}_{today}.txt", prompt,
                caption=f"📋 [{ticker}] SEO Prompt人工兜底（{reason}）— {today}"
            )
            continue

        try:
            path_en, path_zh = _write_seo_article_files(
                ticker, fields["slug_clean"], fields["seo_en_raw"], fields["seo_zh_raw"]
            )
            rel_en = os.path.relpath(path_en, ASXBOX_REPO)
            rel_zh = os.path.relpath(path_zh, ASXBOX_REPO)
            pushed = push_to_github(
                [rel_en, rel_zh],
                commit_message=f"content: {ticker} SEO article {today}",
            )
            if pushed:
                send_telegram(f"✅ [Top{rank} {ticker}] SEO文章已生成并推送GitHub")
                success_count += 1
            else:
                send_telegram(
                    f"⚠️ [Top{rank} {ticker}] SEO文章已生成但GitHub推送失败，"
                    f"请查看screener.log手动处理"
                )
        except Exception as e:
            log.error(f"[{ticker}] 写文件/推送异常: {e}")
            send_telegram(f"⚠️ [Top{rank} {ticker}] SEO文章写入/推送异常: {e}，已附上Prompt人工兜底")
            send_document(
                f"seo_prompt_fallback_{ticker}_{today}.txt", prompt,
                caption=f"📋 [{ticker}] SEO Prompt人工兜底（写入异常）— {today}"
            )

        time.sleep(1.0)

    log.info(f"run_seo_article_flow完成：{success_count}/{len(signals)} 只成功")

# ════════════════════════════════════════════════════════════
# 8. 日报Prompt流程
# ════════════════════════════════════════════════════════════

def run_report_flow(all_data: dict, market_snap: dict,
                    screener_signals: Optional[list] = None) -> None:
    today     = date.today().isoformat()
    xjo_s     = market_snap.get("xjo_series")
    today_ann = fetch_today_announcements()

    log.info("=== 日报Prompt流程启动 ===")

    if screener_signals:
        target_tickers = [s["ticker"] for s in screener_signals]
        log.info(f"使用screener结果：{target_tickers}")
    else:
        target_tickers = get_top_movers(all_data, top_n=TOP_N)
        log.info(f"使用涨幅Movers：{target_tickers}")

    if not target_tickers:
        log.error("日报：无目标股票")
        send_telegram("⚠️ 日报生成失败：无法确定目标股票，请查看 screener.log")
        return

    stock_blocks = []
    for rank, ticker in enumerate(target_tickers, 1):
        code = ticker.replace(".AX", "")
        df   = all_data.get(ticker)
        if df is None:
            log.warning(f"日报：无K线数据 [{ticker}]，跳过")
            continue

        log.info(f"  [#{rank}] {ticker} 构建日报数据包...")
        existing = next((s for s in (screener_signals or []) if s["ticker"] == ticker), None)
        tech     = existing if existing else build_tech_summary(df, xjo_s)

        if existing and "composite_score" not in tech:
            tech["composite_score"] = calc_composite_score(tech)
        elif not existing:
            tech["composite_score"] = None
            log.info(f"  [{ticker}] 降级模式（大盘红灯/涨幅Movers），"
                     f"不计算composite_score，避免展示无意义分数")

        fund      = existing if (existing and existing.get("company_name")) else fetch_fundamentals(ticker)
        ann_hist  = fetch_announcements(code, today_ann=today_ann)
        news      = fetch_news(ticker, fund.get("company_name", ""))
        timeline  = build_timeline_text(code, ann_hist, news, today_ann)
        pdf_texts = [a["pdf_text"] for a in ann_hist if a.get("pdf_text")]

        block = _build_report_stock_block(ticker, tech, fund, timeline, pdf_texts, rank)
        stock_blocks.append(block)
        time.sleep(1.0)

    if not stock_blocks:
        log.error("日报：所有股票数据包构建失败")
        return

    stocks_block = "\n".join(stock_blocks)
    tickers_str  = " / ".join(target_tickers)
    send_telegram(
        f"📂 <b>ASX日报Prompt就绪 {today}</b>\n\n"
        f"股票：{tickers_str}\n"
        f"以下2个文件已发送，复制文件内容给AI生成文章👇"
    )

    for platform in ["twitter", "xiaohongshu"]:
        log.info(f"发送 [{platform}] Prompt文件...")
        prompt_text = serialize_to_prompt(market_snap, stocks_block, platform)
        filename    = f"prompt_{platform}_{today}.txt"
        caption     = f"📋 {platform.upper()} Prompt — {today}"
        send_document(filename, prompt_text, caption=caption)
        time.sleep(1.5)

    log.info("=== 日报Prompt发送完成 ===")

# ════════════════════════════════════════════════════════════
# 9. 主入口
# ════════════════════════════════════════════════════════════

def main() -> None:
    start = time.time()
    log.info("=" * 60)
    log.info(f"ASX System v18.3 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    log.info("=" * 60)

    log.info("【Step 1】大盘快照...")
    market_snap = get_market_snapshot()
    status      = market_snap.get("market_status", "normal")

    if status == "red":
        send_telegram(
            f"🔴 <b>大盘警告 {date.today().isoformat()}</b>\n\n"
            "ASX200大幅跌破50日均线或近期急跌。\n"
            "今日<b>不建议开新仓</b>，收紧止损至5%。\n\n"
            "日报Prompt将改用涨幅Top3生成，供参考。"
        )

    log.info("【Step 2】下载全市场K线（1年数据）...")
    universe = get_asx_universe()
    if not universe:
        log.error("股票池为空，终止")
        send_telegram("🚨 启动失败：股票池获取失败，请查看 screener.log")
        return

    all_data = download_ohlcv(universe, period="1y")
    if not all_data:
        log.error("K线下载失败，终止")
        send_telegram("🚨 启动失败：K线下载失败，请查看 screener.log")
        return

    elapsed_dl = round((time.time() - start) / 60, 1)
    log.info(f"【Step 2】K线完成：{len(all_data)} 只，{elapsed_dl} 分钟")

    log.info("【Step 2.5】更新历史信号回测结果...")
    update_signal_outcomes(all_data)

    screener_signals = []
    if status != "red":
        log.info("【Step 3】选股筛选流程...")
        screener_signals = run_screener_flow(all_data, market_snap)
    else:
        log.info("【Step 3】大盘红灯，跳过选股")

    if screener_signals:
        log.info("【Step 3.5】SEO文章逐只生成流程...")
        run_seo_article_flow(screener_signals, market_snap)
    else:
        log.info("【Step 3.5】无screener信号，跳过SEO文章生成，网站保持昨日数据")

    log.info("【Step 4】日报Prompt生成流程...")
    run_report_flow(all_data, market_snap,
                    screener_signals=screener_signals if screener_signals else None)

    elapsed = round((time.time() - start) / 60, 1)
    log.info("=" * 60)
    log.info(f"ASX System 全部完成，总耗时：{elapsed} 分钟")
    log.info("=" * 60)
    send_telegram(
        f"🏁 <b>ASX System v18.3 全部完成</b>\n"
        f"📅 {date.today().isoformat()} | ⏱ 总耗时：{elapsed} 分钟\n"
        f"选股：{'跳过（大盘红灯）' if status == 'red' else f'Top{len(screener_signals)}已完成'}\n"
        f"SEO文章/信号JSON：{'已生成，见上方推送结果' if screener_signals else '跳过'}\n"
        f"日报Prompt：Twitter / 小红书 已发送"
    )


if __name__ == "__main__":
    main()
