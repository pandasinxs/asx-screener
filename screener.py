# ============================================================
# ASX SYSTEM — screener.py  v14
#
# 流程一：EOD选股
#   全市场K线 → T1-T4筛选 → Top3加权评分 → 新闻/公告时间线
#   → Gemini深度分析 → Telegram → signals.json → GitHub推送
#
# 流程二：每日日报Prompt（不调用Gemini）
#   Top3 Movers → 技术面+精选新闻+公告+PDF关键段落
#   → 构建三平台Prompt → Telegram附件（SEO/Twitter/小红书）
#
# v14新增（相对v13）：
#   - calc_confidence()：基于层级的置信度算法（替代composite_score当confidence用）
#   - _parse_gemini_json_fields()：正则抓取Gemini输出的JSON字段
#   - generate_signals_json()：生成en.json/zh.json写入asxbox仓库
#   - push_to_github()：自动检测branch，推送signals到Cloudflare Pages
#   - Gemini Prompt末尾强制输出JSON_TAG/ONE_LINER四字段
#   - _json_valid标志：Gemini失败时跳过该股票不写入JSON
#   - send_document()：日报Prompt改为.txt附件发送（解决消息过长分割问题）
#   - 日报平台矩阵：SEO文章 / Twitter / 小红书（替代原Telegram简报）
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("screener.log", encoding="utf-8"),
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
RETRY_MAX       = 20
RETRY_WAIT      = 30
TIMEOUT         = 15
TOP_N           = 3     # 精选Top 3

# ── GitHub网站仓库配置 ────────────────────────────────────────
ASXBOX_REPO     = os.path.expanduser("~/asxbox")
SIGNALS_DIR     = os.path.join(ASXBOX_REPO, "src", "data", "signals")

ASX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept":     "application/json",
    "Referer":    "https://www.asx.com.au",
}
ASX_ANN_ALL    = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
# ASX单股历史公告API已确认不存在（404）。
# 经测试所有参数变体均被服务器忽略，无法按ticker过滤。
# 历史公告通过 fetch_announcements() 从今日公告缓存积累，见下方说明。
PDF_DL_BASE    = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/{doc_key}?access_token=83ff96335c2d45a094df02a206a39ff4"
GOOGLE_RSS     = "https://news.google.com/rss/search?q={q}&hl=en-AU&gl=AU&ceid=AU:en"

PDF_MAX_CHARS    = 2000   # 每个PDF关键段落合计上限（关键词提取后更精准）
PDF_MAX_PER_STOCK = 2
NEWS_MAX         = 5

# ── 公告白名单（保留有实质意义的类型，过滤合规噪音）──────────
# ASX documentType完整列表中，以下类型对股价有实质影响
ANN_WHITELIST = {
    # 业绩 & 财务
    "Quarterly Activities Report",
    "Quarterly Cashflow Report",
    "Half Yearly Report",
    "Preliminary Final Report",
    "Annual Report",
    "Full Year Results",
    "Half Year Results",
    "Appendix 4C",
    "Appendix 4D",
    "Appendix 4E",
    # 资源 & 勘探（矿业/能源关键）
    "Quarterly Production Report",
    "Resource/Reserve Update",
    "Exploration Results",
    "Drilling Results",
    "Mining Results",
    "Results of Operations",
    # 重大事件
    "Merger/Acquisition",
    "Takeover",
    "Scheme of Arrangement",
    "Strategic Review",
    "Major Contract",
    "Material Contract",
    "Capital Raising",
    "Placement",
    "Rights Issue",
    "Share Purchase Plan",
    "CEO/Chairman Change",
    "Director Change",
    "Suspension",
    "Trading Halt",
    "Trading Halt Lifted",
    # 指引 & 展望
    "Guidance",
    "Market Update",
    "Business Update",
    "Investor Presentation",
    "Progress Report",
    "Project Update",
}

# 噪音类型关键词（documentType包含这些词则过滤）
ANN_NOISE_KEYWORDS = [
    "appendix 3", "change of address", "change of registered",
    "notice of meeting", "proxy form", "lodge", "constitution",
    "cleansing statement", "reinstatement", "transfer of interest",
    "share registry", "cease to be", "becoming substantial",
    "shareholder", "top 20", "section 708",
]

# PDF关键词：命中这些词的段落才提取
PDF_KEY_TERMS = [
    "revenue", "production", "guidance", "result", "profit", "loss",
    "cash", "ebitda", "npat", "highlights", "outlook", "summary",
    "drill", "resource", "reserve", "acquisition", "contract",
    "milestone", "update", "completion", "approval", "forecast",
]

# 加权评分权重
SCORE_WEIGHTS = {
    "rs_vs_xjo":      0.35,
    "adx14":          0.25,
    "vol_ratio":      0.20,
    "close_pos_pct":  0.10,
    "price_pct_1y":   0.10,
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
    """
    过去126个交易日（约6个月）内，单日涨跌幅超过阈值的事件。
    让AI能把价格行为和新闻事件对应起来。
    返回：[{"date": "YYYY-MM-DD", "change_pct": float}]
    """
    pct    = close.pct_change() * 100
    recent = pct.iloc[-126:]
    events = []
    for dt, val in recent.items():
        if abs(val) >= threshold_pct:
            events.append({
                "date":       str(dt)[:10],
                "change_pct": round(float(val), 1),
            })
    return sorted(events, key=lambda x: x["date"], reverse=True)[:10]


def build_tech_summary(df: pd.DataFrame,
                       xjo: Optional[pd.Series] = None) -> dict:
    """
    完整技术指标摘要。选股筛选和日报均调用此函数。
    新增：price_percentile_1y / vol_consistency / price_events
    """
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

    roll_max   = close.rolling(126).max()
    max_dd     = float(((close - roll_max) / roll_max * 100).min())
    day_range  = lh - ll
    close_pos  = (lc - ll) / day_range if day_range > 0 else 0.5

    # ── 新增：价格1年历史分位 ─────────────────────────────────
    # 当前价格在过去252日收盘价中的百分位
    # 90%分位突破 vs 50%分位反弹，意义完全不同
    hist_close  = close.iloc[-252:] if len(close) >= 252 else close
    price_pct_1y = round(float((hist_close <= lc).sum() / len(hist_close) * 100), 1)

    # ── 新增：量能连续性 ─────────────────────────────────────
    # 近5日成交量是否逐步递增（连续温和放量比单日爆量质量更高）
    vol5 = volume.iloc[-5:]
    vol_consistency = bool(all(
        vol5.iloc[i] <= vol5.iloc[i + 1] for i in range(len(vol5) - 1)
    ))

    # ── 新增：价格事件（6个月内单日±5%的节点）────────────────
    price_events = calc_price_events(close)

    return {
        # 基础价格
        "price"          : round(lc, 3),
        "change_pct"     : round((lc / prev - 1) * 100, 2),
        "volume"         : round(lv),
        "vol_ratio"      : round(lv / vm20, 2) if vm20 > 0 else 1.0,
        "close_pos_pct"  : round(close_pos * 100, 1),
        # 趋势指标
        "rsi14"          : round(float(rsi_s.iloc[-1]), 1),
        "adx14"          : round(float(adx_s.iloc[-1]), 1),
        "plus_di"        : round(float(pdi_s.iloc[-1]), 1),
        "minus_di"       : round(float(mdi_s.iloc[-1]), 1),
        "vwap20"         : round(vwap_val, 3),
        "vwap_up"        : vwap_slope > 0,
        "rs_vs_xjo"      : calc_rs(close, xjo) if xjo is not None else 1.0,
        # 均线
        "ma20"           : round(float(ma20.iloc[-1]), 3),
        "ma50"           : round(float(ma50.iloc[-1]), 3),
        "ma50_up"        : float(ma50.iloc[-1]) > float(ma50.iloc[-11]),
        "ma200"          : round(float(ma200.iloc[-1]), 3) if len(close) >= 200 else None,
        # 波动 & 风险
        "atr14_pct"      : round(atr14 / lc * 100, 2),
        "w52_hi"         : round(w52_hi, 3),
        "w52_lo"         : round(w52_lo, 3),
        "dist_52w_hi_pct": round((lc / w52_hi - 1) * 100, 1),
        "max_dd_6m_pct"  : round(max_dd, 1),
        # ── v13新增 ──────────────────────────────────────────
        "price_pct_1y"   : price_pct_1y,      # 价格1年历史分位（%）
        "vol_consistency": vol_consistency,    # 近5日量能是否逐步递增
        "price_events"   : price_events,       # 6个月内大涨大跌节点
        # 原始序列（供筛选逻辑使用，不进Prompt）
        "_close"  : close,
        "_high"   : high,
        "_low"    : low,
        "_volume" : volume,
    }


def calc_composite_score(tech: dict) -> float:
    """
    加权综合评分，用于Top3排序。
    各指标先归一化到0-1再加权，避免量纲不同造成的偏差。
    """
    def norm(val, lo, hi):
        return max(0.0, min(1.0, (val - lo) / (hi - lo))) if hi > lo else 0.0

    scores = {
        "rs_vs_xjo"     : norm(tech.get("rs_vs_xjo", 1.0), 0.8, 1.5),
        "adx14"         : norm(tech.get("adx14", 0),        15, 50),
        "vol_ratio"     : norm(tech.get("vol_ratio", 1.0),  1.0, 4.0),
        "close_pos_pct" : norm(tech.get("close_pos_pct", 50), 40, 100),
        "price_pct_1y"  : norm(tech.get("price_pct_1y", 50), 50, 100),
    }
    return round(sum(SCORE_WEIGHTS[k] * v for k, v in scores.items()), 4)


def calc_confidence(tech: dict, tier_level: str) -> float:
    """
    置信度（0.0-1.0），用于signals.json的confidence字段。

    不同于composite_score（相对排序分），confidence是对信号有效性
    的概率估计，有实际含义：0.85表示该信号约有85%的概率延续趋势。

    计算逻辑：
    - 基准分由筛选层级决定（T1质量最高，T4最低）
    - ADX趋势强度加成（ADX>40视为强趋势）
    - 相对强度加成（RS跑赢大盘越多越好）
    - 量能连续性小幅加成
    - 距52周高点越近折扣越小（已接近突破位）
    - 硬上限0.92（无完美信号），硬下限0.50
    """
    base_map = {"T1": 0.85, "T2": 0.75, "T3": 0.65, "T4": 0.55}
    base = base_map.get(tier_level, 0.60)

    # ADX加成：ADX在25-50之间线性加成，最多+0.05
    adx   = tech.get("adx14", 20)
    adx_bonus = min(0.05, max(0.0, (adx - 25) / (50 - 25) * 0.05))

    # RS加成：RS在1.0-1.3之间线性加成，最多+0.05
    rs    = tech.get("rs_vs_xjo", 1.0)
    rs_bonus = min(0.05, max(0.0, (rs - 1.0) / 0.3 * 0.05))

    # 量能连续性加成
    vol_bonus = 0.02 if tech.get("vol_consistency") else 0.0

    # 距52周高点折扣：dist_52w_hi_pct是负数（如-5%表示距高点5%）
    # 越接近高点（绝对值越小）折扣越小
    dist = abs(tech.get("dist_52w_hi_pct", -20))
    dist_penalty = min(0.05, dist / 20 * 0.05)

    confidence = base + adx_bonus + rs_bonus + vol_bonus - dist_penalty
    return round(min(0.92, max(0.50, confidence)), 2)


# ════════════════════════════════════════════════════════════
# JSON字段提取 & signals.json生成 & GitHub推送
# ════════════════════════════════════════════════════════════

def _parse_gemini_json_fields(text: str) -> dict:
    """
    从Gemini分析文本里用正则抓取四个固定字段。
    Gemini被要求在分析末尾输出：
        【JSON_TAG_EN】Bullish Momentum
        【JSON_TAG_ZH】强势突破
        【JSON_ONE_LINER_ZH】一句中文解释
        【JSON_ONE_LINER_EN】One line English explanation

    字段缺失时返回空字符串，不抛异常。
    """
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
            log.warning(f"_parse_gemini_json_fields: [{key}] 未找到，Gemini输出可能不完整")
    return result


def generate_signals_json(signals: list) -> bool:
    """
    从signals列表生成en.json和zh.json并写入~/asxbox/src/data/signals/。

    signals列表里每个元素需包含：
        ticker, confidence, _json_tag_en, _json_tag_zh,
        _json_one_liner_en, _json_one_liner_zh

    有信号才写文件，无信号直接返回False（网站保持上一天数据）。
    """
    if not signals:
        log.info("generate_signals_json: 无信号，跳过写文件")
        return False

    today_str = date.today().isoformat()

    # 构建英文版
    en_payload = {
        "date": today_str,
        "signals": [
            {
                "symbol":     s["ticker"],
                "tag":        s.get("_json_tag_en", ""),
                "confidence": s.get("confidence", 0.60),
                "one_liner":  s.get("_json_one_liner_en", ""),
            }
            for s in signals
        ]
    }

    # 构建中文版
    zh_payload = {
        "date": today_str,
        "signals": [
            {
                "symbol":     s["ticker"],
                "tag":        s.get("_json_tag_zh", ""),
                "confidence": s.get("confidence", 0.60),
                "one_liner":  s.get("_json_one_liner_zh", ""),
            }
            for s in signals
        ]
    }

    try:
        os.makedirs(SIGNALS_DIR, exist_ok=True)
        en_path = os.path.join(SIGNALS_DIR, "en.json")
        zh_path = os.path.join(SIGNALS_DIR, "zh.json")

        with open(en_path, "w", encoding="utf-8") as f:
            json.dump(en_payload, f, ensure_ascii=False, indent=2)
        with open(zh_path, "w", encoding="utf-8") as f:
            json.dump(zh_payload, f, ensure_ascii=False, indent=2)

        log.info(f"signals.json写入成功：{len(signals)} 个信号")
        return True
    except Exception as e:
        log.error(f"generate_signals_json写文件失败: {e}")
        return False


def push_to_github() -> bool:
    """
    将asxbox仓库的变更推送到GitHub。
    只推送src/data/signals/目录，不影响其他文件。
    自动检测默认branch（避免main/master硬编码失败）。
    失败时写log，不中断主流程。
    """
    today_str = date.today().isoformat()
    try:
        # 自动检测默认branch
        r = subprocess.run(
            ["git", "-C", ASXBOX_REPO, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=10
        )
        branch = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else "main"
        log.info(f"push_to_github: 使用branch={branch}")

        cmds = [
            ["git", "-C", ASXBOX_REPO, "add",
             "src/data/signals/en.json",
             "src/data/signals/zh.json"],
            ["git", "-C", ASXBOX_REPO, "commit",
             "-m", f"chore: update signals {today_str}"],
            ["git", "-C", ASXBOX_REPO, "push", "origin", branch],
        ]
        for cmd in cmds:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                if "nothing to commit" in result.stdout + result.stderr:
                    log.info("push_to_github: 无变更，跳过commit")
                    return True
                log.error(
                    f"push_to_github git命令失败: {' '.join(cmd)}\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}"
                )
                return False
            log.info(f"git: {' '.join(cmd[2:])} → OK")

        log.info(f"push_to_github: 推送成功 [{today_str}] branch={branch}")
        return True
    except subprocess.TimeoutExpired:
        log.error("push_to_github: git命令超时（30秒）")
        return False
    except Exception as e:
        log.error(f"push_to_github异常: {e}")
        return False

# ════════════════════════════════════════════════════════════
# 3. 数据获取
# ════════════════════════════════════════════════════════════

def _get(url: str, params: dict = None, label: str = "") -> Optional[dict]:
    """统一GET，所有HTTP错误分类写入log"""
    try:
        r = requests.get(url, params=params, headers=ASX_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        log.error(f"HTTP错误 [{label}] {url}: {e}")
    except requests.ConnectionError as e:
        log.error(f"连接错误 [{label}] {url}: {e}")
    except requests.Timeout:
        log.error(f"超时 [{label}] {url}")
    except Exception as e:
        log.error(f"请求异常 [{label}] {url}: {e}")
    return None


def get_asx_universe() -> list:
    try:
        df  = pd.read_csv(
            "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
            skiprows=1, encoding="latin1",
        )
        col = next((c for c in df.columns if "code" in c.lower()), None)
        if not col:
            log.error("ASX列表列名未找到")
            return []
        codes = df[col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r"^[A-Z]{1,5}$")]
        result = [f"{c}.AX" for c in valid]
        log.info(f"ASX股票池：{len(result)} 只")
        return result
    except Exception as e:
        log.error(f"get_asx_universe失败: {e}")
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
        "sector_leaders": [],
        "xjo_series": None,
    }
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
            drop = (close.resample("W").last().pct_change().iloc[-2:] < -0.05).any()
            if dev < -0.03 or drop:
                snap["market_status"] = "red"
            elif dev < 0:
                snap["market_status"] = "yellow"
            elif pct > 1.0:
                snap["market_status"] = "bullish"
    except Exception as e:
        log.error(f"大盘XJO失败: {e}")

    sector_map = {
        "金融": "^AXFJ", "资源": "^AXMJ", "医疗": "^AXHJ",
        "科技": "^AXIJ", "能源": "^AXEJ", "消费": "^AXSJ",
    }
    changes = []
    for name, sym in sector_map.items():
        try:
            df_s = yf.download(sym, period="5d", interval="1d", progress=False)
            if not df_s.empty and len(df_s) >= 2:
                c = df_s["Close"].squeeze()
                changes.append((name, round((float(c.iloc[-1]) / float(c.iloc[-2]) - 1) * 100, 2)))
        except Exception as e:
            log.warning(f"板块数据失败 [{name}]: {e}")
        time.sleep(0.1)
    snap["sector_leaders"] = sorted(changes, key=lambda x: x[1], reverse=True)[:3]
    log.info(f"大盘: XJO {snap['xjo_change_pct']:+.2f}% 状态:{snap['market_status']}")
    return snap


def get_top_movers(all_data: dict, top_n: int = TOP_N) -> list:
    """从已有K线算当日涨幅，返回Top N。不重新下载。"""
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


def fetch_fundamentals(ticker: str) -> dict:
    try:
        info = yf.Ticker(ticker).info
        return {
            "company_name": info.get("longName", ticker),
            "sector":       info.get("sector", "未知"),
            "industry":     info.get("industry", "未知"),
            "market_cap_m": round(info.get("marketCap", 0) / 1_000_000, 1),
        }
    except Exception as e:
        log.warning(f"fetch_fundamentals失败 [{ticker}]: {e}")
        return {"company_name": ticker, "sector": "未知",
                "industry": "未知", "market_cap_m": 0.0}


def _ann_significance(headline: str, sensitive: bool,
                      doc_type: str, pdf_text: str,
                      pub_date: str) -> int:
    """公告重要性评分（0-10）"""
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
    """判断公告是否为噪音（合规/行政类，对股价无实质影响）"""
    combined = (doc_type + " " + headline).lower()
    return any(kw in combined for kw in ANN_NOISE_KEYWORDS)


# ── 今日公告缓存（进程内有效，避免多次重复请求）──────────────
# ── SQLite公告数据库（本地历史积累）────────────────────────
# ASX没有任何可用的单股历史公告API（经详尽测试确认）。
# 解决方案：每次运行时把今日全市场公告存入本地SQLite。
# 第1天只有今日数据，30天后有30天历史，180天后有完整半年时间线。
# 这比任何第三方API都可靠，因为数据是自己积累的。
ANN_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "announcements.db")

_today_ann_cache: dict = {}


def _init_ann_db() -> None:
    """初始化SQLite公告数据库（首次运行自动建表）"""
    import sqlite3
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS announcements (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol      TEXT    NOT NULL,
                    date        TEXT    NOT NULL,
                    headline    TEXT,
                    sensitive   INTEGER DEFAULT 0,
                    doc_type    TEXT,
                    doc_key     TEXT,
                    pdf_text    TEXT,
                    significance INTEGER DEFAULT 0,
                    UNIQUE(symbol, date, headline)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON announcements(symbol, date)")
            conn.commit()
        log.info(f"公告数据库就绪：{ANN_DB_PATH}")
    except Exception as e:
        log.error(f"公告数据库初始化失败: {e}")


def _save_announcements_to_db(ann_dict: dict) -> None:
    """将今日公告字典批量写入SQLite（IGNORE重复）"""
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
    """从SQLite读取单股近N天历史公告，按significance+date降序"""
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
            {
                "date":        r[0],
                "headline":    r[1],
                "sensitive":   bool(r[2]),
                "doc_type":    r[3],
                "pdf_text":    r[4] or "",
                "significance": r[5],
            }
            for r in rows
        ]
        log.info(f"公告DB [{code}]: {len(result)} 条历史记录（近{days}天）")
        return result
    except Exception as e:
        log.error(f"公告DB读取失败 [{code}]: {e}")
        return []


def fetch_today_announcements() -> dict:
    """
    拉取今日全市场公告，过滤噪音，price-sensitive公告提取PDF。
    结果：① 写入SQLite积累历史 ② 进程内缓存避免重复请求。
    返回：{code: {headline, sensitive, doc_type, documentKey, pdf_text, significance}}
    """
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

            # price-sensitive公告用documentKey下载PDF（每股限1份）
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
                    "headline":    headline,
                    "sensitive":   is_sens,
                    "doc_type":    doc_type,
                    "documentKey": doc_key,
                    "pdf_text":    pdf_txt,
                    "significance": sig,
                }
        if got_old or len(items) < 100:
            break
        page += 1
        time.sleep(0.3)

    # 写入SQLite（积累历史）
    _save_announcements_to_db(result)
    _today_ann_cache[today] = result
    log.info(f"今日公告：{len(result)} 只（有效），PDF {len(pdf_done)} 份")
    return result


def fetch_announcements(code: str,
                        today_ann: Optional[dict] = None) -> list:
    """
    获取单股公告列表（今日 + SQLite历史积累）。

    数据来源说明：
    - ASX单股历史公告API经详尽测试确认不可用（所有端点404或忽略过滤参数）
    - 本函数从本地SQLite读取历史数据（每日自动积累）
    - 第1天：仅有今日数据；运行30天后：有30天历史；180天后：完整半年
    - 今日公告若已在DB中则自动合并，不重复

    返回：按significance+date降序的公告列表（最多20条）
    """
    # 从SQLite读取历史（包含今日已写入的）
    history = _load_announcements_from_db(code, days=180)

    # 若今日公告尚未在DB中（极少数情况：DB写入失败），从内存补充
    if today_ann and code in today_ann:
        today_str = date.today().isoformat()
        already   = any(a["date"] == today_str for a in history)
        if not already:
            ann  = today_ann[code]
            item = {
                "date":        today_str,
                "headline":    ann["headline"],
                "sensitive":   ann["sensitive"],
                "doc_type":    ann.get("doc_type", ""),
                "pdf_text":    ann.get("pdf_text", ""),
                "significance": ann.get("significance", 0),
            }
            history.insert(0, item)
            log.info(f"公告 [{code}]: 今日公告从内存补充（DB未命中）")

    return history



def _extract_pdf_keywords(url: str) -> str:
    """
    关键词段落提取（替代截取前N字符）：
    1. 用pdfplumber提取全文
    2. 找包含PDF_KEY_TERMS的段落
    3. 合并这些段落，截取PDF_MAX_CHARS上限
    比前N字符方法更准确：跳过封面/目录，直接找数字和结论
    """
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=ASX_HEADERS, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "pdf" not in ct.lower() and not url.lower().endswith(".pdf"):
            log.warning(f"_extract_pdf_keywords: 非PDF [{url[:60]}] CT:{ct}")
            return ""

        pages_text = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:12]:   # 最多读12页
                t = page.extract_text()
                if t:
                    pages_text.append(t)

        full_text = "\n".join(pages_text)

        # 按段落切割，提取包含关键词的段落
        paragraphs     = re.split(r"\n{2,}", full_text)
        key_paragraphs = []
        for para in paragraphs:
            para_lower = para.lower()
            hits = sum(1 for kw in PDF_KEY_TERMS if kw in para_lower)
            if hits >= 1 and len(para.strip()) > 30:
                key_paragraphs.append((hits, para.strip()))

        # 按命中数降序排列，取最相关的段落
        key_paragraphs.sort(key=lambda x: x[0], reverse=True)
        extracted = "\n\n".join(p for _, p in key_paragraphs[:8])

        # 清理 + 截取
        extracted = re.sub(r"[ \t]+", " ", extracted).strip()
        if len(extracted) > PDF_MAX_CHARS:
            extracted = extracted[:PDF_MAX_CHARS] + "\n...[截断]"

        if not extracted:
            log.debug(f"PDF无关键词命中，返回前500字符 [{url[:60]}]")
            return full_text[:500]

        log.debug(f"PDF关键段落提取成功 [{url[:60]}]: {len(extracted)} 字符")
        return extracted

    except requests.HTTPError as e:
        log.error(f"PDF下载HTTP错误 [{url[:60]}]: {e}")
    except requests.ConnectionError as e:
        log.error(f"PDF下载连接错误 [{url[:60]}]: {e}")
    except requests.Timeout:
        log.error(f"PDF下载超时 [{url[:60]}]")
    except Exception as e:
        log.error(f"PDF提取失败 [{url[:60]}]: {e}")
    return ""


def fetch_news(ticker: str, company_name: str = "") -> list:
    """Google RSS + yfinance双源，去重，最多NEWS_MAX条"""
    code   = ticker.replace(".AX", "")
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    raw    = []

    for q in [f"ASX:{code}", f"{company_name} ASX" if company_name else f"{code} ASX Australia"]:
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
        except requests.HTTPError as e:
            log.error(f"Google RSS HTTP错误 [{q}]: {e}")
        except requests.ConnectionError as e:
            log.error(f"Google RSS 连接错误 [{q}]: {e}")
        except ET.ParseError as e:
            log.error(f"Google RSS XML解析错误 [{q}]: {e}")
        except Exception as e:
            log.error(f"Google RSS 未知错误 [{q}]: {e}")
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
    """
    构建精选时间线文本：
    - 公告按significance评分排序，优先显示高价值公告
    - 公告标注significance分和是否price-sensitive
    - ★ price-sensitive公告若有PDF关键段落，附在标题下方
    - 新闻和公告合并，按日期降序
    """
    events = []

    for a in announcements:
        sig  = a.get("significance", 0)
        flag = "⭐" if a["sensitive"] else "📋"
        sig_label = f"[重要度:{sig}]" if sig >= 5 else ""
        line = f"{flag}{sig_label}[公告] {a['headline']}"

        # PDF关键段落附加在公告标题下方（缩进显示，与新闻区分）
        pdf_txt = a.get("pdf_text", "")
        if pdf_txt:
            line += f"\n    📄PDF关键内容: {pdf_txt[:400]}"

        events.append({
            "date": a["date"],
            "text": line,
            "sort_key": (a["date"], sig),
        })

    for n in news:
        events.append({
            "date": n["date"],
            "text": f"📰[新闻] {n['title']} ({n['source']})",
            "sort_key": (n["date"], 0),
        })

    if today_ann and code in today_ann:
        ta   = today_ann[code]
        flag = "⭐" if ta["sensitive"] else "📋"
        events.append({
            "date": date.today().isoformat(),
            "text": f"{flag}[今日公告] {ta['headline']}",
            "sort_key": (date.today().isoformat(), 10),
        })

    seen, lines = set(), []
    for e in sorted(events, key=lambda x: x["sort_key"], reverse=True):
        key = e["date"] + e["text"][:50]
        if key not in seen:
            seen.add(key)
            lines.append(f"{e['date']}  {e['text']}")

    return "\n".join(lines[:20]) if lines else "暂无近期公告/新闻"

# ════════════════════════════════════════════════════════════
# 4. Gemini（仅选股分析调用）
# ════════════════════════════════════════════════════════════

def ask_gemini(prompt: str, label: str = "") -> str:
    if not gemini_client:
        log.warning("Gemini未配置")
        return ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL, contents=prompt, config=GEMINI_CFG_DEEP
            )
            if attempt > 1:
                log.info(f"Gemini成功 [{label}] 第{attempt}次")
            return resp.text.strip()
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("429", "503", "RESOURCE_EXHAUSTED", "overloaded", "quota")):
                if attempt < RETRY_MAX:
                    log.warning(f"Gemini限速 [{label}] {attempt}/{RETRY_MAX}，{RETRY_WAIT}s后重试...")
                    time.sleep(RETRY_WAIT)
                else:
                    log.error(f"Gemini [{label}] 达到10分钟上限，放弃")
                    return ""
            else:
                log.error(f"Gemini不可重试错误 [{label}]: {err}")
                return ""
    return ""

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
        try:
            r = requests.post(url, json={
                "chat_id": CHAT_ID, "text": chunk,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=10)
            r.raise_for_status()
        except requests.HTTPError as e:
            log.error(f"Telegram HTTP错误: {e}")
        except Exception as e:
            log.error(f"Telegram发送失败: {e}")
        time.sleep(0.5)



def send_document(filename: str, content: str, caption: str = "") -> None:
    """
    通过Telegram sendDocument接口发送.txt文件附件。
    用于发送日报Prompt——内容太长不适合直接贴消息，
    手机端可直接下载文件，复制粘贴给AI生成文章。
    """
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过send_document")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        r = requests.post(
            url,
            data={"chat_id": CHAT_ID, "caption": caption},
            files={"document": (filename, content.encode("utf-8"), "text/plain")},
            timeout=30,
        )
        r.raise_for_status()
        log.info(f"文件发送成功: {filename} ({len(content)} 字符)")
    except requests.HTTPError as e:
        log.error(f"send_document HTTP错误 [{filename}]: {e}")
    except Exception as e:
        log.error(f"send_document失败 [{filename}]: {e}")

# ════════════════════════════════════════════════════════════
# 6. Prompt构建
# ════════════════════════════════════════════════════════════

def _build_screener_prompt(signal: dict, timeline: str,
                           tier_label: str) -> str:
    """选股深度分析Prompt（Gemini调用用）"""
    t = signal
    ma200_str    = f"MA200:{t['ma200']}" if t.get("ma200") else "MA200:数据不足"
    vol_c_str    = "✅ 近5日量能逐步递增" if t.get("vol_consistency") else "量能无持续性"
    pct_1y_str   = f"{t.get('price_pct_1y', 50)}%分位（1年历史）"

    # 价格事件文本
    pe = t.get("price_events", [])
    pe_str = "\n".join(
        f"  {e['date']} 单日{'+' if e['change_pct'] > 0 else ''}{e['change_pct']}%"
        for e in pe[:6]
    ) if pe else "  近6个月无±5%单日大幅波动"

    tech_block = (
        f"价格:{t['price']}({t['change_pct']:+.2f}%) | "
        f"1年历史分位:{pct_1y_str} | {vol_c_str}\n"
        f"MA20:{t['ma20']} MA50:{t['ma50']} {ma200_str}\n"
        f"RSI:{t['rsi14']} ADX:{t['adx14']} +DI:{t['plus_di']} -DI:{t['minus_di']}\n"
        f"VWAP20:{'上升' if t['vwap_up'] else '下降'} 量比:{t['vol_ratio']}x\n"
        f"RS(vsXJO):{t['rs_vs_xjo']} ATR:{t['atr14_pct']}% "
        f"52W高:{t['w52_hi']}(距{t['dist_52w_hi_pct']}%) 低:{t['w52_lo']}\n"
        f"近6月最大回撤:{t['max_dd_6m_pct']}%\n"
        f"综合评分:{t.get('composite_score', 'N/A')}"
    )

    return f"""你是一位专注ASX市场的资深机构分析师。今天是{date.today().isoformat()}。

===== 分析标的 =====
{t['ticker']} | 筛选等级:{tier_label} | 综合评分:{t.get('composite_score','N/A')}
{t.get('company_name','未知')} ({t.get('sector','未知')}/{t.get('industry','未知')}) 市值:{t.get('market_cap_m',0)}M AUD

===== 技术指标（1年数据）=====
{tech_block}

===== 近6个月单日大幅波动节点 =====
{pe_str}

===== 精选新闻/公告时间线（已过滤噪音，按重要度排序）=====
{timeline}

===== 分析任务 =====
请严格按以下4部分输出，每部分2-3句，语言精炼专业：

【技术形态】结合1年历史分位和量能连续性，评估当前突破质量和支撑压力位。

【事件驱动分析】对照价格波动节点和时间线，找出最重要的1-2个催化剂事件，
判断市场是否已充分定价。

【催化剂预测】基于公告周期（季报/年报/项目进展规律），
预测未来4-8周最可能的催化剂类型和时间窗口。

【综合结论】给出买入/观望/回避建议，说明止损位（基于ATR或关键支撑），
以及最值得关注的一个上行/下行风险。

规则：不确定内容标注"需进一步核查"，禁止编造数据。

===== 固定输出字段（必须在分析末尾严格按格式输出，不得省略）=====
【JSON_TAG_EN】（英文信号标签，2-4个词，如：Bullish Momentum / Range Break Setup / Overbought Pressure）
【JSON_TAG_ZH】（中文信号标签，2-4个字，如：强势突破 / 区间试探 / 超买压力）
【JSON_ONE_LINER_ZH】（一句中文核心解释，≤25字，描述当前技术或事件驱动的关键状态）
【JSON_ONE_LINER_EN】（One English sentence, ≤20 words, same meaning as ZH above）"""


def _build_report_stock_block(ticker: str, tech: dict,
                               fund: dict, timeline: str,
                               pdf_texts: list,
                               rank: int) -> str:
    """日报Prompt中单只股票的数据块（包含全部材料）"""
    ma200_str  = f"MA200:{tech['ma200']}" if tech.get("ma200") else "MA200:N/A"
    vol_c_str  = "量能连续递增✅" if tech.get("vol_consistency") else "量能无持续性"
    pct_1y_str = f"{tech.get('price_pct_1y', 50)}%分位(1年)"

    pe = tech.get("price_events", [])
    pe_str = " | ".join(
        f"{e['date']}:{'+' if e['change_pct'] > 0 else ''}{e['change_pct']}%"
        for e in pe[:5]
    ) if pe else "近6个月无大幅波动"

    tech_line = (
        f"价格:{tech['price']}({tech['change_pct']:+.2f}%) {pct_1y_str} {vol_c_str}\n"
        f"MA50:{tech['ma50']} {ma200_str} RSI:{tech['rsi14']} ADX:{tech['adx14']} "
        f"量比:{tech['vol_ratio']}x RS:{tech['rs_vs_xjo']} "
        f"ATR:{tech['atr14_pct']}% 52W高:{tech['w52_hi']}(距{tech['dist_52w_hi_pct']}%)\n"
        f"近6月最大回撤:{tech['max_dd_6m_pct']}% | 综合评分:{tech.get('composite_score','N/A')}\n"
        f"大涨大跌节点：{pe_str}"
    )

    block = (
        f"\n{'='*50}\n"
        f"#{rank} {ticker} | {fund.get('company_name', ticker)}\n"
        f"板块:{fund.get('sector','未知')} | 市值:{fund.get('market_cap_m',0)}M AUD\n"
        f"{'='*50}\n"
        f"【技术面数据】\n{tech_line}\n\n"
        f"【精选新闻/公告时间线（含PDF关键段落，已过滤噪音）】\n{timeline}"
    )

    # 日报场景PDF需要完整版（不截断），供你写文章引用原文数据。
    # timeline里已嵌入400字符精简版供AI快速参考；这里补充完整全文。
    for i, txt in enumerate(pdf_texts[:PDF_MAX_PER_STOCK], 1):
        if len(txt) > 400:   # 只有完整版比timeline里的摘要更长时才补充
            block += f"\n\n【价格敏感公告完整原文#{i}（供撰文引用）】\n{txt}"

    return block


def serialize_to_prompt(market_snap: dict, stocks_block: str,
                        platform: str) -> str:
    """
    生成最终Prompt文本，直接发Telegram给你。
    你收到后复制给AI生成文章，不经过Gemini API。
    """
    sector_str = "、".join(
        f"{s}({p:+.1f}%)" for s, p in market_snap.get("sector_leaders", [])
    ) or "数据暂缺"
    status_map = {
        "red": "大幅下跌⚠️", "yellow": "轻微走弱",
        "bullish": "强势上涨", "normal": "窄幅震荡",
    }
    market_block = (
        f"日期：{market_snap.get('date', date.today().isoformat())}\n"
        f"ASX200：{market_snap.get('xjo_close',0)} "
        f"({market_snap.get('xjo_change_pct',0):+.2f}%，"
        f"{status_map.get(market_snap.get('market_status','normal'),'正常')})\n"
        f"今日领涨板块：{sector_str}"
    )

    instructions = {
        "seo": """生成**中文** SEO网页文章（占位说明，后续细化）：撰写一篇面向搜索引擎的ASX市场分析长文，结构化标题+小标题，自然融入关键词，内容覆盖今日市场概况和精选股票分析。""",

        "twitter": """Generate English Twitter/X thread (use ---TWEET--- between tweets):
Tweet 1(hook ≤250): market summary
Tweets 2-4: one per stock — $TICKER + % move + catalyst (1 line) + Buy/Watch/Avoid
Tweet 5: outlook + #ASX #AustralianStocks + disclaimer
No fabrication. Flag uncertainty as "to verify".""",

        "xiaohongshu": """生成**中文**小红书投资笔记：
标题（≤20字，含关键数据）→ 开头钩子(2句) → 每只股票用"为什么关注XX"叙事(核心逻辑+技术直觉+我的判断) → 今日投资启示 → 话题标签(#澳股 #ASX投资等3-5个)
要求：专业但亲切，不确定注明"待核查"，末尾加免责声明""",
    }

    instruction = instructions.get(platform, instructions["seo"])

    return f"""📋 <b>ASX日报Prompt — {platform.upper()} — {market_snap.get('date','')}</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>👇 复制以下全部内容给AI生成文章</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

你是一位专注澳大利亚股市(ASX)的资深投资分析师。

=== 今日市场数据 ===
{market_block}

=== Top 3 精选股票数据包（含技术面+新闻+公告+PDF关键段落）===
{stocks_block}

=== 输出任务 ===
{instruction}

规则：数据来自公开渠道，不构成投资建议。数据矛盾时优先级：公告原文>新闻>技术指标。禁止重复罗列原始数字，转化为判断语言。
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

# ════════════════════════════════════════════════════════════
# 7. 选股筛选
# ════════════════════════════════════════════════════════════

def _passes_tier(tech: dict, tier: dict) -> bool:
    lc        = tech["price"]
    vol_ratio = tech["vol_ratio"]
    close_pos = tech["close_pos_pct"] / 100.0
    w52_hi    = tech["w52_hi"]
    volume_s  = tech["_volume"]
    high_s    = tech["_high"]
    low_s     = tech["_low"]

    if lc < tech["ma50"] or not tech["ma50_up"]:
        return False
    if float(volume_s.iloc[-20:].mean()) * lc < 300_000:
        return False

    r15 = pd.concat([high_s, low_s], axis=1).iloc[-15:]
    pr  = (float(r15.iloc[:, 0].max()) - float(r15.iloc[:, 1].min())) / lc
    if pr > tier["consol"]:
        return False

    if tier["vol_decline"]:
        if float(volume_s.iloc[-10:].mean()) >= float(volume_s.iloc[-20:-10].mean()):
            return False

    if tier["near_52w_hi"] and lc < w52_hi * 0.90:
        return False
    if tech["adx14"] < tier["adx_min"]:
        return False
    if tier["di_cross"] and tech["plus_di"] <= tech["minus_di"]:
        return False
    if tier["vwap_above"] and (lc < tech["vwap20"] or not tech["vwap_up"]):
        return False
    if tech["rs_vs_xjo"] < tier["rs_min"]:
        return False
    if vol_ratio < tier["vol_mult"]:
        return False
    if not (tier["rsi_lo"] <= tech["rsi14"] <= tier["rsi_hi"]):
        return False
    if close_pos < tier["close_pos"]:
        return False
    return True


def run_screener_flow(all_data: dict, market_snap: dict) -> list:
    """
    T1→T4分级筛选 → 加权评分 → Top3。
    返回最终Top3 signals列表（供日报复用）。
    """
    today  = date.today().strftime("%Y-%m-%d")
    start  = time.time()
    status = market_snap.get("market_status", "normal")
    xjo_s  = market_snap.get("xjo_series")
    xjo_pct = market_snap.get("xjo_change_pct", 0)

    market_note  = "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。" if status == "yellow" else ""
    market_label = "⚠️ " if status == "yellow" else ""

    today_ann = fetch_today_announcements()

    # T1→T4全部筛选，合并排序取Top3
    # 每只股票记录通过的最高层级（T1>T2>T3>T4），避免同一股票重复出现
    # 这样既保证质量（评分高的排前面），又保证数量（不会因T1无信号就停）
    log.info("分级筛选（T1-T4全部扫描，合并排序）...")
    seen_tickers = {}   # {ticker: signal_dict}，只保留最高层级

    for tier in TIERS:
        log.info(f"  {tier['level']} ({tier['label']})...")
        count = 0
        for ticker, df in all_data.items():
            if ticker in seen_tickers:
                continue   # 已被更高层级收录，跳过
            try:
                if len(df) < 60:
                    continue
                tech = build_tech_summary(df, xjo_s)
                if _passes_tier(tech, tier):
                    tech["ticker"]     = ticker
                    tech["tier_level"] = tier["level"]
                    tech["tier_label"] = tier["label"]
                    seen_tickers[ticker] = tech
                    count += 1
            except Exception as e:
                log.debug(f"筛选异常 [{ticker}]: {e}")
        log.info(f"    → 本层新增 {count} 个（累计 {len(seen_tickers)} 个）")

    raw_signals = list(seen_tickers.values())
    elapsed_screen = round((time.time() - start) / 60, 1)

    if not raw_signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只（T1-T4均无信号）\n"
            f"市场动能不足，建议观望。耗时：{elapsed_screen}分钟{market_note}"
        )
        return []

    # 加权评分 + Top3（跨层级统一排序，T1信号评分天然高于T4）
    for s in raw_signals:
        s["composite_score"] = calc_composite_score(s)
    raw_signals.sort(key=lambda x: x["composite_score"], reverse=True)
    raw_signals = raw_signals[:10]  # 先取Top10做基本面过滤

    signals = []
    for s in raw_signals:
        fund = fetch_fundamentals(s["ticker"])
        if fund.get("market_cap_m", 0) * 1e6 < 50_000_000:
            log.debug(f"市值过滤 [{s['ticker']}]")
            continue
        s.update(fund)
        s["entry_limit"] = round(s["price"] * 1.02, 3)
        s["stop_loss"]   = round(s["price"] * 0.90, 3)
        s["take_profit"] = round(s["price"] * 1.20, 3)
        signals.append(s)
        if len(signals) == TOP_N:
            break

    # 汇总用的层级标签：显示各信号来自哪个层级
    tier_summary = {}
    for s in signals:
        lv = s.get("tier_level", "T?")
        tier_summary[lv] = tier_summary.get(lv, 0) + 1
    tier_label = " / ".join(f"{lv}×{n}" for lv, n in sorted(tier_summary.items()))
    tier_level = signals[0].get("tier_level", "T?") if signals else "T?"

    # 汇总消息
    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"ASX200：{xjo_pct:+.2f}%  |  扫描：{len(all_data)} 只  |  耗时：{elapsed_screen}分钟\n"
        f"层级分布：{tier_label}  |  精选 Top {len(signals)} 只\n\n"
        + "\n".join(
            f"#{i+1} {s['ticker']} | [{s.get('tier_level','?')}] 评分:{s['composite_score']} "
            f"RS:{s['rs_vs_xjo']} ADX:{s['adx14']} 量比:{s['vol_ratio']}x"
            for i, s in enumerate(signals)
        )
        + market_note
    )

    # 写入长期监测队列（供 intraday_monitor.py 使用）
    wdb.init_watchlist_db()
    for s in signals:
        wdb.upsert_watchlist(
            ticker=s["ticker"],
            company_name=s.get("company_name", s["ticker"]),
            tier_level=s.get("tier_level", tier_level),
            tier_label=s.get("tier_label", tier_label),
            composite_score=s["composite_score"],
        )

    # Top3逐只Gemini深度分析
    log.info(f"深度分析 Top {len(signals)} 只（Gemini）...")
    for idx, s in enumerate(signals, 1):
        code = s["ticker"].replace(".AX", "")
        log.info(f"  [#{idx}] {s['ticker']} 评分:{s['composite_score']}...")

        ann_hist = fetch_announcements(code, today_ann=today_ann)
        news     = fetch_news(s["ticker"], s.get("company_name", ""))
        timeline = build_timeline_text(code, ann_hist, news, today_ann)

        prompt   = _build_screener_prompt(s, timeline, s.get("tier_label", tier_label))
        analysis = ask_gemini(prompt, label=s["ticker"])
        if not analysis:
            analysis = "⚠️ Gemini分析暂时不可用"

        # ── 抓取JSON字段（从Gemini输出末尾提取）────────────────
        json_fields = _parse_gemini_json_fields(analysis)
        s["_json_tag_en"]       = json_fields.get("tag_en", "")
        s["_json_tag_zh"]       = json_fields.get("tag_zh", "")
        s["_json_one_liner_zh"] = json_fields.get("one_liner_zh", "")
        s["_json_one_liner_en"] = json_fields.get("one_liner_en", "")
        s["confidence"]         = calc_confidence(s, s.get("tier_level", tier_level))
        # Gemini失败时JSON字段全空，标记该股票不写入signals.json
        s["_json_valid"] = bool(
            s["_json_tag_en"] and s["_json_one_liner_en"]
        )
        log.info(
            f"  JSON字段 [{s['ticker']}]: tag_en={s['_json_tag_en']!r} "
            f"confidence={s['confidence']} valid={s['_json_valid']}"
        )

        ann_info = today_ann.get(code, {})
        ann_line = ""
        if ann_info:
            flag     = "⭐ " if ann_info["sensitive"] else ""
            ann_line = f"\n📋 今日公告：{flag}{ann_info['headline']}"

        ma200_str   = f" MA200:${s['ma200']}" if s.get("ma200") else ""
        vol_c_badge = " 📈量能连续" if s.get("vol_consistency") else ""
        s_tier_label = s.get("tier_label", tier_label)
        send_telegram(
            f"<b>#{idx} {s_tier_label} {s.get('company_name', s['ticker'])}</b> ({s['ticker']})\n"
            f"📅 {today} | {s.get('sector','未知')} | 市值:${s.get('market_cap_m',0)}M | "
            f"综合评分:{s['composite_score']}\n\n"
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

    # ── 生成signals.json并推送GitHub ────────────────────────────
    # 只写入JSON字段有效的信号（Gemini成功生成了tag和one_liner）
    # 无有效信号时网站保持上一天数据，不推送
    valid_signals = [s for s in signals if s.get("_json_valid")]
    log.info(f"生成signals.json：{len(valid_signals)}/{len(signals)} 只有效JSON字段...")
    written = generate_signals_json(valid_signals)
    if written:
        pushed = push_to_github()
        if pushed:
            send_telegram(
                f"🌐 <b>网站已更新</b> {today}\n"
                f"signals.json已推送GitHub，Cloudflare正在重建。\n"
                f"信号数量：{len(signals)} 只"
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

# ════════════════════════════════════════════════════════════
# 8. 日报Prompt流程（不调用Gemini）
# ════════════════════════════════════════════════════════════

def run_report_flow(all_data: dict, market_snap: dict,
                    screener_signals: Optional[list] = None) -> None:
    """
    日报Prompt生成：
    - 优先使用screener已选出的Top3（避免重复数据抓取）
    - 若screener未运行（大盘红灯），改用涨幅Top3 Movers
    - 全程不调用Gemini
    """
    today  = date.today().isoformat()
    xjo_s  = market_snap.get("xjo_series")
    today_ann = fetch_today_announcements()

    log.info("=== 日报Prompt流程启动 ===")

    # 确定目标股票：优先screener结果，其次涨幅Movers
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

    # 逐只构建完整数据包
    stock_blocks = []
    for rank, ticker in enumerate(target_tickers, 1):
        code = ticker.replace(".AX", "")
        df   = all_data.get(ticker)
        if df is None:
            log.warning(f"日报：无K线数据 [{ticker}]，跳过")
            continue

        log.info(f"  [#{rank}] {ticker} 构建日报数据包...")

        # 技术面（若screener已算则复用，否则重新计算）
        existing = next((s for s in (screener_signals or []) if s["ticker"] == ticker), None)
        tech = existing if existing else build_tech_summary(df, xjo_s)
        if "composite_score" not in tech:
            tech["composite_score"] = calc_composite_score(tech)

        fund = existing if (existing and existing.get("company_name")) else fetch_fundamentals(ticker)

        # 历史公告（含PDF关键段落提取）+ 新闻
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

    # 先发一条摘要通知（短消息，不会被分割）
    tickers_str = " / ".join(target_tickers)
    send_telegram(
        f"📂 <b>ASX日报Prompt就绪 {today}</b>\n\n"
        f"股票：{tickers_str}\n"
        f"以下3个文件已发送，复制文件内容给AI生成文章👇"
    )

    # 每个平台发一个.txt附件（不分割，手机直接下载复制）
    for platform in ["seo", "twitter", "xiaohongshu"]:
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
    log.info(f"ASX System v14 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    log.info("=" * 60)

    # Step 1：大盘快照
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

    # Step 2：全市场K线（一次下载，两个流程共享）
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

    # Step 3：选股流程（大盘红灯时跳过）
    screener_signals = []
    if status != "red":
        log.info("【Step 3】选股筛选流程...")
        screener_signals = run_screener_flow(all_data, market_snap)
    else:
        log.info("【Step 3】大盘红灯，跳过选股")

    # Step 4：日报Prompt（始终运行，复用screener结果）
    log.info("【Step 4】日报Prompt生成流程...")
    run_report_flow(all_data, market_snap,
                    screener_signals=screener_signals if screener_signals else None)

    elapsed = round((time.time() - start) / 60, 1)
    log.info("=" * 60)
    log.info(f"ASX System 全部完成，总耗时：{elapsed} 分钟")
    log.info("=" * 60)
    send_telegram(
        f"🏁 <b>ASX System v14 全部完成</b>\n"
        f"📅 {date.today().isoformat()} | ⏱ 总耗时：{elapsed} 分钟\n"
        f"选股：{'跳过（大盘红灯）' if status == 'red' else f'Top{len(screener_signals)}已完成'}\n"
        f"日报Prompt：SEO / Twitter / 小红书 已发送"
    )


if __name__ == "__main__":
    main()
