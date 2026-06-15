# ============================================================
# ASX SYSTEM — screener.py  v13
#
# 流程一：EOD选股
#   全市场K线 → T1-T4筛选 → Top3加权评分 → 新闻/公告时间线
#   → Gemini深度分析 → Telegram
#
# 流程二：每日日报Prompt（不调用Gemini）
#   Top3 Movers → 技术面+精选新闻+公告+PDF关键段落
#   → 构建三平台Prompt → Telegram
#
# v13改进：
#   - 加权综合评分排序，Top3精选
#   - 公告白名单过滤（保留有意义类型，过滤合规噪音）
#   - PDF关键词段落提取（替代截取前N字符）
#   - 新增price_percentile_1y / vol_consistency / price_events
#   - 公告significance评分
#   - 所有网络错误分类写入log
# ============================================================

import os, io, re, sys, time, logging
import xml.etree.ElementTree as ET
import requests
import yfinance as yf
import pandas as pd
import pdfplumber
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
    - 新闻和公告合并，按日期降序
    """
    events = []

    for a in announcements:
        sig  = a.get("significance", 0)
        flag = "⭐" if a["sensitive"] else "📋"
        sig_label = f"[重要度:{sig}]" if sig >= 5 else ""
        events.append({
            "date": a["date"],
            "text": f"{flag}{sig_label}[公告] {a['headline']}",
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

规则：不确定内容标注"需进一步核查"，禁止编造数据。"""


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
        f"【精选新闻/公告时间线（已过滤噪音）】\n{timeline}"
    )

    # PDF关键段落（仅添加已提取的）
    for i, txt in enumerate(pdf_texts[:PDF_MAX_PER_STOCK], 1):
        block += f"\n\n【价格敏感公告关键内容#{i}（关键词段落提取）】\n{txt}"

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
        "telegram": """生成中文 ASX 日交易简报（专业交易员 + 事件驱动 + 时间线叙事）
━━━━━━━━━━━━━━━━━━
【核心目标】
基于输入的：
- 个股技术指标（必须充分使用）
- 大盘/市场情绪指标（必须纳入判断）
- 最新 + 历史公告/新闻 + 历史数据
输出：
👉 “市场状态 + 个股演化时间线 + 当前阶段 + 下一步推演”
本质：交易叙事重建，而不是信息总结。
━━━━━━━━━━━━━━━━━━
【市场情绪识别（必须先做）】
必须先判断整体市场属于：
- Risk-on（风险偏好上升）
- Risk-off（避险）
- Rotation（板块轮动）
- Choppy（震荡无趋势）
判断依据必须来自输入数据：
- 大盘走势/成交量
- 关键指数技术结构
- 个股整体涨跌分布
并在开头体现。
━━━━━━━━━━━━━━━━━━
【结构】
1. 开头（2–3句）
- 市场情绪判断（必须明确）
- 当前主线资金方向
- 一个结构性预期差或风险点
━━━━━━━━━━━━━━━━━━
2. 个股
每只必须构建“时间线故事”：
━━━━━━━━━━━━━━━━━━
【A. 时间线叙事（必须）】
必须包含：
- 过去（历史公告/旧新闻）
- 中间（市场反应/价格变化）
- 现在（最新公告/异动）
- 当前阶段判断（重估 / 兑现 / 加速 / 破位 / 反转）
要求：
→ 必须形成因果链（事件 → 反应 → 结构变化）
━━━━━━━━━━━━━━━━━━
【B. 技术面（必须高密度使用）】
必须使用3–6个指标组合：
- 趋势结构（上升/下降/震荡）
- 均线体系（MA20/50/200）
- 动量（RSI / MACD）
- 成交量变化
- 支撑/阻力
- 波动/突破结构
要求：
- 必须解释“指标意味着什么”
- 技术面必须服务于“阶段判断”
━━━━━━━━━━━━━━━━━━
【C. 驱动逻辑】
- 为什么市场在“此刻”重新定价
- 公告/新闻如何改变预期
━━━━━━━━━━━━━━━━━━
【D. 下一步推演】
必须包含：
- 1–3个催化剂
- 成功路径 → 结果
- 失败路径 → 风险结构
━━━━━━━━━━━━━━━━━━
【E. 我的判断】
- 倾向观察 / 倾向参与 / 倾向回避
禁止强投资指令
每只≤140字
━━━━━━━━━━━━━━━━━━
【风格随机】
A 机构分析师（结构化）
B 交易员复盘（节奏感）
C 事件叙事（故事化）
━━━━━━━━━━━━━━━━━━
【核心强制规则】
- 必须市场情绪 + 个股情绪一致性分析
- 必须时间线（过去→现在→未来）
- 必须因果链（公告→资金→技术→阶段）
- 必须至少1个预期差或反直觉点
━━━━━━━━━━━━━━━━━━
【免责声明】
末尾加上
⚠️仅为市场分析，不构成投资建议""",

        "twitter": """Generate an English ASX trading thread for X (Twitter)
━━━━━━━━━━━━━━━━━━
You are a discretionary ASX trader writing a daily X (Twitter) thread.

OUTPUT FORMAT RULE:
- Every tweet must be wrapped in a code block using triple backticks
- No extra commentary outside code blocks
- No labels like "Tweet 1"
- Only raw content inside each block

CORE GOAL:
Human-like trading notes with fragmented thinking, not analysis.

HARD RULES:
- One idea per tweet
- No structured explanations
- No report language
- No symmetry between tweets
- No repeated phrasing patterns
- No academic tone

MANDATORY HUMAN SIGNALS:
- 1 self-doubt sentence total in the whole thread (NOT every tweet)
- 1 incomplete thought somewhere
- 1 emotional reaction somewhere
- At least 1 subtle contradiction across tweets

STYLE:
- Fragmented
- Uneven logic
- Mixed conviction
- Slight inconsistency allowed

STRUCTURE:
5–7 tweets only.

Tweet 1: Hook (market + contradiction)
Tweet 2: Market context
Tweet 3–5: Stocks (one idea each)
Tweet 6 (optional): outlook

STOCK FORMAT:
Ticker + move
→ single driver
→ reaction
→ stance

ANTI-AI RULES:
- Avoid "suggests / indicates / therefore / because"
- Avoid full reasoning chains
- Prefer incomplete thoughts

OUTPUT RULE:
Each tweet must be in its own code block""",

        "xiaohongshu": """生成中文小红书 ASX交易复盘笔记（交易叙事日记风格）
━━━━━━━━━━━━━━━━━━
你是ASX短线交易者，每天做盯盘记录，不是写报告。

目标：
用自然人话记录今天市场 + 3个盲盒（“盲盒”指代“股票”） + 真实交易决策。

━━━━━━━━━━━━━━
整体原则
- 不写分析报告
- 不强制模板句
- 所有结构只提供“方向提示”，不做固定句式
- 允许真实交易结果：看好/看空/买入/卖出/持有/错过

━━━━━━━━━━━━━━
标题（提示，不是模板）
需要表达：
- 今天的情绪冲突或交易状态
- 可以是看懂/看不懂/做对/做错/没动/差点进

但：
- 不限制必须用某几个词
- 不限制结构

━━━━━━━━━━━━━━
开头（提示）
需要包含：
- 今天市场整体感受（情绪化）
- 今天看了3个盲盒
- 可有轻微吐槽或主观感受

不要求固定句式

━━━━━━━━━━━━━━
过渡句（提示）
只需要一个自然切换，从闲聊到认真分析，不要求固定表达，必须说人话

━━━━━━━━━━━━━━
每个盲盒（核心）

每个盲盒包含4个信息，但表达方式自由：

1. （可选）你的第一感觉（可以是好/坏/犹豫/兴奋/无感）
2. 发生了什么（盘面 / 新闻 / 变化）（必须包含历史新闻和现在新闻）
3. 你的真实交易行为（重点：必须真实）

交易行为允许全部类型，包括但不限于：
- 看好直接买入
- 看好但等待回调
- 回调加仓
- 轻仓试探
- 已持有继续拿
- 止盈/止损离场
- 想买但没敢
- 错过
- 继续观察
- 放弃

4. 可选：你的正向预测（短线或中线均可，不强制）

说明：
- 不要求每只股票都有正向预测
- 不要求必须扩展逻辑
- 只在你自然想补充时写

━━━━━━━━━━━━━━
结尾（提示）
写一个当天交易感受或认知总结：
- 可以是经验
- 可以是反思
- 可以是情绪总结
- 不要求统一方向

━━━━━━━━━━━━━━
风格要求
- 人话优先
- 短句优先
- 允许情绪
- 不要报告感
- 不要固定模板句
- 不要强制关键词
━━━━━━━━━━━━━━━━━━
【标签】
#澳股 #ASX（3–5个）
末尾加上
⚠️仅个人记录，不构成投资建议""",
    }

    instruction = instructions.get(platform, instructions["telegram"])

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

    # T1→T4筛选
    log.info("分级筛选（T1→T4）...")
    found_tier, raw_signals = None, []
    for tier in TIERS:
        log.info(f"  {tier['level']} ({tier['label']})...")
        tier_sigs = []
        for ticker, df in all_data.items():
            try:
                if len(df) < 60:
                    continue
                tech = build_tech_summary(df, xjo_s)
                if _passes_tier(tech, tier):
                    tech["ticker"] = ticker
                    tier_sigs.append(tech)
            except Exception as e:
                log.debug(f"筛选异常 [{ticker}]: {e}")
        log.info(f"    → {len(tier_sigs)} 个")
        if tier_sigs:
            found_tier, raw_signals = tier, tier_sigs
            break

    elapsed_screen = round((time.time() - start) / 60, 1)

    if not raw_signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只（T1-T4均无信号）\n"
            f"市场动能不足，建议观望。耗时：{elapsed_screen}分钟{market_note}"
        )
        return []

    # 加权评分 + Top3
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

    tier_label = found_tier["label"]
    tier_level = found_tier["level"]

    # 汇总消息
    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"ASX200：{xjo_pct:+.2f}%  |  扫描：{len(all_data)} 只  |  耗时：{elapsed_screen}分钟\n"
        f"信号等级：{tier_label}  |  精选 Top {len(signals)} 只\n\n"
        + "\n".join(
            f"#{i+1} {s['ticker']} | 评分:{s['composite_score']} "
            f"RS:{s['rs_vs_xjo']} ADX:{s['adx14']} 量比:{s['vol_ratio']}x"
            for i, s in enumerate(signals)
        )
        + market_note
    )

    # Top3逐只Gemini深度分析
    log.info(f"深度分析 Top {len(signals)} 只（Gemini）...")
    for idx, s in enumerate(signals, 1):
        code = s["ticker"].replace(".AX", "")
        log.info(f"  [#{idx}] {s['ticker']} 评分:{s['composite_score']}...")

        ann_hist = fetch_announcements(code, today_ann=today_ann)
        news     = fetch_news(s["ticker"], s.get("company_name", ""))
        timeline = build_timeline_text(code, ann_hist, news, today_ann)

        prompt   = _build_screener_prompt(s, timeline, tier_label)
        analysis = ask_gemini(prompt, label=s["ticker"])
        if not analysis:
            analysis = "⚠️ Gemini分析暂时不可用"

        ann_info = today_ann.get(code, {})
        ann_line = ""
        if ann_info:
            flag     = "⭐ " if ann_info["sensitive"] else ""
            ann_line = f"\n📋 今日公告：{flag}{ann_info['headline']}"

        ma200_str   = f" MA200:${s['ma200']}" if s.get("ma200") else ""
        vol_c_badge = " 📈量能连续" if s.get("vol_consistency") else ""
        send_telegram(
            f"<b>#{idx} {tier_label} {s.get('company_name', s['ticker'])}</b> ({s['ticker']})\n"
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

    # 发送三个平台Prompt
    for platform in ["telegram", "twitter", "xiaohongshu"]:
        log.info(f"发送 [{platform}] Prompt...")
        prompt_text = serialize_to_prompt(market_snap, stocks_block, platform)
        send_telegram(prompt_text)
        time.sleep(2.0)

    log.info("=== 日报Prompt发送完成 ===")

# ════════════════════════════════════════════════════════════
# 9. 主入口
# ════════════════════════════════════════════════════════════

def main() -> None:
    start = time.time()
    log.info("=" * 60)
    log.info(f"ASX System v13 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
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
        f"🏁 <b>ASX System v13 全部完成</b>\n"
        f"📅 {date.today().isoformat()} | ⏱ 总耗时：{elapsed} 分钟\n"
        f"选股：{'跳过（大盘红灯）' if status == 'red' else f'Top{len(screener_signals)}已完成'}\n"
        f"日报Prompt：Telegram/Twitter/小红书 已发送"
    )


if __name__ == "__main__":
    main()
