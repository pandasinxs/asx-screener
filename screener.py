# ============================================================
# ASX SYSTEM — screener.py  v15
#
# 流程一：EOD选股
#   全市场K线 → T1-T4筛选 → Top3加权评分 → 新闻/公告时间线
#   → Gemini深度分析 → Telegram → signals.json → GitHub推送
#
# 流程二：每日日报Prompt（不调用Gemini）
#   Top3 Movers → 技术面+精选新闻+公告+PDF关键段落
#   → 构建三平台Prompt → Telegram附件（SEO/Twitter/小红书）
#
# v15新增（相对v14）：
#   - signals_history表：记录全部T1-T4候选股（含落选），用于回测
#   - save_signal_to_history()：ATR倍数止盈止损，is_selected区分Top3/候选
#   - update_signal_outcomes()：每次运行自动更新历史信号结果（WIN/LOSS/TIMEOUT）
#   - 催化剂评分注入筛选循环
#   - T1-T4全部扫描合并排序（保证数量同时保证质量）
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
TOP_N           = 3

ASXBOX_REPO  = os.path.expanduser("~/asxbox")
SIGNALS_DIR  = os.path.join(ASXBOX_REPO, "src", "data", "signals")

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

# 回测参数（一旦开始记录不要修改，修改后前后数据不可比）
BT_STOP_ATR_MULT   = 2   # 止损 = 入场价 - 2×ATR14
BT_TARGET_ATR_MULT = 4   # 止盈 = 入场价 + 4×ATR14
BT_TIMEOUT_DAYS    = 20  # 超过20个交易日算TIMEOUT

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
    "rs_vs_xjo":      0.30,
    "adx14":          0.20,
    "vol_ratio":      0.15,
    "close_pos_pct":  0.08,
    "price_pct_1y":   0.07,
    "catalyst":       0.20,
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
    }


def calc_composite_score(tech: dict) -> float:
    def norm(val, lo, hi):
        return max(0.0, min(1.0, (val - lo) / (hi - lo))) if hi > lo else 0.0
    scores = {
        "rs_vs_xjo"     : norm(tech.get("rs_vs_xjo", 1.0), 0.8, 1.5),
        "adx14"         : norm(tech.get("adx14", 0),        15, 50),
        "vol_ratio"     : norm(tech.get("vol_ratio", 1.0),  1.0, 4.0),
        "close_pos_pct" : norm(tech.get("close_pos_pct", 50), 40, 100),
        "price_pct_1y"  : norm(tech.get("price_pct_1y", 50), 50, 100),
        "catalyst"      : tech.get("catalyst", 0.0),
    }
    return round(sum(SCORE_WEIGHTS[k] * v for k, v in scores.items()), 4)


def calc_confidence(tech: dict, tier_level: str) -> float:
    base_map  = {"T1": 0.85, "T2": 0.75, "T3": 0.65, "T4": 0.55}
    base      = base_map.get(tier_level, 0.60)
    adx       = tech.get("adx14", 20)
    rs        = tech.get("rs_vs_xjo", 1.0)
    adx_bonus = min(0.05, max(0.0, (adx - 25) / (50 - 25) * 0.05))
    rs_bonus  = min(0.05, max(0.0, (rs - 1.0) / 0.3 * 0.05))
    vol_bonus = 0.02 if tech.get("vol_consistency") else 0.0
    dist      = abs(tech.get("dist_52w_hi_pct", -20))
    dist_pen  = min(0.05, dist / 20 * 0.05)
    return round(min(0.92, max(0.50, base + adx_bonus + rs_bonus + vol_bonus - dist_pen)), 2)

# ════════════════════════════════════════════════════════════
# JSON字段提取 & signals.json生成 & GitHub推送
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


def push_to_github() -> bool:
    today_str = date.today().isoformat()
    try:
        r = subprocess.run(
            ["git", "-C", ASXBOX_REPO, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=10
        )
        branch = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else "main"
        log.info(f"push_to_github: 使用branch={branch}")
        cmds = [
            ["git", "-C", ASXBOX_REPO, "pull", "--no-rebase", "origin", branch],
            ["git", "-C", ASXBOX_REPO, "add",
             "src/data/signals/en.json", "src/data/signals/zh.json"],
            ["git", "-C", ASXBOX_REPO, "commit",
             "-m", f"chore: update signals {today_str}"],
            ["git", "-C", ASXBOX_REPO, "push", "origin", branch],
        ]
        for cmd in cmds:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                if "nothing to commit" in result.stdout + result.stderr:
                    log.info("push_to_github: 无变更，跳过commit")
                    return True
                log.error(f"push_to_github失败: {' '.join(cmd)}\n"
                          f"stdout:{result.stdout}\nstderr:{result.stderr}")
                return False
            log.info(f"git: {' '.join(cmd[2:])} → OK")
        log.info(f"push_to_github: 推送成功 [{today_str}] branch={branch}")
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
        codes  = df[col].dropna().astype(str).str.strip()
        valid  = codes[codes.str.match(r"^[A-Z]{1,5}$")]
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
    """初始化SQLite（announcements + signals_history两张表）"""
    import sqlite3
    try:
        with sqlite3.connect(ANN_DB_PATH) as conn:
            # ── 公告表 ──────────────────────────────────────────
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
            # ── 回测信号历史表 ────────────────────────────────────
            # 记录每日全部T1-T4候选股（含未入选），用于统计真实胜率
            # 止盈止损基于ATR倍数（BT_TARGET_ATR_MULT / BT_STOP_ATR_MULT）
            # ⚠️ 参数一旦开始记录不要修改，否则前后数据不可比
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


# ── 回测：写入信号历史 ────────────────────────────────────────

def save_signal_to_history(signal: dict, market_snap: dict,
                           is_selected: bool) -> None:
    """
    将单只候选股票写入signals_history。
    is_selected=True  → 当天进入Top3推送
    is_selected=False → 候选但未入选（同样记录，用于对比回测）

    止盈止损用ATR倍数：
      止损 = entry - BT_STOP_ATR_MULT  × ATR14
      止盈 = entry + BT_TARGET_ATR_MULT × ATR14
    风险收益比固定为 1:2，不依赖主观判断。
    """
    import sqlite3
    try:
        lc       = signal.get("price", 0)
        atr      = lc * signal.get("atr14_pct", 2.0) / 100
        sl       = round(lc - BT_STOP_ATR_MULT * atr, 4)
        tp       = round(lc + BT_TARGET_ATR_MULT * atr, 4)
        code     = signal.get("ticker", "").replace(".AX", "")
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
    """
    用当天最新K线更新历史PENDING信号的结果。
    逐日检查是否触发止盈/止损，超过BT_TIMEOUT_DAYS算TIMEOUT。
    在main()的K线下载完成后立即调用，确保数据最新。
    """
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

            # 转换index为日期字符串，只看信号日之后的数据
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

            # 逐日检查触发顺序（止损优先，同日同时触发算止损）
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
    except requests.ConnectionError as e:
        log.error(f"PDF下载连接错误 [{url[:60]}]: {e}")
    except requests.Timeout:
        log.error(f"PDF下载超时 [{url[:60]}]")
    except Exception as e:
        log.error(f"PDF提取失败 [{url[:60]}]: {e}")
    return ""


def fetch_news(ticker: str, company_name: str = "") -> list:
    code   = ticker.replace(".AX", "")
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    raw    = []
    for q in [f"ASX:{code}",
               f"{company_name} ASX" if company_name else f"{code} ASX Australia"]:
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
    events = []
    for a in announcements:
        sig      = a.get("significance", 0)
        flag     = "⭐" if a["sensitive"] else "📋"
        sig_label = f"[重要度:{sig}]" if sig >= 5 else ""
        line      = f"{flag}{sig_label}[公告] {a['headline']}"
        pdf_txt   = a.get("pdf_text", "")
        if pdf_txt:
            line += f"\n    📄PDF关键内容: {pdf_txt[:400]}"
        events.append({"date": a["date"], "text": line,
                        "sort_key": (a["date"], sig)})
    for n in news:
        events.append({"date": n["date"],
                        "text": f"📰[新闻] {n['title']} ({n['source']})",
                        "sort_key": (n["date"], 0)})
    if today_ann and code in today_ann:
        ta   = today_ann[code]
        flag = "⭐" if ta["sensitive"] else "📋"
        events.append({"date": date.today().isoformat(),
                        "text": f"{flag}[今日公告] {ta['headline']}",
                        "sort_key": (date.today().isoformat(), 10)})
    seen, lines = set(), []
    for e in sorted(events, key=lambda x: x["sort_key"], reverse=True):
        key = e["date"] + e["text"][:50]
        if key not in seen:
            seen.add(key)
            lines.append(f"{e['date']}  {e['text']}")
    return "\n".join(lines[:20]) if lines else "暂无近期公告/新闻"

# ════════════════════════════════════════════════════════════
# 4. Gemini
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
    """Prompt以.txt附件发送，避免长消息被Telegram分割"""
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

def _build_screener_prompt(signal: dict, timeline: str, tier_label: str) -> str:
    t = signal
    ma200_str  = f"MA200:{t['ma200']}" if t.get("ma200") else "MA200:数据不足"
    vol_c_str  = "✅ 近5日量能逐步递增" if t.get("vol_consistency") else "量能无持续性"
    pct_1y_str = f"{t.get('price_pct_1y', 50)}%分位（1年历史）"
    pe         = t.get("price_events", [])
    pe_str     = "\n".join(
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


def _build_report_stock_block(ticker: str, tech: dict, fund: dict,
                               timeline: str, pdf_texts: list, rank: int) -> str:
    ma200_str  = f"MA200:{tech['ma200']}" if tech.get("ma200") else "MA200:N/A"
    vol_c_str  = "量能连续递增✅" if tech.get("vol_consistency") else "量能无持续性"
    pct_1y_str = f"{tech.get('price_pct_1y', 50)}%分位(1年)"
    pe         = tech.get("price_events", [])
    pe_str     = " | ".join(
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
    for i, txt in enumerate(pdf_texts[:PDF_MAX_PER_STOCK], 1):
        if len(txt) > 400:
            block += f"\n\n【价格敏感公告完整原文#{i}（供撰文引用）】\n{txt}"
    return block


def serialize_to_prompt(market_snap: dict, stocks_block: str, platform: str) -> str:
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
        "seo": """You are a professional ASX equity research analyst and SEO content engine.

Your task is to generate a high-quality END-OF-DAY (EOD) stock analysis article based on structured market data.

This content is designed for:
- SEO indexing (Google search traffic)
- Retail trader education
- Post-market strategy interpretation
- Content automation pipeline

Input data contain data of up to 3 stocks. Generate SEO articles for each stock.

-------------------------------------------------
CRITICAL CONTEXT
-------------------------------------------------
This is END-OF-DAY (EOD) data.

You MUST:
- Use full-session price action (NOT intraday signals)
- Focus on closing behavior, not triggers
- Avoid VWAP, entry signals, or intraday mechanics
- Avoid any "real-time execution framing"

-------------------------------------------------
OUTPUT REQUIREMENT (STRICT)
-------------------------------------------------
You MUST generate:

1. English SEO articles for each stock provided in Markdown format
2. Chinese SEO article for each stock provided in Markdown format

Each article must be output in a separate code block.

-------------------------------------------------
FILE NAMING RULE
-------------------------------------------------
Before output, provide filenames:

Format:
YYYY-MM-DD-TICKER-KEYTHEME.md

Example:
2026-06-17-AIA.AX-approaching-resistance.md

Key theme must reflect dominant narrative:
(e.g. breakout, earnings momentum, resistance test, trend continuation)

-------------------------------------------------
ARTICLE STRUCTURE (SEO + TRADING HYBRID)
-------------------------------------------------

Each article MUST include:

## 1. YAML Front Matter (mandatory)

Include:
- title (SEO optimized, natural language)
- description (1-2 sentences, search oriented)
- pubDate (YYYY-MM-DD)

## 2. Market Context Section
- ASX200 performance
- sector leadership
- macro tone (risk-on / risk-off / rotation)

## 3. Stock Overview
- company name, sector, market cap, positioning summary (1 paragraph)

## 4. Technical Analysis (EOD-based)
- MA50 / MA200 trend structure
- RSI interpretation
- ADX trend strength
- volume confirmation
- proximity to 52-week high/low
IMPORTANT: No entry/exit triggers, no VWAP, no intraday logic

## 5. Catalyst & Narrative Flow (MOST IMPORTANT)
Build a STORY: Catalyst → Market reaction → Confirmation → Interpretation
Always explain "why now"

## 6. EOD Outlook
- continuation vs exhaustion vs consolidation
- next session bias
- key resistance/support zones

## 7. Conclusion
- one paragraph synthesis
- classify stock behavior

## 8. FAQ Section (SEO-critical, at least 4 questions)
Cover: Driver Explanation / Sustainability / Market Structure / Forward Scenario
Questions must adapt to stock-specific narrative

-------------------------------------------------
STYLE RULES
- Analyst tone, not news reporter tone
- No rigid templates or robotic phrasing
- No hallucinated data

-------------------------------------------------
Backtest Before Final Output (Mandatory Execution)

First generate an initial draft for self-backtesting. Verify all requirements met, 1 English + 1 Chinese article per stock. Output only the final version.
""",

        "twitter": """You are an event-driven ASX equity trader generating high-signal X (Twitter) content.

INPUT: ASX index data, sector performance, up to 3 stocks

🚨 STOCK ISOLATION RULE: Treat EACH stock as independent. Process ONE stock at a time. Do NOT mix stocks.

📦 OUTPUT MODE (STRICT)
- Each stock: EXACTLY 4 tweets
- Each tweet in its own triple backtick code block
- Stock A (4 blocks) → Stock B (4 blocks) → Stock C (4 blocks)

📉 FIXED 4-TWEET STRUCTURE
TWEET 1: Catalyst + Market Interpretation ($TICKER + price move + key event + repricing reason)
TWEET 2: Core Driver + Expectation Shift (what changed + BEFORE vs AFTER delta)
TWEET 3: Flow + Positioning (who is involved + flow type)
TWEET 4: Risk + Outcome (why move may fail + directional bias)

🧠 TRADER SPEECH RULES
- No formal comparisons, no full causal chains, no labeled reasoning
- Thoughts incomplete or abrupt, logic steps skipped
- Fragments over full sentences, hesitation allowed
- Each tweet ≤280 chars, no multi-paragraph

❌ HARD ANTI-FILLER
- No "interesting", "market watching", "suggests/indicates/therefore"
- No essay-style, no repeated structures, no macro commentary

🧠 HUMAN SIGNALS (per tweet set)
- Max 1-2 uncertainty expressions total
- At least 1 emotional reaction, 1 incomplete thought, 1 subtle contradiction
- Conviction varies across tweets

Backtest Before Final Output (Mandatory): Generate draft, verify format correct, output only final version.
""",

        "xiaohongshu": """ASX小红书交易盲盒复盘（叙事流IP版）

你是ASX短线交易者，每天做盯盘记录，不写报告。

━━━━━━━━━━━━━━
核心定义
"盲盒" = 股票（用于隐喻表达，规避平台审查）
全文统一使用"盲盒"，不使用"股票/标的"。
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
18字以内，表达冲突或状态（对今天的选股的主观感受，自由表达）

━━━━━━━━━━━━━━
二、固定开场（IP锚点）
第一句必须类似"让我们打开今天的盲盒"，然后直接进入每一个盲盒的点评。

━━━━━━━━━━━━━━
三、每个盲盒各写一段点评

1. 催化剂（必须包含）
- 今天发生了什么（必须带上股票代码）
- 如果有公告必须带一句总结
- 不单列数据，用人话解释

2. 历史背景 + 关注连续性
- 参考表达"我上月看它发生哪一件事，结果是怎么样，然后今天发生了什么事"
- 像"持续观察者视角"，不是一次性解读

3. 一句话分析（结构判断 + 交易决策合并）
- 允许乐观表达：等回撤就冲、决定试水、看好、趋势向好
- 不允许悲观表达：不追、不买、不看好

━━━━━━━━━━━━━━
备注1、盲盒数量：最多3个，按输入数据决定
备注2、不单独写开头总结段和结尾总结段
备注3、风格：人话叙事，有情绪，像交易日记

━━━━━━━━━━━━━━
备注4、回测再二次输出（必须执行）
1、先出一版文案给自己回测
2、每个盲盒用150字以内复述（备注3语言风格）
3、筛选替换所有股票和投资相关用词
4、检查是否有强烈个人观点，没有就加上返回第2步
5、检查逻辑错误
6、检查字数限制
7、不用交代过程，只给出最终版文案

━━━━━━━━━━━━━━
结尾后加上标签：
#ASX #复盘
⚠️仅个人记录，不构成投资建议
（标签不算在字数限制以内，标签与正文之间空两行）
""",
    }

    instruction = instructions.get(platform, instructions["seo"])

    return f"""📋 <b>ASX日报Prompt — {platform.upper()} — {market_snap.get('date','')}</b>

你是一位专注澳大利亚股市(ASX)的资深投资分析师。以下是今日为你准备的输入数据。

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
    today   = date.today().strftime("%Y-%m-%d")
    start   = time.time()
    status  = market_snap.get("market_status", "normal")
    xjo_s   = market_snap.get("xjo_series")
    xjo_pct = market_snap.get("xjo_change_pct", 0)

    market_note  = "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。" if status == "yellow" else ""
    market_label = "⚠️ " if status == "yellow" else ""

    today_ann = fetch_today_announcements()

    # T1→T4全部扫描，每只股票只记录通过的最高层级
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
                    # 催化剂评分注入
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

    raw_signals    = list(seen_tickers.values())
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
    raw_signals = raw_signals[:10]

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

    # 层级汇总
    tier_summary = {}
    for s in signals:
        lv = s.get("tier_level", "T?")
        tier_summary[lv] = tier_summary.get(lv, 0) + 1
    tier_label = " / ".join(f"{lv}×{n}" for lv, n in sorted(tier_summary.items()))
    tier_level = signals[0].get("tier_level", "T?") if signals else "T?"

    # ── 回测：记录全部候选（含未入选）────────────────────────────
    selected_tickers = {s["ticker"] for s in signals}
    for s in raw_signals:
        save_signal_to_history(
            s, market_snap,
            is_selected=(s["ticker"] in selected_tickers)
        )
    log.info(f"signals_history写入：{len(raw_signals)} 条候选（Top3已标记）")

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

    # 写入watchlist监测队列
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

    # signals.json → GitHub
    valid_signals = [s for s in signals if s.get("_json_valid")]
    log.info(f"生成signals.json：{len(valid_signals)}/{len(signals)} 只有效JSON字段...")
    written = generate_signals_json(valid_signals)
    if written:
        pushed = push_to_github()
        if pushed:
            send_telegram(
                f"🌐 <b>网站已更新</b> {today}\n"
                f"signals.json已推送GitHub，Cloudflare正在重建。\n"
                f"信号数量：{len(valid_signals)} 只"
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
        if "composite_score" not in tech:
            tech["composite_score"] = calc_composite_score(tech)

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
        f"以下3个文件已发送，复制文件内容给AI生成文章👇"
    )

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
    log.info(f"ASX System v15 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
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

    # Step 2：全市场K线
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

    # Step 2.5：更新历史信号回测结果
    log.info("【Step 2.5】更新历史信号回测结果...")
    update_signal_outcomes(all_data)

    # Step 3：选股流程
    screener_signals = []
    if status != "red":
        log.info("【Step 3】选股筛选流程...")
        screener_signals = run_screener_flow(all_data, market_snap)
    else:
        log.info("【Step 3】大盘红灯，跳过选股")

    # Step 4：日报Prompt
    log.info("【Step 4】日报Prompt生成流程...")
    run_report_flow(all_data, market_snap,
                    screener_signals=screener_signals if screener_signals else None)

    elapsed = round((time.time() - start) / 60, 1)
    log.info("=" * 60)
    log.info(f"ASX System 全部完成，总耗时：{elapsed} 分钟")
    log.info("=" * 60)
    send_telegram(
        f"🏁 <b>ASX System v15 全部完成</b>\n"
        f"📅 {date.today().isoformat()} | ⏱ 总耗时：{elapsed} 分钟\n"
        f"选股：{'跳过（大盘红灯）' if status == 'red' else f'Top{len(screener_signals)}已完成'}\n"
        f"日报Prompt：SEO / Twitter / 小红书 已发送"
    )


if __name__ == "__main__":
    main()