# ============================================================
# ASX SWING TRADE SCREENER v11
# 重构：删除T5-T8 | 改进指标(ADX/VWAP/RS) | 深度分析流程
# Gemini 2.5 Flash | 固定30秒重试最多10分钟
# ============================================================

import os
import time
import logging
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, date, timedelta
from typing import Optional
from google import genai

# ── 日志配置 ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('screener.log', encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)

# ── 环境变量 ──────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

# ── Gemini配置 ────────────────────────────────────────────────
GEMINI_MODEL       = "gemini-2.5-flash"
# 深度分析开thinking(催化剂预测需要推理链)，快速任务关闭节省token
GEMINI_CONFIG_DEEP = {"thinking_config": {"thinking_budget": 512}}
GEMINI_CONFIG_FAST = {"thinking_config": {"thinking_budget": 0}}

gemini_client = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

# ── 重试参数：固定30秒，最多20次=10分钟上限 ──────────────────
RETRY_MAX  = 20
RETRY_WAIT = 30

# ── API端点 ───────────────────────────────────────────────────
ASX_ANN_URL    = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_TICKER_ANN = "https://asx.api.markitdigital.com/asx-research/1.0/company/{code}/announcements"
ASX_HEADERS    = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept':     'application/json',
    'Referer':    'https://www.asx.com.au'
}

# ── 分级筛选层级（仅T1-T4，删除T5-T8）───────────────────────
TIERS = [
    {
        "level": "T1", "label": "🔴 精英",
        "vol_mult": 2.0, "close_pos": 0.88, "consol": 0.12,
        "rsi_lo": 45,   "rsi_hi": 65,
        "adx_min": 28,  "di_cross": True,
        "vwap_above": True,  "rs_min": 1.05,
        "vol_decline": True, "near_52w_hi": True,
        "note": "最高质量：所有条件严格满足，ADX趋势成形，跑赢大盘"
    },
    {
        "level": "T2", "label": "🟠 优质",
        "vol_mult": 1.5, "close_pos": 0.75, "consol": 0.15,
        "rsi_lo": 42,   "rsi_hi": 68,
        "adx_min": 25,  "di_cross": True,
        "vwap_above": True,  "rs_min": 1.02,
        "vol_decline": True, "near_52w_hi": True,
        "note": "高质量信号，趋势明确"
    },
    {
        "level": "T3", "label": "🟡 标准",
        "vol_mult": 1.2, "close_pos": 0.60, "consol": 0.20,
        "rsi_lo": 38,   "rsi_hi": 72,
        "adx_min": 20,  "di_cross": True,
        "vwap_above": True,  "rs_min": 1.0,
        "vol_decline": True, "near_52w_hi": False,
        "note": "标准质量，趋势初步形成"
    },
    {
        "level": "T4", "label": "🟢 放宽",
        "vol_mult": 1.0, "close_pos": 0.50, "consol": 0.25,
        "rsi_lo": 35,   "rsi_hi": 75,
        "adx_min": 15,  "di_cross": False,
        "vwap_above": True,  "rs_min": 0.98,
        "vol_decline": False,"near_52w_hi": False,
        "note": "参考信号，需结合基本面判断"
    },
]


# ════════════════════════════════════════════════════════════
# Gemini调用：固定30秒重试，最多10分钟
# ════════════════════════════════════════════════════════════

def ask_gemini(prompt: str, use_thinking: bool = False, label: str = "") -> str:
    """
    固定30秒间隔重试，最多20次(=10分钟)，成功即停止不继续循环。
    use_thinking=True: thinking_budget=512，用于催化剂预测等推理任务
    use_thinking=False: 关闭thinking，节省token
    """
    if not gemini_client:
        return ""

    config = GEMINI_CONFIG_DEEP if use_thinking else GEMINI_CONFIG_FAST

    for attempt in range(1, RETRY_MAX + 1):
        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=config
            )
            if attempt > 1:
                log.info(f"Gemini成功 [{label}] 第{attempt}次尝试")
            return response.text.strip()

        except Exception as e:
            err = str(e)
            is_retryable = any(k in err for k in (
                "429", "503", "RESOURCE_EXHAUSTED", "overloaded", "quota"
            ))
            if is_retryable and attempt < RETRY_MAX:
                log.warning(
                    f"Gemini限速 [{label}] 第{attempt}/{RETRY_MAX}次，"
                    f"{RETRY_WAIT}秒后重试..."
                )
                time.sleep(RETRY_WAIT)
            elif is_retryable and attempt == RETRY_MAX:
                log.error(f"Gemini [{label}] 达到10分钟上限，放弃")
                return ""
            else:
                # 认证失败/网络错误等不可重试错误
                log.error(f"Gemini不可重试错误 [{label}]: {err}")
                return ""

    return ""


# ════════════════════════════════════════════════════════════
# 改进技术指标
# ════════════════════════════════════════════════════════════

def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = -delta.clip(upper=0).rolling(period).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))


def calc_adx(high: pd.Series, low: pd.Series,
             close: pd.Series, period: int = 14) -> tuple:
    """
    ADX + DI指标。
    替换OBV的理由：OBV在ASX小盘股中因配股/大宗交易频繁出现单日跳变，
    导致OBV趋势完全失真。ADX直接衡量趋势强度，更稳定可靠。
    ADX>25=趋势成形；+DI>-DI=多头主导。
    """
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)

    up_move   = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm  = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr      = tr.rolling(period).mean()
    plus_di  = 100 * (plus_dm.rolling(period).mean()  / atr.replace(0, 1e-10))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, 1e-10))
    dx       = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-10))
    adx      = dx.rolling(period).mean()

    return adx, plus_di, minus_di


def calc_vwap_slope(close: pd.Series, volume: pd.Series,
                    window: int = 20) -> tuple:
    """
    20日滚动VWAP及斜率。
    替换MFI的理由：MFI(40-70)限制区间存在逻辑缺陷——突破70恰恰是
    资金强力流入信号，原代码反而将其过滤。VWAP更直接反映机构成本线。
    vwap_slope>0 = 机构持续买入，成本线上移。
    """
    vwap  = (close * volume).rolling(window).sum() / volume.rolling(window).sum()
    slope = float(vwap.iloc[-1]) - float(vwap.iloc[-6])  # 5日斜率
    return float(vwap.iloc[-1]), slope


def calc_relative_strength(close: pd.Series,
                            benchmark: pd.Series, period: int = 20) -> float:
    """
    相对强度 = 个股20日涨幅 / 大盘20日涨幅。
    RS>1.0 = 跑赢大盘。这是判断主力驻扎的核心信号：
    大盘下跌但个股横盘/上涨，说明有机构在护盘建仓。
    """
    try:
        stock_ret = float(close.iloc[-1]) / float(close.iloc[-period]) - 1
        bench_ret = float(benchmark.iloc[-1]) / float(benchmark.iloc[-period]) - 1
        if abs(bench_ret) < 1e-6:
            return 1.0
        return (1 + stock_ret) / (1 + bench_ret)
    except Exception:
        return 1.0


def check_market_status() -> tuple:
    """
    检查ASX200大盘，同时返回XJO数据供RS计算复用（避免重复下载）。
    返回：(status: str, xjo_close: pd.Series | None)
    """
    try:
        xjo   = yf.download("^AXJO", period="1y", interval="1d", progress=False)
        if xjo.empty or len(xjo) < 50:
            return "green", None
        close = xjo['Close'].squeeze()
        ma50  = close.rolling(50).mean()
        dev   = (float(close.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1])
        drop  = (close.resample('W').last().pct_change().iloc[-2:] < -0.05).any()
        if dev < -0.03 or drop:
            return "red", close
        if dev < 0:
            return "yellow", close
        return "green", close
    except Exception as e:
        log.warning(f"大盘状态检查失败，默认green: {e}")
        return "green", None


# ════════════════════════════════════════════════════════════
# 数据获取层
# ════════════════════════════════════════════════════════════

def get_asx_universe() -> list:
    try:
        df  = pd.read_csv(
            "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
            skiprows=1, encoding='latin1'
        )
        col = next((c for c in df.columns if 'code' in c.lower()), None)
        if col is None:
            log.error("ASX股票列表列名未找到")
            return []
        codes   = df[col].dropna().astype(str).str.strip()
        valid   = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        log.info(f"ASX股票池：{len(tickers)} 只")
        return tickers
    except Exception as e:
        log.error(f"获取ASX列表失败: {e}")
        return []


def batch_download_all(tickers: list, batch_size: int = 50) -> dict:
    """下载1年日线数据（改为1年，RS计算和技术摘要需要更长历史）"""
    all_data  = {}
    n_batches = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch     = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        if batch_num % 5 == 0 or batch_num == 1:
            log.info(f"  下载 {batch_num}/{n_batches} 批...")
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="1y", interval="1d", progress=False)
                if not df.empty and len(df) >= 60:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(batch, period="1y", interval="1d",
                                  progress=False, group_by='ticker')
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how='all')
                        if not tdf.empty and len(tdf) >= 60:
                            all_data[t] = tdf
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"批量下载失败，单只降级: {e}")
            for t in batch:
                try:
                    df = yf.download(t, period="1y", interval="1d", progress=False)
                    if not df.empty and len(df) >= 60:
                        all_data[t] = df
                except Exception as e2:
                    log.debug(f"单只下载失败 [{t}]: {e2}")
        time.sleep(0.5)
    log.info(f"  下载完成：{len(all_data)}/{len(tickers)} 只有效")
    return all_data


def get_today_announcements() -> dict:
    today  = date.today().isoformat()
    result = {}
    page   = 0
    while True:
        try:
            r = requests.get(
                ASX_ANN_URL,
                params={'itemsPerPage': 100, 'page': page},
                headers=ASX_HEADERS, timeout=10
            )
            r.raise_for_status()
            items = r.json().get('data', {}).get('items', [])
            if not items:
                break
            got_old = False
            for item in items:
                if item.get('date', '')[:10] < today:
                    got_old = True
                    break
                sym = item.get('symbol', '')
                if sym and sym not in result:
                    result[sym] = {
                        'headline' : item.get('headline', '')[:70],
                        'sensitive': item.get('isPriceSensitive', False)
                    }
            if got_old or len(items) < 100:
                break
            page += 1
            time.sleep(0.3)
        except requests.HTTPError as e:
            log.error(f"公告API HTTP错误: {e}")
            break
        except Exception as e:
            log.error(f"公告API异常: {e}")
            break
    log.info(f"今日公告：{len(result)} 只股票")
    return result


def get_historical_announcements(code: str, days: int = 180) -> list:
    """
    抓取单只股票近N天历史公告，用于构建新闻时间线。
    使用ASX per-ticker公告端点（与全市场公告端点不同）。
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    result = []
    page   = 0
    url    = ASX_TICKER_ANN.format(code=code)
    while True:
        try:
            r = requests.get(
                url,
                params={'itemsPerPage': 20, 'page': page},
                headers=ASX_HEADERS, timeout=10
            )
            r.raise_for_status()
            items = r.json().get('data', {}).get('items', [])
            if not items:
                break
            got_old = False
            for item in items:
                pub_date = item.get('date', '')[:10]
                if pub_date < cutoff:
                    got_old = True
                    break
                result.append({
                    'date'     : pub_date,
                    'headline' : item.get('headline', '')[:80],
                    'sensitive': item.get('isPriceSensitive', False),
                    'doc_type' : item.get('documentType', '')
                })
            if got_old or len(items) < 20:
                break
            page += 1
            time.sleep(0.2)
        except Exception as e:
            log.debug(f"历史公告获取失败 [{code}]: {e}")
            break
    return result


def get_yf_news(code: str) -> list:
    """yfinance近期新闻"""
    try:
        stock  = yf.Ticker(f"{code}.AX")
        today  = date.today().isoformat()
        result = []
        for n in (stock.news or [])[:10]:
            content = n.get('content', {})
            title   = content.get('title', '')
            pub     = content.get('pubDate', '')[:10]
            if title:
                result.append({
                    'title' : title[:80],
                    'date'  : pub,
                    'today' : pub == today,
                    'source': content.get('provider', {}).get('displayName', '')
                })
        result.sort(key=lambda x: x['date'], reverse=True)
        return result
    except Exception as e:
        log.debug(f"yfinance新闻失败 [{code}]: {e}")
        return []


def build_news_timeline(code: str) -> str:
    """
    合并历史公告(ASX) + 近期新闻(yfinance)，构建时间线文本。
    去重：同日同标题前50字只保留一条。
    最多返回30条，避免超出token限制。
    """
    ann_list  = get_historical_announcements(code)
    news_list = get_yf_news(code)

    events = []
    for a in ann_list:
        flag = "⭐" if a['sensitive'] else "📋"
        events.append({'date': a['date'], 'text': f"{flag}[公告] {a['headline']}"})
    for n in news_list:
        events.append({'date': n['date'], 'text': f"📰[新闻] {n['title']} ({n['source']})"})

    seen, result = set(), []
    for e in sorted(events, key=lambda x: x['date'], reverse=True):
        key = e['date'] + e['text'][:50]
        if key not in seen:
            seen.add(key)
            result.append(f"{e['date']}  {e['text']}")

    return "\n".join(result[:30]) if result else "暂无近期公告/新闻"


def get_technical_summary(ticker: str, df: pd.DataFrame,
                           xjo_close: Optional[pd.Series]) -> dict:
    """
    计算筛选通过股票的完整技术指标摘要，供Gemini深度分析使用。
    包含：均线、RSI、ADX/DI、VWAP、RS、ATR、52周高低、最大回撤。
    """
    close  = df['Close'].squeeze()
    high   = df['High'].squeeze()
    low    = df['Low'].squeeze()
    volume = df['Volume'].squeeze()

    rsi                    = calc_rsi(close)
    adx_s, plus_di_s, minus_di_s = calc_adx(high, low, close)
    vwap_val, vwap_slope   = calc_vwap_slope(close, volume)

    ma20  = close.rolling(20).mean()
    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    lc   = float(close.iloc[-1])
    lv   = float(volume.iloc[-1])
    vm20 = float(volume.rolling(20).mean().iloc[-1])

    w52_hi = float(high.rolling(min(252, len(high))).max().iloc[-1])
    w52_lo = float(low.rolling(min(252, len(low))).min().iloc[-1])

    # 6个月最大回撤
    roll_max = close.rolling(126).max()
    max_dd   = float(((close - roll_max) / roll_max * 100).min())

    # ATR波动率
    prev_c = close.shift(1)
    tr     = pd.concat([high-low, (high-prev_c).abs(), (low-prev_c).abs()], axis=1).max(axis=1)
    atr14  = float(tr.rolling(14).mean().iloc[-1])

    rs = calc_relative_strength(close, xjo_close) if xjo_close is not None else 1.0

    return {
        'price'              : round(lc, 3),
        'ma20'               : round(float(ma20.iloc[-1]), 3),
        'ma50'               : round(float(ma50.iloc[-1]), 3),
        'ma200'              : round(float(ma200.iloc[-1]), 3) if len(close) >= 200 else None,
        'rsi14'              : round(float(rsi.iloc[-1]), 1),
        'adx14'              : round(float(adx_s.iloc[-1]), 1),
        'plus_di'            : round(float(plus_di_s.iloc[-1]), 1),
        'minus_di'           : round(float(minus_di_s.iloc[-1]), 1),
        'vwap20'             : round(vwap_val, 3),
        'vwap_slope'         : "上升" if vwap_slope > 0 else "下降",
        'vol_ratio'          : round(lv / vm20, 2),
        'rs_vs_xjo'          : round(rs, 3),
        'atr14_pct'          : round(atr14 / lc * 100, 2),
        'w52_hi'             : round(w52_hi, 3),
        'w52_lo'             : round(w52_lo, 3),
        'dist_52w_hi_pct'    : round((lc / w52_hi - 1) * 100, 1),
        'max_dd_6m_pct'      : round(max_dd, 1),
    }


def enrich_fundamentals(signals: list) -> list:
    """
    仅对筛选通过的股票(≤15只)调用.info获取市值/行业。
    原版在analyze_stock内调用 = ~2000次 → 现在最多15次。
    """
    enriched = []
    for s in signals:
        try:
            info   = yf.Ticker(s['ticker']).info
            mktcap = info.get('marketCap', 0)
            if mktcap < 50_000_000:
                log.debug(f"市值过滤 [{s['ticker']}]: {mktcap/1e6:.1f}M")
                continue
            s['market_cap_m'] = round(mktcap / 1_000_000)
            s['sector']       = info.get('sector', '未知')
            s['industry']     = info.get('industry', '未知')
            s['company_name'] = info.get('longName', s['ticker'])
            enriched.append(s)
        except Exception as e:
            log.debug(f"基本面补全失败 [{s['ticker']}]: {e}")
            s['market_cap_m'] = 0
            s['sector']       = '未知'
            s['industry']     = '未知'
            s['company_name'] = s['ticker']
            enriched.append(s)
    return enriched


# ════════════════════════════════════════════════════════════
# 核心：深度Gemini分析（每只股票独立调用，开thinking）
# ════════════════════════════════════════════════════════════

def deep_analyze_stock(signal: dict, tech: dict, news_timeline: str,
                       tier_label: str) -> str:
    """
    构建完整分析包 → Gemini深度分析。
    开启thinking_budget=512：催化剂预测和趋势预判需要推理链。
    Prompt设计原则：结构化输出4段，精炼不冗余，节省token。
    """
    t = tech
    ma200_str = f"MA200:{t['ma200']}" if t.get('ma200') else "MA200:数据不足"
    tech_block = (
        f"价格:{t['price']} MA20:{t['ma20']} MA50:{t['ma50']} {ma200_str}\n"
        f"RSI:{t['rsi14']} ADX:{t['adx14']} +DI:{t['plus_di']} -DI:{t['minus_di']}\n"
        f"VWAP20:{t['vwap20']}({t['vwap_slope']}) 量比:{t['vol_ratio']}x\n"
        f"相对强度(vs XJO):{t['rs_vs_xjo']} ATR波动率:{t['atr14_pct']}%\n"
        f"52W高:{t['w52_hi']} 52W低:{t['w52_lo']} 距52W高:{t['dist_52w_hi_pct']}%\n"
        f"近6月最大回撤:{t['max_dd_6m_pct']}%"
    )
    company_info = (
        f"{signal.get('company_name', signal['ticker'])} "
        f"({signal.get('sector','未知')}/{signal.get('industry','未知')}) "
        f"市值:{signal.get('market_cap_m',0)}M AUD"
    )

    prompt = f"""你是一位专注ASX市场的资深机构分析师。今天是{date.today().isoformat()}。

===== 分析标的 =====
{signal['ticker']} | 筛选等级:{tier_label} | {company_info}

===== 技术指标（近1年数据） =====
{tech_block}

===== 近6个月新闻/公告时间线 =====
{news_timeline}

===== 分析任务 =====
请严格按以下4部分输出，每部分2-3句，语言精炼专业：

【技术形态】当前趋势结构，关键支撑/压力位，量价关系评估。

【事件分析】时间线中最重要的1-2个价格驱动事件，判断其影响是否已被市场消化。

【催化剂预测】基于公告规律和行业周期，预测未来4-8周最可能出现的催化剂类型和时间窗口。

【综合结论】未来4-8周趋势方向，核心理由，明确给出：买入 / 观望 / 回避，并说明止损逻辑。

规则：不确定内容标注"需进一步核查"，禁止编造数据，禁止重复技术数据原文。"""

    log.info(f"深度分析 [{signal['ticker']}] thinking=ON...")
    result = ask_gemini(prompt, use_thinking=True, label=signal['ticker'])
    return result if result else "⚠️ Gemini分析暂时不可用，请稍后手动复查"


# ════════════════════════════════════════════════════════════
# 技术筛选层（改进版）
# ════════════════════════════════════════════════════════════

def analyze_stock(ticker: str, df: pd.DataFrame,
                  tier: dict, xjo_close: Optional[pd.Series]) -> Optional[dict]:
    """
    改进版筛选逻辑：
    - OBV → ADX+DI（更稳定，不受单日异常成交量影响）
    - MFI(42-72限制) → VWAP偏离+斜率（修正反向过滤缺陷）
    - 20日最高点 → 52周高点90%（更有意义的突破参考位）
    - vol_decline窗口修正：10日vs10日等长对比（原版10日vs20日不对称）
    - 新增相对强度RS过滤
    注意：.info调用已移至enrich_fundamentals，此处不调用
    """
    try:
        close  = df['Close'].squeeze()
        high   = df['High'].squeeze()
        low    = df['Low'].squeeze()
        volume = df['Volume'].squeeze()

        if len(close) < 60:
            return None

        lc   = float(close.iloc[-1])
        lh   = float(high.iloc[-1])
        ll   = float(low.iloc[-1])
        lvol = float(volume.iloc[-1])

        # 流动性过滤（日均成交额<30万AUD）
        if float(volume.iloc[-20:].mean()) * lc < 300_000:
            return None

        # 均线趋势：价格在上升的MA50之上
        ma50      = close.rolling(50).mean()
        lm50      = float(ma50.iloc[-1])
        lm50_prev = float(ma50.iloc[-11])
        if lc < lm50 or lm50 <= lm50_prev:
            return None

        # 盘整幅度
        r15 = df.iloc[-15:]
        pr  = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > tier["consol"]:
            return None

        # 量能递减（修正：等长10日窗口）
        if tier["vol_decline"]:
            if float(volume.iloc[-10:].mean()) >= float(volume.iloc[-20:-10].mean()):
                return None

        # ADX趋势强度（替换OBV）
        adx_s, plus_di_s, minus_di_s = calc_adx(high, low, close)
        ladx      = float(adx_s.iloc[-1])
        lplus_di  = float(plus_di_s.iloc[-1])
        lminus_di = float(minus_di_s.iloc[-1])
        if ladx < tier["adx_min"]:
            return None
        if tier["di_cross"] and lplus_di <= lminus_di:
            return None

        # VWAP确认（替换MFI）
        vwap_val, vwap_slope = calc_vwap_slope(close, volume)
        if tier["vwap_above"] and (lc < vwap_val or vwap_slope <= 0):
            return None

        # 相对强度（新增）
        rs = calc_relative_strength(close, xjo_close)
        if rs < tier["rs_min"]:
            return None

        # 52周高点附近（替换20日高点判断）
        if tier["near_52w_hi"]:
            w52_hi = float(high.rolling(min(252, len(high))).max().iloc[-1])
            if lc < w52_hi * 0.90:
                return None

        # 放量确认
        vol_ma20 = float(volume.rolling(20).mean().iloc[-1])
        if lvol < vol_ma20 * tier["vol_mult"]:
            return None

        # RSI范围
        lrsi = float(calc_rsi(close).iloc[-1])
        if not (tier["rsi_lo"] <= lrsi <= tier["rsi_hi"]):
            return None

        # 收盘位置
        day_range = lh - ll
        close_pos = (lc - ll) / day_range if day_range > 0 else 0.5
        if close_pos < tier["close_pos"]:
            return None

        return {
            'ticker'     : ticker,
            'price'      : round(lc, 3),
            'entry_limit': round(lc * 1.02, 3),
            'stop_loss'  : round(lc * 0.90, 3),
            'take_profit': round(lc * 1.20, 3),
            'rsi'        : round(lrsi, 1),
            'adx'        : round(ladx, 1),
            'plus_di'    : round(lplus_di, 1),
            'vol_ratio'  : round(lvol / vol_ma20, 2),
            'close_pos'  : round(close_pos * 100, 1),
            'rs_vs_xjo'  : round(rs, 3),
        }
    except Exception as e:
        log.debug(f"analyze_stock异常 [{ticker}]: {e}")
        return None


# ════════════════════════════════════════════════════════════
# 通知层
# ════════════════════════════════════════════════════════════

def send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过发送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        log.error(f"Telegram HTTP错误: {e}")
    except Exception as e:
        log.error(f"Telegram发送失败: {e}")


# ════════════════════════════════════════════════════════════
# 主程序
# ════════════════════════════════════════════════════════════

def run_screener() -> None:
    today = datetime.now().strftime('%Y-%m-%d')
    start = time.time()
    log.info(f"=== ASX Screener v11 启动 [{today}] ===")

    # Step 1：大盘状态（XJO数据同时供RS计算复用）
    market_status, xjo_close = check_market_status()
    log.info(f"大盘状态: {market_status.upper()}")

    if market_status == "red":
        send_telegram(
            f"🔴 <b>大盘警告 {today}</b>\n\n"
            "ASX200大幅跌破50日均线或近期急跌。\n"
            "今日<b>不建议开新仓</b>，收紧止损至5%。"
        )
        return

    market_note  = (
        "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。"
    ) if market_status == "yellow" else ""
    market_label = "⚠️ " if market_status == "yellow" else ""

    # Step 2：股票池 + K线下载
    universe = get_asx_universe()
    if not universe:
        log.error("股票池获取失败，终止")
        return

    log.info(f"批量下载 {len(universe)} 只K线（1年数据）...")
    all_data = batch_download_all(universe)
    log.info(f"下载耗时：{round(time.time() - start)}秒")

    # Step 3：分级筛选（T1→T4，首个有信号的层级停止）
    log.info("分级筛选（T1-T4）...")
    found_tier, raw_signals = None, []
    for tier in TIERS:
        log.info(f"  {tier['level']} ({tier['label']})...")
        tier_signals = [
            r for t, df in all_data.items()
            if (r := analyze_stock(t, df, tier, xjo_close))
        ]
        log.info(f"    → {len(tier_signals)} 个")
        if tier_signals:
            found_tier, raw_signals = tier, tier_signals
            break

    elapsed_screen = round((time.time() - start) / 60, 1)

    # Step 4：无信号处理
    if not raw_signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只（T1-T4均无信号）\n"
            f"市场整体动能不足，建议观望。\n"
            f"耗时：{elapsed_screen}分钟{market_note}"
        )
        return

    # Step 5：排序 + Top15 + 基本面补全（.info仅此处调用）
    # 主排序：相对强度RS（跑赢大盘最重要）；次排序：量比
    raw_signals.sort(key=lambda x: (x['rs_vs_xjo'], x['vol_ratio']), reverse=True)
    raw_signals = raw_signals[:15]
    signals     = enrich_fundamentals(raw_signals)

    tier_label = found_tier["label"]
    tier_level = found_tier["level"]

    # Step 6：发汇总
    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"扫描：{len(all_data)} 只｜筛选耗时：{elapsed_screen}分钟\n"
        f"信号等级：{tier_label}\n"
        f"触发信号：{len(signals)} 只\n"
        f"说明：{found_tier['note']}\n\n"
        + "\n".join([
            f"• {s['ticker']} | RS:{s['rs_vs_xjo']} 量比:{s['vol_ratio']}x ADX:{s['adx']}"
            for s in signals
        ])
        + market_note
    )

    # Step 7：逐只深度分析（拉取公告→构建时间线→Gemini分析）
    log.info("拉取今日公告...")
    ann_map = get_today_announcements()

    log.info(f"开始深度分析 {len(signals)} 只股票（每只独立Gemini调用）...")
    for idx, s in enumerate(signals, 1):
        code = s['ticker'].replace('.AX', '')
        log.info(f"  [{idx}/{len(signals)}] {s['ticker']} 构建分析包...")

        # 技术摘要（完整指标）
        df_stock = all_data.get(s['ticker'])
        tech     = get_technical_summary(s['ticker'], df_stock, xjo_close) if df_stock is not None else {}

        # 新闻时间线（ASX历史公告 + yfinance新闻）
        news_timeline = build_news_timeline(code)

        # 今日公告置顶
        ann_info = ann_map.get(code)
        if ann_info:
            sen_flag = "⭐" if ann_info['sensitive'] else "📋"
            news_timeline = f"{today}  {sen_flag}[今日公告] {ann_info['headline']}\n" + news_timeline

        # Gemini深度分析
        analysis = deep_analyze_stock(s, tech, news_timeline, tier_label)

        # 发详情消息
        ann_line = ""
        if ann_info:
            flag     = "⭐ " if ann_info['sensitive'] else ""
            ann_line = f"\n📋 今日公告：{flag}{ann_info['headline']}"

        msg = (
            f"{tier_label} <b>{s.get('company_name', s['ticker'])}</b> "
            f"({s['ticker']})\n"
            f"📅 {today} | {s.get('sector','未知')} | 市值:${s.get('market_cap_m',0)}M\n\n"
            f"💰 昨收：${s['price']}\n"
            f"🟢 入场上限：${s['entry_limit']}（超过不追）\n"
            f"🎯 止盈：${s['take_profit']}（+20%）\n"
            f"🛑 止损：${s['stop_loss']}（-10%）\n\n"
            f"📊 RSI:{s['rsi']} | ADX:{s['adx']} | +DI:{s['plus_di']}\n"
            f"   量比:{s['vol_ratio']}x | 收盘位:{s['close_pos']}%\n"
            f"   相对强度(vs XJO):{s['rs_vs_xjo']}"
            f"{ann_line}\n\n"
            f"🤖 <b>深度分析</b>\n{analysis}\n\n"
            f"⚠️ 核对图表再决定入场{market_note}"
        )
        send_telegram(msg)
        time.sleep(1.0)  # 避免Telegram限速

    elapsed_total = round((time.time() - start) / 60, 1)
    log.info(f"=== 完成：{tier_level}，{len(signals)} 个，总耗时{elapsed_total}分钟 ===")
    send_telegram(
        f"✅ <b>分析完成</b> {today}\n"
        f"等级：{tier_label} | 共{len(signals)}只 | 耗时:{elapsed_total}分钟"
    )


if __name__ == "__main__":
    run_screener()
