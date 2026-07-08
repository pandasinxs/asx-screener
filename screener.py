# ============================================================
# ASX SYSTEM — screener.py  v18
#
# 流程一：EOD选股
#   全市场K线 → T1-T4筛选 → Top3加权评分 → 新闻/公告时间线
#   → Gemini分析（纯Telegram阅读，不再产出JSON/SEO）
#
# 流程二：SEO文章 + 信号JSON 合并生成（v18新增，取代原来从不触发的
#   run_report_flow "seo" prompt）
#   Top3 → 单次Gemini批量调用，产出：
#     - 每只股票的英文SEO文章 + 中文SEO文章（Markdown，含frontmatter）
#     - 每只股票的URL slug
#     - 每只股票的信号标签字段（JSON_TAG/ONE_LINER，中英文）
#   → 逐只校验（长度/frontmatter/标题数/FAQ） → 校验失败的股票跳过+
#   Telegram告警，绝不用半成品覆盖线上内容 → 校验通过的文章写入
#   src/content/blog/en|zh/ → signals.json重新生成 → 一次性git commit推送
#
# 流程三：每日日报Prompt（Twitter / 小红书，人工使用，不调用Gemini）
#   Top3 Movers → 技术面+精选新闻+公告+PDF关键段落
#   → 构建Prompt → Telegram附件
#
# v15新增（相对v14）：
#   - signals_history表：记录全部T1-T4候选股（含落选），用于回测
#   - save_signal_to_history()：ATR倍数止盈止损，is_selected区分Top3/候选
#   - update_signal_outcomes()：每次运行自动更新历史信号结果（WIN/LOSS/TIMEOUT）
#   - 催化剂评分注入筛选循环
#   - T1-T4全部扫描合并排序（保证数量同时保证质量）
#
# v16新增（相对v15）：
#   - 新增 _check_volume_quality()：量能质量检查，替代原硬性缩量要求
#   - 允许「缩量整理」和「温和持续递增」两种量能模式通过T1-T3
#   - 拒绝「单日脉冲爆量」和「随机震荡无方向」两种无效量能模式
#   - 动能延续型强势股（资源类、小盘成长股）不再被系统性过滤
#
# v17新增（相对v16）：
#   - 新增 _check_trend_persistence()：趋势持续性验证
#     ADX/+DI连续维持天数比例 + MA50斜率方向性，返回0~1分值
#     注入 tech["persistence_score"]，参与composite_score排序
#   - 新增 _check_higher_highs_lows()：价格结构验证
#     近40日「高点抬高+低点抬高」双重确认，作为T1/T2硬性条件
#   - 新增 _check_ma_alignment()：均线多头排列验证
#     MA20>MA50（全层级）+ T1/T2要求MA50>MA200，作为硬性条件
#   - build_tech_summary() 新增 _adx_s/_pdi_s/_mdi_s 三个内部字段
#     供 _check_trend_persistence() 使用，不对外展示
#   - SCORE_WEIGHTS 新增 persistence 维度（权重0.10）
#     其余权重等比下调，总和仍为1.0
#
# v18新增（相对v17，历史记录，部分内容已被v18.2取代）：
#   - 首次尝试将Gemini分析与SEO文章合并为单次批量调用，
#     该架构已在v18.2被用户否决，详见v18.2条目
#
# v18.1新增（相对v18，历史记录，部分内容已被v18.2取代）：
#   - 曾短暂加入seo_article_log表做跨日防重复叙事，已在v18.2彻底删除
#   - _write_seo_article_files() 新增 dir_en/dir_zh 可选参数，
#     支持写入测试目录而不触碰线上BLOG_CONTENT_DIR（v18.2保留）
#   - 新增 TEST_OUTPUT_DIR_EN/ZH + SEO_DRY_RUN 环境变量开关（v18.2保留）
#
# v18.2新增（相对v18.1，用户当轮反馈——当前实际架构）：
#   - 【架构核心改动】分析(Telegram)+信号JSON(GitHub) 与 SEO文章(GitHub)
#     彻底解耦为两条完全独立的流水线：
#     ① run_screener_flow()：逐只调用Gemini做深度分析→Telegram，
#        解析JSON标签字段→生成signals.json→立即push GitHub。
#        这是v17原有行为，此次基本原样恢复，确保信号更新最快最稳定，
#        不等待任何SEO文章调用
#     ② run_seo_article_flow()：Top1/Top2/Top3各自独立调用一次Gemini
#        （3次调用，每次1只股票，产出1篇英文+1篇中文SEO文章），
#        每只股票独立校验、独立commit推送，互不连累
#   - 不再有批量调用（v18.1的_build_seo_and_signal_prompt/
#     _parse_seo_signal_response/_validate_seo_fields/
#     run_seo_signal_flow 已重写为单只股票版本：
#     _build_seo_article_prompt/_parse_seo_article_response/
#     _validate_seo_article_fields/run_seo_article_flow）
#   - GEMINI_CFG_SEO_BATCH 重命名为 GEMINI_CFG_SEO_ARTICLE
#     （含义从"批量"变为"单篇"，参数值不变：max_output_tokens=65535，
#     thinking_budget=1024）
#   - push_to_github() 签名改为 (files: list, commit_message: str)，
#     不再隐式默认signals.json文件，调用方必须显式传入文件列表和
#     commit信息——避免"推signals.json"和"推SEO文章"两个独立场景
#     共享一个隐藏默认值造成耦合
#   - _parse_gemini_json_fields() 恢复（v18曾删除，v18.2按用户要求
#     恢复原v17逻辑），_build_screener_prompt() 末尾"固定输出字段"
#     段落同步恢复
#   - 任意一只股票的SEO文章生成失败（Gemini无响应/校验不过/写入异常），
#     除Telegram文字告警外，附带该股票专属的.txt Prompt供人工兜底，
#     不影响另外两只股票的正常发布
#   - seo_article_log表及配套跨日防重复叙事机制维持删除状态（v18.1已删）
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

# v18.1新增：SEO试运行开关。设置环境变量 SEO_DRY_RUN=1 后跑main()，
# run_seo_article_flow会走试跑模式（写测试目录+发Telegram审阅，
# 不碰GitHub；signals.json不受此开关影响，仍由run_screener_flow正常推送）。
# 调参阶段用这个开关，不用改代码。
SEO_DRY_RUN = os.environ.get("SEO_DRY_RUN", "0") == "1"

# ════════════════════════════════════════════════════════════
# 1. 常量 & 配置
# ════════════════════════════════════════════════════════════

GEMINI_MODEL    = "gemini-2.5-flash"
GEMINI_CFG_DEEP = {"thinking_config": {"thinking_budget": 512}}

# v18.2改动：不再批量调用，改为逐只股票单独调用（每次1篇英文+1篇中文），
# 单次输出量比v18的批量版（3只×双语）小得多，但依然保留生成的max_output_tokens
# 上限（Gemini 2.5 Flash官方文档记录的实际上限），进一步降低截断风险。
GEMINI_CFG_SEO_ARTICLE = {
    "thinking_config": {"thinking_budget": 1024},
    "max_output_tokens": 65535,
}

RETRY_MAX       = 20
RETRY_WAIT      = 30
TIMEOUT         = 15
TOP_N           = 3

ASXBOX_REPO  = os.path.expanduser("~/asxbox")
SIGNALS_DIR  = os.path.join(ASXBOX_REPO, "src", "data", "signals")

# v18新增：SEO文章目录（Astro content collection结构，已与用户确认）
BLOG_CONTENT_DIR_EN = os.path.join(ASXBOX_REPO, "src", "content", "blog", "en")
BLOG_CONTENT_DIR_ZH = os.path.join(ASXBOX_REPO, "src", "content", "blog", "zh")

# v18.1新增：测试模式专用目录，与线上BLOG_CONTENT_DIR完全隔离，
# 试运行时文章写在这里，绝不触碰ASXBOX_REPO的git工作区，
# 避免测试内容被意外扫进下一次真实commit
TEST_OUTPUT_DIR_EN = os.path.expanduser("~/asx_seo_test_output/en")
TEST_OUTPUT_DIR_ZH = os.path.expanduser("~/asx_seo_test_output/zh")

SEO_ARTICLE_MIN_CHARS = 600   # 正文最低字数（不含frontmatter），低于此视为生成失败
# v18.1：SEO_RECENT_LOOKBACK_DAYS 及 seo_article_log 防重复叙事机制已按用户要求删除

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

# ============================================================
# v18: SCORE_WEIGHTS 改为复用trend_strength_score，不再与其
# 重复计算rs_vs_xjo/adx14/vol_ratio。原因：两套体系衡量同一件
# 事但归一化区间不同，导致排序和筛选结果矛盾（真实数据验证：
# T2候选trend_strength更低但composite_score排序更高）。
# close_pos_pct已是_passes_tier()硬性条件，通过筛选后此项
# 已保证达标，排序时边际信息量低，故去除。
# ============================================================

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
        # ── 原始Series（供筛选函数使用，不对外展示）──────────────
        "_close"  : close,
        "_high"   : high,
        "_low"    : low,
        "_volume" : volume,
        # ── v17新增：ADX/DI原始Series供趋势持续性检测使用 ─────────
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


# ════════════════════════════════════════════════════════════
# v16新增：量能质量检查（替代原硬性缩量要求）
# ════════════════════════════════════════════════════════════

def _check_volume_quality(volume_s: pd.Series) -> bool:
    """
    量能质量检查，替代原来的 vol_decline 硬性缩量要求。

    允许两种模式通过（对应两类ASX强势股）：

      模式A — 缩量整理（原逻辑保留）
        近10日均量 < 前10日均量
        → 典型场景：大盘股、基本面驱动的蓄势突破

      模式B — 温和持续递增（v16新增）
        近10日均量适度高于前期（增幅 ≤ 80%），且：
        1. 近3日内无单日异常爆量（单日量 ≤ 近期均量3倍）
        2. 近5日量能方向性向上（线性回归斜率为正且斜率/均值 > 1%）
        → 典型场景：资源股、小盘成长股的动能延续型上涨

    拒绝的情况：
      - 单日脉冲爆量后快速萎缩（量能不可持续）
      - 近期量能随机震荡、无方向性
      - 近期均量增幅超过80%（可能是异常事件驱动而非趋势性放量）

    注意：T4层级的 vol_decline=False，不调用此函数，完全不检查量能模式。
    """
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

    # ── 模式A：缩量整理（原逻辑，直接通过）──────────────────────
    if ratio < 1.0:
        return True

    # ── 模式B：温和递增，以下三个条件必须全部满足 ────────────────

    # 条件1：增幅上限80%，排除异常事件驱动的爆量
    if ratio > 1.8:
        log.debug(f"_check_volume_quality: 量能增幅过大({ratio:.2f}x)，拒绝")
        return False

    # 条件2：近3日内无单日爆量脉冲
    max_single = float(vol_last3.max())
    if recent_mean > 0 and max_single > recent_mean * 3.0:
        log.debug(f"_check_volume_quality: 单日脉冲检测触发({max_single:.0f} > {recent_mean*3:.0f})，拒绝")
        return False

    # 条件3：近5日量能方向性检查（线性回归斜率）
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


# ════════════════════════════════════════════════════════════
# v17新增：趋势持续性验证 / 价格结构验证 / 均线多头排列验证
# ════════════════════════════════════════════════════════════

def _check_trend_persistence(close: pd.Series,
                              adx_s: pd.Series,
                              pdi_s: pd.Series,
                              mdi_s: pd.Series) -> float:
    """
    趋势持续性验证：不是看今天的快照，而是看这个趋势维持了多久。

    逻辑：
      同样 ADX=30 的两只股票，一只刚刚突破28，另一只已持续3周在30以上，
      后者的趋势可靠性远高于前者。

    计分维度（满分1.0）：
      1. ADX过去10日持续高于20的天数比例  → 最高贡献0.40
      2. +DI过去10日持续大于-DI的天数比例 → 最高贡献0.40
      3. MA50过去20日斜率方向性（线性回归）→ 最高贡献0.20

    返回值：persistence_score（0.0~1.0），注入composite_score参与排序。
    不作为硬性过滤条件，避免过度收窄信号池。
    """
    score = 0.0

    # ── 维度1：ADX持续性（过去10日）────────────────────────────
    try:
        adx_10 = adx_s.iloc[-10:].dropna()
        if len(adx_10) >= 5:
            adx_persistence = float((adx_10 > 20).sum()) / len(adx_10)
            score += adx_persistence * 0.40
    except Exception:
        pass

    # ── 维度2：+DI > -DI 持续性（过去10日）──────────────────────
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

    # ── 维度3：MA50斜率方向性（过去20日线性回归）────────────────
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
                    # 斜率为正且有实质意义（>0.05%/日）才加分
                    if slope > 0 and slope_pct > 0.0005:
                        score += 0.20
    except Exception:
        pass

    return round(min(1.0, score), 3)


def _check_higher_highs_lows(high: pd.Series,
                              low: pd.Series,
                              lookback: int = 40) -> bool:
    """
    价格结构验证：近40日是否存在「高点抬高 + 低点抬高」双重确认。

    这是上升趋势的价格本质定义（道氏理论）：
      - Higher High：后20日最高点 > 前20日最高点
      - Higher Low ：后20日最低点 > 前20日最低点
      两者必须同时满足，排除单边拉升后低点下移的假趋势。

    作为T1/T2的硬性过滤条件。
    T3/T4不要求，避免过度收窄信号池。
    """
    if len(high) < lookback or len(low) < lookback:
        return False

    mid = lookback // 2  # = 20

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
    """
    均线多头排列验证：MA20 > MA50 > MA200（层级递进要求）。

    多头排列是机构资金持续流入的结构性证明，比单看价格和MA50更可靠。

    规则：
      全层级（T1-T4）：MA20 > MA50（短期趋势确认）
      T1/T2额外要求  ：MA50 > MA200（中长期趋势确认，MA200数据不足时豁免）

    MA200数据不足200日时，T1/T2不因此被拒绝，仅豁免该条件，
    避免上市不足一年的优质新股被系统性过滤。
    """
    ma20  = tech.get("ma20",  0.0)
    ma50  = tech.get("ma50",  0.0)
    ma200 = tech.get("ma200")  # 可能为None（数据不足200日）

    # 基础条件：MA20 > MA50（全层级硬性要求）
    if ma20 <= ma50:
        log.debug(f"_check_ma_alignment: MA20({ma20:.3f}) <= MA50({ma50:.3f})，拒绝")
        return False

    # T1/T2额外要求：MA50 > MA200
    if tier_level in ("T1", "T2") and ma200 is not None:
        if ma50 <= ma200:
            log.debug(f"_check_ma_alignment [{tier_level}]: MA50({ma50:.3f}) <= MA200({ma200:.3f})，拒绝")
            return False

    return True

# ============================================================
# 趋势强度综合评分 v2 —— 修复"四个tier分布几乎一样"的缺陷
#
# v1问题（已用真实数据验证）：只有volume_multiple一项的归一化区间
# 随tier变化，其余6项区间写死，导致T1-T4的trend_strength_score
# 均值/中位数/标准差几乎完全一致（相差<0.02），四个层级实质上
# 变成了同一个筛选器，违背"T1最严格、T4最宽松"的分层设计初衷。
#
# 修复：让全部7项的归一化区间都参照原v17硬性门槛参数，
# 按tier动态生成，而不是只有一项动态、其余写死。
# ============================================================

def _norm(val: float, lo: float, hi: float) -> float:
    """线性归一化到0-1，超出范围截断"""
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


def calc_trend_strength_score(tech: dict, tier: dict) -> dict:
    """
    计算7个强相关条件的加权综合分数，返回0-1的trend_strength_score。

    v2改动：全部7项归一化区间均参照原v17硬性门槛动态生成，
    确保T1（严格tier）在每一项上的评分标准都比T4（宽松tier）更严苛，
    而不是只有volume_multiple一项体现tier差异。

    区间生成规则：以原v17硬性门槛为"评0.5分"的锚点，
    向上/向下各扩展一定幅度作为0分/1分的边界，
    这样阈值校准出来的分数在语义上更接近"是否达到原硬性标准附近"。
    """
    lc     = tech["price"]
    w52_hi = tech["w52_hi"]

    scores = {}

    # 1. volume_multiple：锚点=tier["vol_mult"]，下限0.5倍锚点，上限1.5倍锚点
    vol_anchor = tier["vol_mult"]
    scores["volume_multiple"] = _norm(
        tech.get("vol_ratio", 1.0), vol_anchor * 0.5, vol_anchor * 1.5
    )

    # 2. near_52w_hi：锚点=tier["near_52w_hi"]对应的门槛
    #    v17里near_52w_hi是布尔值(T1/T2=True要求>=90%，T3/T4=False无要求)，
    #    转换成动态锚点：True→0.90，False→0.75（仍给一定权重，但标准更松）
    dist_ratio = lc / w52_hi if w52_hi > 0 else 0.7
    near_hi_anchor = 0.90 if tier["near_52w_hi"] else 0.75
    scores["near_52w_hi"] = _norm(dist_ratio, near_hi_anchor - 0.15, near_hi_anchor + 0.10)

    # 3. ma_alignment：MA20相对MA50溢价。T1/T2额外要求MA50>MA200（更严格），
    #    用tier level间接调整锚点：T1/T2锚点更高
    ma20 = tech.get("ma20", 0)
    ma50 = tech.get("ma50", 1)
    ma_premium = (ma20 / ma50 - 1) if ma50 > 0 else 0
    ma_anchor  = 0.02 if tier["level"] in ("T1", "T2") else 0.0
    scores["ma_alignment"] = _norm(ma_premium, ma_anchor - 0.03, ma_anchor + 0.05)

    # 4. relative_strength：锚点=tier["rs_min"]（原v17硬性门槛，T1=1.05...T4=0.98）
    rs_anchor = tier["rs_min"]
    scores["relative_strength"] = _norm(tech.get("rs_vs_xjo", 1.0), rs_anchor - 0.10, rs_anchor + 0.15)

    # 5. hh_hl_structure：T1/T2原本是硬性要求（必须同时HH+HL），
    #    T3/T4不要求。锚点体现这个差异
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

    # 6. ma50_trend：MA50斜率，用tier的adx_min间接反映"要求趋势有多强"
    #    ADX门槛越高的tier，对趋势斜率的要求也应该越高
    close_s = tech["_close"]
    slope_anchor = (tier["adx_min"] - 15) / 100  # T1(28)→0.13%，T4(15)→0%
    if len(close_s) >= 61:
        ma50_now  = float(close_s.rolling(50).mean().iloc[-1])
        ma50_prev = float(close_s.rolling(50).mean().iloc[-11])
        ma50_chg  = (ma50_now / ma50_prev - 1) if ma50_prev > 0 else 0
        scores["ma50_trend"] = _norm(ma50_chg, slope_anchor - 0.02, slope_anchor + 0.05)
    else:
        scores["ma50_trend"] = 0.0

    # 7. vwap_position：锚点固定（vwap_above在v17里全层级都要求True，
    #    没有tier间差异，保持原样即可）
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

# v18.2恢复：分析Gemini调用重新自己解析JSON标签字段（v17原有逻辑），
# 与SEO文章生成彻底解耦——signals.json的更新不再等待任何SEO调用。
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
    """
    v18.2改动：不再隐式默认signals.json文件，改为调用方显式传入
    要提交的文件相对路径列表和commit信息。原因：现在有两种完全独立的
    调用场景——① run_screener_flow推signals.json（最高优先级，第一时间
    更新）② run_seo_article_flow逐只推单只股票的md文件——两者不应该
    共享一个隐藏的"默认总是带上signals.json"假设，否则两个场景互相
    耦合，任何一边改动都可能悄悄影响另一边。
    """
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
        cmds = [
            ["git", "-C", ASXBOX_REPO, "pull", "--no-rebase", "origin", branch],
            ["git", "-C", ASXBOX_REPO, "add"] + files,
            ["git", "-C", ASXBOX_REPO, "commit", "-m", commit_message],
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
                    # 修复：达到重试上限仍无有效市值时，直接break跳出循环，
                    # 走向函数末尾的统一兜底return。原代码这里没有break，
                    # 会继续执行下面的return语句，用值为0/None的market_cap
                    # 计算round(market_cap / 1_000_000, 1)——如果market_cap是
                    # None（yfinance偶发返回None而非0），会触发TypeError，
                    # 虽然被外层except捕获不会导致程序崩溃，但会产生
                    # 误导性的重复日志，且逻辑本身不清晰。
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
            # v18.1：seo_article_log表已按用户要求删除（不再做跨日防重复叙事）
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
        sig       = a.get("significance", 0)
        flag      = "⭐" if a["sensitive"] else "📋"
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
# v18新增：SEO文章辅助函数（安全文件名清洗）
# v18.1：_get_recent_seo_angles / _save_seo_article_log 已按用户要求删除
# ════════════════════════════════════════════════════════════

def _extract_frontmatter_title(md_content: str) -> str:
    """从生成的Markdown里提取title字段，仅用于日志记录，失败不影响主流程。"""
    m = re.search(r'title:\s*"([^"]*)"', md_content)
    return m.group(1) if m else ""


def _sanitize_slug(raw: str) -> str:
    """
    文件名安全清洗：绝不直接信任Gemini输出的字符串去拼文件路径，
    只保留小写字母/数字，其余一律转连字符，防止路径穿越或非法文件名。
    """
    s = raw.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:60]


# ════════════════════════════════════════════════════════════
# 4. Gemini
# ════════════════════════════════════════════════════════════

def ask_gemini(prompt: str, label: str = "", config: Optional[dict] = None,
               return_meta: bool = False):
    """
    调用Gemini生成内容。

    v18新增两个可选参数（向后兼容，不传时行为与v17完全一致）：
      config      ：覆盖默认的GEMINI_CFG_DEEP，SEO文章调用使用独立的
                    GEMINI_CFG_SEO_ARTICLE（更大的max_output_tokens）
      return_meta ：True时返回(text, finish_reason, thoughts_tokens,
                    output_tokens)四元组，用于诊断MAX_TOKENS截断——
                    Gemini 2.5系列的thinking token与正文token共享
                    同一个max_output_tokens预算池，且thinking_budget
                    不保证被严格遵守，一旦真的触发截断，这里能给出
                    精确的token消耗数字，而不是靠正则猜测。
                    False（默认）时只返回文本字符串，与旧调用方完全兼容。
    """
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
            if any(k in err for k in ("429", "503", "RESOURCE_EXHAUSTED", "overloaded", "quota")):
                if attempt < RETRY_MAX:
                    log.warning(f"Gemini限速 [{label}] {attempt}/{RETRY_MAX}，{RETRY_WAIT}s后重试...")
                    time.sleep(RETRY_WAIT)
                else:
                    log.error(f"Gemini [{label}] 达到10分钟上限，放弃")
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

    tier_bonus_val = TIER_BONUS.get(t.get("tier_level", ""), 0.0)

    tech_block = (
        f"价格:{t['price']}({t['change_pct']:+.2f}%) | "
        f"1年历史分位:{pct_1y_str} | {vol_c_str}\n"
        f"MA20:{t['ma20']} MA50:{t['ma50']} {ma200_str}\n"
        f"RSI:{t['rsi14']} ADX:{t['adx14']} +DI:{t['plus_di']} -DI:{t['minus_di']}\n"
        f"VWAP20:{'上升' if t['vwap_up'] else '下降'} 量比:{t['vol_ratio']}x\n"
        f"RS(vsXJO):{t['rs_vs_xjo']} ATR:{t['atr14_pct']}% "
        f"52W高:{t['w52_hi']}(距{t['dist_52w_hi_pct']}%) 低:{t['w52_lo']}\n"
        f"近6月最大回撤:{t['max_dd_6m_pct']}%\n"
        f"趋势强度评分:{t.get('trend_strength_score', 'N/A')}"
        f"（筛选/排序核心依据，0-1分，{tier_label}门槛为{TREND_SCORE_THRESHOLD.get(t.get('tier_level',''), 'N/A')}）\n"
        f"趋势持续性:{t.get('persistence_score', 0.0)}（趋势维持时长，非当前强度）\n"
        f"综合评分:{t.get('composite_score', 'N/A')}"
        f"（已含{tier_label}层级加成+{tier_bonus_val}，不同层级间的绝对值不可直接横向比较）"
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
    ma200_str  = f"MA200:{tech['ma200']}" if tech.get("ma200") else "MA200:N/A"
    vol_c_str  = "量能连续递增✅" if tech.get("vol_consistency") else "量能无持续性"
    pct_1y_str = f"{tech.get('price_pct_1y', 50)}%分位(1年)"
    pe         = tech.get("price_events", [])
    pe_str     = " | ".join(
        f"{e['date']}:{'+' if e['change_pct'] > 0 else ''}{e['change_pct']}%"
        for e in pe[:5]
    ) if pe else "近6个月无大幅波动"

    trend_score = tech.get("trend_strength_score")
    trend_score_str = (f"{trend_score}" if trend_score is not None
                       else "N/A（非screener筛选信号，见下方降级模式说明）")

    composite = tech.get("composite_score")
    tier_level = tech.get("tier_level", "")
    if composite is None:
        composite_str = "N/A"
        bonus_note = "（降级模式：大盘红灯或涨幅榜数据，未经tier筛选，无有效综合评分）"
    else:
        composite_str = f"{composite}"
        tier_bonus_val = TIER_BONUS.get(tier_level, 0.0)
        bonus_note = f"（已含层级加成+{tier_bonus_val}）"

    tech_line = (
        f"价格:{tech['price']}({tech['change_pct']:+.2f}%) {pct_1y_str} {vol_c_str}\n"
        f"MA50:{tech['ma50']} {ma200_str} RSI:{tech['rsi14']} ADX:{tech['adx14']} "
        f"量比:{tech['vol_ratio']}x RS:{tech['rs_vs_xjo']} "
        f"ATR:{tech['atr14_pct']}% 52W高:{tech['w52_hi']}(距{tech['dist_52w_hi_pct']}%)\n"
        f"近6月最大回撤:{tech['max_dd_6m_pct']}%\n"
        f"趋势强度评分:{trend_score_str} | 趋势持续性:{tech.get('persistence_score', 0.0)}\n"
        f"综合评分:{composite_str}{bonus_note}\n"
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


# ════════════════════════════════════════════════════════════
# v18新增：SEO文章 + 信号JSON 合并批量Prompt构建 / 解析 / 校验 / 写入
# ════════════════════════════════════════════════════════════

def _build_seo_article_prompt(market_snap: dict, stock_package: dict) -> str:
    """
    v18.2改动：单只股票独立调用（不再批量3只一次性调用）。
    信号标签字段（JSON_TAG/ONE_LINER）已不再由这里产出——那部分已经
    恢复到_build_screener_prompt()里，由screener分析调用负责。
    这里只负责一件事：一篇英文SEO文章 + 一篇中文SEO文章 + 一个slug。
    结构要求（frontmatter/8章节/FAQ/风格铁律/硬性约束/自检环节）
    完整保留，不因为改成单只调用而删减。
    """
    sector_str = "、".join(
        f"{s}({p:+.1f}%)" for s, p in market_snap.get("sector_leaders", [])
    ) or "数据暂缺"
    status_map = {
        "red": "大幅下跌⚠️", "yellow": "轻微走弱",
        "bullish": "强势上涨", "normal": "窄幅震荡",
    }
    market_block = (
        f"Date: {market_snap.get('date', date.today().isoformat())}\n"
        f"ASX200: {market_snap.get('xjo_close',0)} "
        f"({market_snap.get('xjo_change_pct',0):+.2f}%, "
        f"{status_map.get(market_snap.get('market_status','normal'),'normal')})\n"
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
3. Verify the format meets the criteria of md files and output exactly one EN article + one ZH article + one slug.
4. Verify every marker above is spelled exactly as specified.
5. Skip any explanation of this process — output ONLY the final version, using the exact
   marker format above."""


def _parse_seo_article_response(raw_text: str, ticker: str,
                                 gemini_meta: Optional[dict] = None) -> dict:
    """
    v18.2改动：单只股票解析，不再需要按ticker拆block（每次调用本来就只有
    一只股票）。只提取slug/英文文章/中文文章，JSON标签字段不再由这里产出。

    gemini_meta（可选）：{"finish_reason", "thoughts_tokens", "output_tokens"}
    用于在marker缺失时给出精确失败原因，而不是"疑似截断"这种猜测性描述。
    """
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
    """
    任何一项不通过 → 这只股票的文章本次跳过，绝不用半成品覆盖GitHub。
    v18.2改动：去掉JSON_TAG/ONE_LINER相关校验（这些字段已不在这次调用产出）。
    """
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

def _write_seo_article_files(ticker: str, slug: str, content_en: str, content_zh: str,
                              dir_en: Optional[str] = None, dir_zh: Optional[str] = None) -> tuple:
    """
    文件名格式（已与用户确认，固定不变）：
        {YYYY-MM-DD}-{完整ticker含.AX}-{slug}.md
        例：2026-07-07-BHP.AX-iron-ore-rally-continuation.md
    中英文文件用完全相同的文件名，只是分别存放在不同目录。
    文件名不依赖Gemini输出的整体格式——Gemini只负责生成slug这一个
    3-6个单词的短语（通过SEO_SLUG marker提取），日期和ticker都是
    外部代码拼装的，slug本身还会经过_sanitize_slug()二次清洗
    （只保留小写字母数字和连字符），所以最终文件名的正确性不依赖
    Gemini有没有守规矩。

    v18.1新增dir_en/dir_zh参数：不传时使用线上真实目录
    （BLOG_CONTENT_DIR_EN/ZH）；试运行(dry_run)时由调用方传入
    TEST_OUTPUT_DIR_EN/ZH，两套目录完全隔离。
    """
    dir_en = dir_en or BLOG_CONTENT_DIR_EN
    dir_zh = dir_zh or BLOG_CONTENT_DIR_ZH
    filename = f"{date.today().isoformat()}-{ticker}-{slug}.md"
    os.makedirs(dir_en, exist_ok=True)
    os.makedirs(dir_zh, exist_ok=True)
    path_en = os.path.join(dir_en, filename)
    path_zh = os.path.join(dir_zh, filename)
    with open(path_en, "w", encoding="utf-8") as f:
        f.write(content_en)
    with open(path_zh, "w", encoding="utf-8") as f:
        f.write(content_zh)
    return path_en, path_zh


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

- Each stock must contain EXACTLY 4 tweets
- Each TWEET must be wrapped in its own triple backtick code block
- No text outside code blocks
- Clean, copy-ready format

If multiple stocks exist:

* Output stock A (4 tweets/ 4 code blocks)
* then stock B (4 tweets/ 4 code blocks)
* then stock C (4 tweets/ 4 code blocks)

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
- Key event (announcement / update / news)
- Immediate reason market is repricing

TWEET 2 — CORE DRIVER + EXPECTATION SHIFT (COMBINED)
- What fundamentally changed (growth / margins / balance sheet)
- BEFORE vs AFTER market expectation (must be explicit delta in your own words)

TWEET 3 — FLOW + POSITIONING
- Who is likely involved (for example: funds / retail / momentum / short covering)
- Type of flow （for example: new money / continuation / re-rating / squeeze）

TWEET 4 — OUTCOME + TREND
- Sustainability of narrative
- Final directional bias (early / mid / late phase repricing)
- Short/Medium/Long-Term Trend Analysis

Tweet 1 can be structured.
Tweets 2–4 must explicitly avoid any pattern that could be interpreted as formatting.
Each tweet must be ≤280 characters; no multi-paragraph or multi-point construction.

--------------------------------------------------

🔧 CRITICAL COMPRESSION RULE (MANDATORY)

Under NO circumstance can tweet count exceed 4.

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

- Max 1–2 uncertainty expressions total
- At least 1 emotional reaction (e.g. “feels crowded”, “not clean”, “too smooth”)
- At least 1 incomplete thought
- At least 1 subtle contradiction
- Conviction must vary across tweets

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
2、每一个盲盒（股票）用150字以内的字数，按照备注3的语言风格，以主观的视角复述一次草稿，作为输出的文案。
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

    return f"""📋 <b>ASX日报Prompt — {platform.upper()} — {market_snap.get('date','')}</b>

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

# ============================================================
# _passes_tier() 最终替换版
#
# 改动：原有7个高相关硬性条件(volume_multiple/near_52w_hi/
# ma_alignment/relative_strength/hh_hl_structure/ma50_trend/
# vwap_position)合并为trend_strength_score加权评分。
#
# 保留硬性AND的条件（"能不能交易"问题，不是"趋势强弱"问题）：
# liquidity / consolidation / volume_quality / di_direction /
# adx_strength / rsi_range / close_position
#
# 阈值来源：用真实ASX全市场1628只股票数据反推校准
# （2026-07-04诊断，trend_strength_score v2版本）
# ============================================================

TREND_SCORE_THRESHOLD = {
    "T1": 0.45,   # trend_strength_score层面通过率约7.6%（123/1628）
    "T2": 0.40,   # 约12.8%（209/1628）
    "T3": 0.35,   # 约31.0%（504/1628）
    "T4": 0.30,   # 约45.5%（740/1628）
}
# 注：以上是trend_strength_score单独层面的通过率，
# 叠加liquidity/consolidation/volume_quality/di_direction/
# adx_strength/rsi_range/close_position这7个硬性条件后，
# 最终真实通过率会显著更低，需要跑一次完整run_screener_flow()
# 验证实际效果，而不是只看这一层的数字。

def _passes_tier(tech: dict, tier: dict) -> bool:
    lc        = tech["price"]
    vol_ratio = tech["vol_ratio"]
    close_pos = tech["close_pos_pct"] / 100.0
    volume_s  = tech["_volume"]
    high_s    = tech["_high"]
    low_s     = tech["_low"]

    # ── 硬性条件1：最低流动性（"能不能交易"问题，不参与评分）──────
    if float(volume_s.iloc[-20:].mean()) * lc < 300_000:
        return False

    # ── 硬性条件2：整理幅度（防止追涨过高风险位）──────────────────
    r15 = pd.concat([high_s, low_s], axis=1).iloc[-15:]
    pr  = (float(r15.iloc[:, 0].max()) - float(r15.iloc[:, 1].min())) / lc
    if pr > tier["consol"]:
        return False

    # ── 硬性条件3：v16量能质量检查（排除爆量脉冲/无方向震荡）──────
    if tier["vol_decline"]:
        if not _check_volume_quality(volume_s):
            return False

    # ── 硬性条件4：DI方向（趋势方向性的基础判断，不模糊化）────────
    if tier["di_cross"] and tech["plus_di"] <= tech["minus_di"]:
        return False

    # ── 硬性条件5：ADX强度门槛（是否存在明确趋势）─────────────────
    if tech["adx14"] < tier["adx_min"]:
        return False

    # ── 硬性条件6：RSI区间（防止追高/抄底两个极端）────────────────
    if not (tier["rsi_lo"] <= tech["rsi14"] <= tier["rsi_hi"]):
        return False

    # ── 硬性条件7：收盘位置（当天买盘强度，短周期信号）────────────
    if close_pos < tier["close_pos"]:
        return False

    # ── 趋势强度综合评分（替代原7个高相关硬性AND条件）─────────────
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

    # 市值过滤：Top10候选池全部要求通过（不只是Top3），
    # 因为Top4-Top10现在也要进入watchlist做盘中监测，
    # 同样需要满足最低市值门槛，避免监测流动性太差的股票
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

    # Top3：仍然是市值过滤后的候选池里排名最前的3只，
    # 用于JSON推送/SEO文章/Gemini分析/详细Telegram，逻辑不变
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

        # 改动：watchlist写入范围从signals（Top3）扩大到
        # filtered_pool（市值过滤后的Top10全部），
        # Top1-Top10同等对待，不做优先级区分
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
    """
    记录本次运行的T1-T4筛选质量诊断信息到独立日志文件。
    """
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
    """
    诊断版本的_passes_tier，不做提前return，
    而是记录每一个条件的通过/失败情况，返回完整字典。
    
    用途：临时诊断工具，找出T1/T2为什么几乎不触发。
    正式筛选逻辑仍用_passes_tier()，不受影响。
    """
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
        checks["volume_quality"] = True  # T4不检查，视为通过

    checks["ma_alignment"] = _check_ma_alignment(tech, tier["level"])

    if tier["level"] in ("T1", "T2"):
        checks["hh_hl_structure"] = _check_higher_highs_lows(high_s, low_s)
    else:
        checks["hh_hl_structure"] = True  # T3/T4不要求

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
    """
    诊断工具：对全市场股票跑指定tier的诊断版筛选，
    统计每个条件的失败率，找出系统性瓶颈。
    
    独立运行，不影响正常的run_screener_flow()流程。
    """
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
    """
    阈值校准工具：计算全市场股票的trend_strength_score分布，
    输出不同阈值下各tier的通过率，供人工选定合适阈值。

    这是一次性诊断工具，用真实市场数据反推TREND_SCORE_THRESHOLD，
    不是凭空猜测的数字。

    注意：此函数只计算trend_strength_score本身的分布，
    不叠加liquidity/consolidation等其他硬性条件，
    这样能单独看清楚这一个评分维度的真实区分能力。
    """
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
    """
    v18.2：T1-T4筛选 + Gemini逐只深度分析 → Telegram + 解析JSON标签字段
    → 生成signals.json → push GitHub。这是v17原有行为的恢复，与SEO文章
    生成（run_seo_article_flow）彻底解耦——signals.json的更新不等待
    任何SEO调用，保证信号发布是最快最稳定的一环。
    """
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

    # Top3逐只Gemini深度分析（Telegram阅读 + 解析JSON标签字段，
    # 用于下面生成signals.json；SEO文章由run_seo_article_flow单独负责）
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

        # v18.2恢复：JSON标签字段解析+confidence计算，回到v17原有逻辑，
        # 与SEO文章生成彻底解耦，signals.json不再等待任何SEO调用
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

    # v18.2恢复：signals.json生成 + GitHub推送，独立于SEO文章，
    # 最高优先级，第一时间更新，不等任何SEO调用结果
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


def run_seo_article_flow(signals: list, market_snap: dict, dry_run: bool = False) -> None:
    """
    v18.2重写：不再是单次批量调用，改为Top1/Top2/Top3各自独立调用Gemini
    （3次调用，每次1只股票，产出1篇英文+1篇中文SEO文章）。

    与信号JSON彻底解耦：signals.json的生成和推送已经在run_screener_flow()
    里完成，本函数只负责文章，不再产出/依赖任何JSON标签字段。

    每只股票独立处理、独立commit：
      - 某只股票的Gemini调用/校验失败 → 该股票单独跳过+Telegram告警，
        并把该股票专属的prompt原文作为.txt附件发送，供人工手动补救；
        不影响其他两只股票的正常发布
      - 某只股票成功 → 立即写文件+push，不等其他股票

    dry_run=True时：写入本地测试目录（TEST_OUTPUT_DIR_EN/ZH），文章原文
    直接发Telegram供审阅，不推送GitHub。
    """
    if not signals:
        log.info("run_seo_article_flow: 无signals，跳过（大盘红灯或T1-T4均无候选）")
        return

    today      = date.today().isoformat()
    today_ann  = fetch_today_announcements()
    mode_label = "🧪 测试模式" if dry_run else "正式模式"
    log.info(f"=== SEO文章逐只生成流程启动（{mode_label}） ===")

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
            if dry_run:
                path_en, path_zh = _write_seo_article_files(
                    ticker, fields["slug_clean"], fields["seo_en_raw"], fields["seo_zh_raw"],
                    dir_en=TEST_OUTPUT_DIR_EN, dir_zh=TEST_OUTPUT_DIR_ZH,
                )
                fname = os.path.basename(path_en)
                send_document(fname, fields["seo_en_raw"], caption=f"🧪 {ticker} 英文文章试跑结果")
                send_document(fname, fields["seo_zh_raw"], caption=f"🧪 {ticker} 中文文章试跑结果")
                send_telegram(f"🧪 [Top{rank} {ticker}] 试跑完成，未推送GitHub，写入测试目录：{path_en}")
                success_count += 1
            else:
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

    log.info(f"run_seo_article_flow完成（{mode_label}）：{success_count}/{len(signals)} 只成功")

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

    # v18改动：平台列表移除"seo"——SEO文章已由run_seo_article_flow
    # 直接调用Gemini生成并自动推送GitHub，不再需要手动Prompt流程
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
    log.info(f"ASX System v18 启动 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
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

    # Step 3：选股流程（仅生成Telegram分析，不再产出JSON/SEO）
    screener_signals = []
    if status != "red":
        log.info("【Step 3】选股筛选流程...")
        screener_signals = run_screener_flow(all_data, market_snap)
    else:
        log.info("【Step 3】大盘红灯，跳过选股")

    # Step 3.5：SEO文章逐只生成并推送GitHub（v18.2：与Step3的signals.json
    # 彻底解耦，signals.json在Step3已经推送完毕，这里只负责文章）
    if screener_signals:
        mode_note = "（🧪 SEO_DRY_RUN=1，试跑模式，不碰GitHub）" if SEO_DRY_RUN else ""
        log.info(f"【Step 3.5】SEO文章逐只生成流程...{mode_note}")
        run_seo_article_flow(screener_signals, market_snap, dry_run=SEO_DRY_RUN)
    else:
        log.info("【Step 3.5】无screener信号，跳过SEO文章生成，网站保持昨日数据")

    # Step 4：日报Prompt（Twitter/小红书，人工使用）
    log.info("【Step 4】日报Prompt生成流程...")
    run_report_flow(all_data, market_snap,
                    screener_signals=screener_signals if screener_signals else None)

    elapsed = round((time.time() - start) / 60, 1)
    log.info("=" * 60)
    log.info(f"ASX System 全部完成，总耗时：{elapsed} 分钟")
    log.info("=" * 60)
    send_telegram(
        f"🏁 <b>ASX System v18 全部完成</b>\n"
        f"📅 {date.today().isoformat()} | ⏱ 总耗时：{elapsed} 分钟\n"
        f"选股：{'跳过（大盘红灯）' if status == 'red' else f'Top{len(screener_signals)}已完成'}\n"
        f"SEO文章/信号JSON：{'已生成，见上方推送结果' if screener_signals else '跳过'}\n"
        f"日报Prompt：Twitter / 小红书 已发送"
    )


if __name__ == "__main__":
    main()
