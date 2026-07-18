# ============================================================
# ASX SYSTEM — intraday_monitor.py  v3
#
# 长期盘中监测：监测 screener.py (EOD) 筛选出的股票，
# 在监测期内（按筛选等级T1-T4分配天数，可累加）每15分钟扫描一次，
# 判断四种入场模式是否触发，仅在有信号时推送Telegram。
#
# v3改动（相对v2，解决"止损用固定比例、跟仓位计算脱节"的问题）：
#   1. 新增真实ATR14计算，复用lock_daily_reference()已下载的
#      日线数据（不多花yfinance请求），跟prior_high_20d/
#      pullback_recent_low一样缓存到watchlist_db，每天算一次。
#   2. 模式1/2的止损从"固定×0.995"改成"结构性参考位 − ATR×1.5"
#      （ATR_STOP_MULTIPLIER跟daily_analysis.py保持同一数值/
#      同一风控哲学）。模式4维持不变——它的止损是回调最低点本身，
#      daily_analysis.py原本设计就没加缓冲，不该在这里另搞一套。
#      ATR算不出来时（数据不足），自动退回v2的固定比例逻辑，
#      不阻断信号。
#   3. 新增仓位计算：之前v1/v2版本触发信号时只给止损价和目标价，
#      从没算过"这笔该买多少股"——实际执行时只能翻回daily_analysis.py
#      盘前用ATR算出来的仓位，但那个仓位对应的止损距离，跟
#      intraday这边实际触发的止损可能对不上（同一份资金风险预算，
#      被两个不同的止损距离各自套用了一遍）。v3在触发信号的当下，
#      用这个模式实际的止损距离重新算一次仓位，跟止损保持一致。
#
# 四种模式（均为15分钟K线级别的"代理判断"，非逐笔tick级）：
#   模式1 突破瞬间买：15分钟K线收盘突破prior_high_20d + 放量 + 未被砸回
#   模式2 回踩确认买：突破后（可跨天）回踩缩量企稳，重新拉升的那一根K线
#   模式3 尾盘确认买（T+1）：15:30-15:45时段维持强势，次日开盘附近了结
#   模式4 回调确认买：健康回调触底反弹后，当日延续确认
#
# 状态路由（与daily_analysis.py的今日跨日状态一一对应）：
#   ready              → 模式1/2/3（突破轨道）
#   pullback_bottoming → 模式4（回调轨道）
#   pullback_healthy / watch / caution / accumulating → 跳过，只积累快照
#   unknown / stale    → 降级只跑突破轨道（模式4依赖当天确认的
#                        pullback_bottoming状态才有意义，分析层
#                        不可用时没有这个前提，不运行）
#
# 设计基线（与screener.py保持一致的风控/数据规范）：
#   - 跳过开盘集合竞价噪音（09:30前）和收盘集合竞价（16:00后）
#   - 流动性门槛与screener.py一致（避免低基数下的"虚假放量"）
#   - 所有基准位（前高/量能均值/回调参考位/ATR14）在每个交易日
#     开盘后锁定一次，全天不变，防未来函数
#   - 健康度检查：监测期内若股票转弱，提前清出队列，不死板跑满天数
#   - 每次轮询写入intraday_snapshots，供历史比对和回测
#
# 运行方式：crontab每15分钟触发一次，脚本内部自行判断是否在交易时段内
# ============================================================

import os
import sys
import time
import logging
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import watchlist_db as wdb

# ════════════════════════════════════════════════════════════
# 0. 日志 & 环境变量
# ════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("intraday_monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")

SYD_TZ = ZoneInfo("Australia/Sydney")

# ════════════════════════════════════════════════════════════
# 1. 常量配置
# ════════════════════════════════════════════════════════════

TIMEOUT = 15

MARKET_OPEN          = "10:00"
SESSION_START_SAFE   = "10:15"
MARKET_CLOSE         = "16:00"
LATE_SESSION_START   = "15:30"
LATE_SESSION_END     = "15:45"

MIN_DOLLAR_VOLUME_INTRADAY = 300_000

BREAKOUT_LOOKBACK_DAYS   = 20
VOL_SPIKE_RATIO_M1       = 1.8
VOL_SPIKE_RATIO_HIST     = 1.5
BREAKOUT_FAILURE_PCT     = -0.3

# 模式2专属（此前叫PULLBACK_MAX_DEPTH_PCT/PULLBACK_VOL_SHRINK_RATIO，
# v2改名加MODE2_前缀——因为模式4也要用"回调深度/缩量比"这类概念，
# 但数值含义完全不同（模式2是浅回踩4%以内，模式4是深回调8%-25%），
# 同名不同义容易混淆甚至误改，改名区分）
MODE2_PULLBACK_MAX_DEPTH_PCT    = 4.0
MODE2_PULLBACK_VOL_SHRINK_RATIO = 0.7
# v2新增：模式2允许"最近突破"回溯的天数上限。超过这个天数，
# 即使item里还留着last_breakout_date，也不再当作有效的模式2前提
# ——时间太久，prior_high_20d大概率已经跟着行情滚动了很多，
# 用一个过旧的"突破事件"去触发回踩确认意义不大。
# 用日历天数近似（非严格交易日计数），周末/假期会有小幅出入，可接受。
MODE2_BREAKOUT_LOOKBACK_DAYS    = 5

LATE_SESSION_NEAR_HIGH_PCT = 1.5
LATE_SESSION_MIN_VOL_RATIO = 0.8

HEALTH_MIN_RS_VS_XJO = 0.97
HEALTH_BELOW_MA50_GRACE_DAYS = 2

# 模式4专属（回调确认买，配合daily_analysis.py的pullback_bottoming状态）
# ⚠️ 以下三个深度/窗口相关常量，数值必须手动跟daily_analysis.py里
# 同名常量（PULLBACK_MIN_DEPTH_PCT/PULLBACK_MAX_DEPTH_PCT/
# PULLBACK_LOOKBACK_DAYS）保持一致——两边是独立实现，不共享代码，
# 改一处记得改另一处，否则两个文件对"什么算健康回调"的定义会
# 逐渐不同步。
MODE4_PULLBACK_MIN_DEPTH_PCT = 8.0
MODE4_PULLBACK_MAX_DEPTH_PCT = 25.0
MODE4_PULLBACK_LOOKBACK_DAYS = 20
# 触底确认标准，同样需要跟daily_analysis.py的
# BOTTOM_CLOSE_POS_MIN/BOTTOM_VOL_UPTICK_MIN保持数值一致
MODE4_BOTTOM_CLOSE_POS_MIN  = 0.60
MODE4_BOTTOM_VOL_UPTICK_MIN = 1.0

# v3新增：真实ATR止损倍数 + 仓位计算参数。
# ⚠️ 以下数值必须手动跟daily_analysis.py里的同名常量保持一致
# （TOTAL_CAPITAL/RISK_PER_TRADE/ATR_STOP_MULTIPLIER/MAX_POSITION_PCT/
# MIN_POSITION_VALUE/CMC_RATE/CMC_MIN_FEE）——两边是独立实现，
# 不共享代码，改一处记得改另一处。这是本次改动里第二个"需要
# 手动同步"的常量组（第一个是MODE4_*），如果以后要消除这类
# 手动同步风险，可以考虑抽一个两边都import的共享配置文件。
TOTAL_CAPITAL        = 50_000
RISK_PER_TRADE       = 0.008     # 0.8% = $400
ATR_STOP_MULTIPLIER  = 1.5
MAX_POSITION_PCT     = 0.20
MIN_POSITION_VALUE   = 6_500
CMC_RATE             = 0.0011
CMC_MIN_FEE          = 7.0
ATR_PERIOD           = 14

_health_fail_streak: dict = {}


# ════════════════════════════════════════════════════════════
# 2. 工具函数
# ════════════════════════════════════════════════════════════

def now_syd() -> datetime:
    return datetime.now(SYD_TZ)


def is_trading_day_and_time() -> bool:
    n = now_syd()
    if n.weekday() >= 5:
        return False
    t = n.strftime("%H:%M")
    return SESSION_START_SAFE <= t <= MARKET_CLOSE


def is_late_session_window() -> bool:
    t = now_syd().strftime("%H:%M")
    return LATE_SESSION_START <= t <= LATE_SESSION_END


def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }, timeout=10)
        r.raise_for_status()
    except requests.HTTPError as e:
        log.error(f"Telegram HTTP错误: {e}")
    except Exception as e:
        log.error(f"Telegram发送失败: {e}")


def safe_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    try:
        s = df[col].squeeze()
        if isinstance(s, pd.Series):
            return s
        return pd.Series([s])
    except Exception:
        return None


# ════════════════════════════════════════════════════════════
# 3. 数据获取
# ════════════════════════════════════════════════════════════

def download_intraday(ticker: str, retries: int = 3) -> Optional[pd.DataFrame]:
    """
    下载当日15分钟K线。

    关键设计（2026-07修复）：
    实测发现yfinance用period="1d"请求极低流动性股票时，
    如果当天K线数量过少（比如全天只有2-3根有成交的15分钟bar），
    yf.download会直接返回空结果并抛出"possibly delisted"这个
    具有误导性的错误——这不代表真的退市，只是yfinance在
    period="1d"这个窄窗口下对稀疏数据的处理逻辑过于严格。

    实测验证（SOR.AX真实案例）：
    period="1d" → 空结果，报错"possibly delisted"
    period="5d" → 正常返回，能看到当天3根K线（含2根有效成交）

    修复方式：统一用period="5d"请求（避免窄窗口误判），
    拿到结果后立刻筛选出"悉尼时区今天"这一天的K线再返回。
    对下游完全透明——monitor_one_ticker()和三个模式判断函数
    拿到的依然是"只包含今天K线"的DataFrame，语义不变，
    不需要改动任何下游逻辑。

    时区处理：必须用悉尼时区的"今天"去过滤，不能用系统本地/UTC的
    今天，否则在悉尼时间0点-10点这段UTC仍是前一天的窗口会误判。
    """
    today_syd = now_syd().date()

    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, period="5d", interval="15m",
                              progress=False, prepost=False)
            if df is None or df.empty:
                log.warning(f"日内K线为空 [{ticker}] 第{attempt}次")
                time.sleep(2)
                continue

            # 转换到悉尼时区（yfinance返回的index可能已带时区，也可能是naive）
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC").tz_convert(SYD_TZ)
            else:
                df.index = df.index.tz_convert(SYD_TZ)

            # 筛选出今天的K线，保持返回语义和之前period="1d"一致
            today_mask = df.index.date == today_syd
            df_today = df[today_mask].copy()

            if df_today.empty:
                log.warning(f"日内K线中无今日数据 [{ticker}] 第{attempt}次"
                           f"（5天数据共{len(df)}行，但今日{today_syd}无匹配行）")
                time.sleep(2)
                continue

            return df_today

        except Exception as e:
            log.error(f"日内K线下载失败 [{ticker}] 第{attempt}次: {e}")
            time.sleep(3)
    return None


def download_daily_reference(ticker: str, retries: int = 3) -> Optional[pd.DataFrame]:
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, period="2mo", interval="1d", progress=False)
            if df is None or df.empty or len(df) < BREAKOUT_LOOKBACK_DAYS + 1:
                log.warning(f"日K线不足 [{ticker}] 第{attempt}次")
                time.sleep(2)
                continue
            return df
        except Exception as e:
            log.error(f"日K线下载失败 [{ticker}] 第{attempt}次: {e}")
            time.sleep(3)
    return None


# ════════════════════════════════════════════════════════════
# 4. 每日基准位锁定
# ════════════════════════════════════════════════════════════

def compute_pullback_reference(high: pd.Series, low: pd.Series,
                                close: pd.Series) -> Optional[dict]:
    """
    模式4专用：独立计算回调参考位（回调最低点/回撤深度）。

    只做数值计算，不重新判断"是否健康/是否触底"——那些判断已经
    由daily_analysis.py在盘前完成（today_status=pullback_bottoming
    这个状态本身就是判断结果）。这里只是为了拿到模式4需要的
    止损参考位和回撤幅度。

    计算方式对齐daily_analysis.py的calc_pullback_health()：
    用最近MODE4_PULLBACK_LOOKBACK_DAYS天的最高价定位"回调起点"，
    回调最低点 = 从那个高点往后（不含今天）的最低价。

    数据源差异（预期内，非bug）：daily_analysis.py用
    intraday_snapshots聚合出的日内快照日高/日低（仅10:00-16:00
    交易时段），这里用yfinance的日线OHLC（覆盖全交易时段，
    含开盘/收盘集合竞价）。两边算出的数字可能有细微差异，
    属于两种数据源的正常誤差，不代表逻辑不一致。

    返回None：数据不足，或当前回撤幅度已经不在8%-25%区间内
    （比如今天数据传入前已经用了收盘价，跟daily_analysis.py
    盘前判断的那一刻有细微出入）——此时模式4没有有效参考位，
    调用方应该跳过模式4的信号判断。
    """
    if high is None or low is None or close is None:
        return None
    if len(close) < MODE4_PULLBACK_LOOKBACK_DAYS:
        return None

    recent_high = float(high.iloc[-MODE4_PULLBACK_LOOKBACK_DAYS:].max())
    if recent_high <= 0:
        return None

    current_close = float(close.iloc[-1])
    depth_pct = round((recent_high - current_close) / recent_high * 100, 2)
    if not (MODE4_PULLBACK_MIN_DEPTH_PCT <= depth_pct <= MODE4_PULLBACK_MAX_DEPTH_PCT):
        return None

    high_idx_pos = high.iloc[-MODE4_PULLBACK_LOOKBACK_DAYS:].values.argmax()
    pullback_start_idx = len(close) - MODE4_PULLBACK_LOOKBACK_DAYS + high_idx_pos
    if pullback_start_idx >= len(close) - 1:
        return None

    recent_low = float(low.iloc[pullback_start_idx:].min())
    return {"recent_high": recent_high, "recent_low": recent_low, "depth_pct": depth_pct}


def calc_atr14(high: pd.Series, low: pd.Series, close: pd.Series,
                period: int = ATR_PERIOD) -> Optional[float]:
    """
    真实ATR计算，公式跟daily_analysis.py的load_atr()一致：
    TR = max(高-低, |高-昨收|, |低-昨收|)，ATR = TR的period日滚动均值。

    v2版本止损用的是"20日高低价差/20"这个粗略代理（注释里自己
    也承认"非真实ATR，仅供参考"）。v3改成用同一份已下载的日线
    数据算真实ATR，不需要额外的yfinance请求。
    """
    if high is None or low is None or close is None or len(close) < period + 1:
        return None
    try:
        prev = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev).abs(),
            (low - prev).abs(),
        ], axis=1).max(axis=1)
        atr = float(tr.rolling(period).mean().iloc[-1])
        return round(atr, 4) if atr > 0 else None
    except Exception:
        return None


def calculate_position_intraday(entry_price: float, stop_distance: float) -> Optional[dict]:
    """
    v3新增：intraday_monitor.py自己的仓位计算，公式跟
    daily_analysis.py的calculate_position()完全一致（Fixed
    Fractional），只是止损距离用的是"这个模式实际触发的止损距离"，
    而不是daily_analysis.py盘前用ATR算出来的那个距离——这两个
    距离在v2版本里可能对不上（同一份0.8%风险预算，被两套不同的
    止损距离各自套用了一遍），v3让触发信号时的仓位跟触发信号时
    的止损保持严格一致。

    stop_distance<=0时返回None（数据异常，不应该据此下单）。
    """
    if stop_distance is None or stop_distance <= 0 or entry_price <= 0:
        return None

    max_loss = TOTAL_CAPITAL * RISK_PER_TRADE
    shares_by_risk = int(max_loss / stop_distance)
    max_by_capital = int(TOTAL_CAPITAL * MAX_POSITION_PCT / entry_price)
    min_by_value   = int(MIN_POSITION_VALUE / entry_price)

    final_shares = min(shares_by_risk, max_by_capital)
    final_shares = max(final_shares, min_by_value)
    if final_shares <= 0:
        return None

    final_value = final_shares * entry_price
    commission  = max(final_value * CMC_RATE, CMC_MIN_FEE) * 2
    actual_risk = final_shares * stop_distance + commission

    return {
        "shares": final_shares,
        "position_value": round(final_value, 2),
        "position_pct": round(final_value / TOTAL_CAPITAL * 100, 1),
        "commission_est": round(commission, 2),
        "actual_risk": round(actual_risk, 2),
        "actual_risk_pct": round(actual_risk / TOTAL_CAPITAL * 100, 2),
    }


def lock_daily_reference(item: dict) -> Optional[dict]:
    """
    锁定当日基准位：突破轨道用的prior_high_20d/prior_low_20d/
    avg_vol_20d，以及v2新增的回调轨道用的pullback_recent_low/
    pullback_depth_pct。两组基准位共用同一次daily_df下载，
    每个交易日只算一次，全天不变（防未来函数），后续15分钟轮询
    直接复用缓存（不再重新下载）。
    """
    ticker = item["ticker"]
    today  = date.today().isoformat()

    if item.get("ref_date") == today and item.get("prior_high_20d"):
        item["_pullback_ref"] = (
            {"recent_low": item["pullback_recent_low"], "depth_pct": item["pullback_depth_pct"]}
            if item.get("pullback_recent_low") is not None else None
        )
        return item

    daily_df = download_daily_reference(ticker)
    if daily_df is None:
        log.error(f"无法锁定基准位 [{ticker}]：日K线获取失败")
        return None

    try:
        close  = safe_series(daily_df, "Close")
        high   = safe_series(daily_df, "High")
        low    = safe_series(daily_df, "Low")
        volume = safe_series(daily_df, "Volume")

        if str(daily_df.index[-1].date()) == today:
            high, low, volume, close = high.iloc[:-1], low.iloc[:-1], volume.iloc[:-1], close.iloc[:-1]

        prior_high = float(high.iloc[-BREAKOUT_LOOKBACK_DAYS:].max())
        prior_low  = float(low.iloc[-BREAKOUT_LOOKBACK_DAYS:].min())
        avg_vol    = float(volume.iloc[-BREAKOUT_LOOKBACK_DAYS:].mean())

        pullback_ref = compute_pullback_reference(high, low, close)
        atr14 = calc_atr14(high, low, close)

        wdb.update_daily_reference(ticker, today, prior_high, prior_low, avg_vol)
        wdb.update_pullback_reference(
            ticker, today,
            pullback_ref["recent_low"] if pullback_ref else None,
            pullback_ref["depth_pct"] if pullback_ref else None,
        )
        wdb.update_atr_reference(ticker, today, atr14)
        item.update({
            "ref_date": today, "prior_high_20d": prior_high,
            "prior_low_20d": prior_low, "avg_vol_20d": avg_vol,
            "pullback_recent_low": pullback_ref["recent_low"] if pullback_ref else None,
            "pullback_depth_pct": pullback_ref["depth_pct"] if pullback_ref else None,
            "_pullback_ref": pullback_ref,
            "atr14": atr14,
        })
        log.info(
            f"基准锁定 [{ticker}]：前高{prior_high:.3f} 前低{prior_low:.3f} 均量{avg_vol:,.0f}"
            f" | ATR14={atr14 if atr14 else 'N/A'}"
            + (f" | 回调参考：低点{pullback_ref['recent_low']:.3f}"
               f"（回撤{pullback_ref['depth_pct']}%）"
               if pullback_ref else " | 回调参考：不适用")
        )
        return item
    except Exception as e:
        log.error(f"基准位计算失败 [{ticker}]: {e}")
        return None


# ════════════════════════════════════════════════════════════
# 5. 健康度检查
# ════════════════════════════════════════════════════════════

def check_health(ticker: str, daily_df: pd.DataFrame) -> tuple:
    try:
        close = safe_series(daily_df, "Close")
        if close is None or len(close) < 50:
            return True, ""

        ma50 = close.rolling(50).mean()
        below_streak = 0
        for i in range(1, HEALTH_BELOW_MA50_GRACE_DAYS + 1):
            if float(close.iloc[-i]) < float(ma50.iloc[-i]):
                below_streak += 1
        if below_streak >= HEALTH_BELOW_MA50_GRACE_DAYS:
            return False, f"连续{HEALTH_BELOW_MA50_GRACE_DAYS}日跌破MA50"

        return True, ""
    except Exception as e:
        log.warning(f"健康度检查异常 [{ticker}]: {e}")
        return True, ""


# ════════════════════════════════════════════════════════════
# 6. 三种模式判断
# ════════════════════════════════════════════════════════════

def _build_bar_record(ts: pd.Timestamp, row: pd.Series, prior_high: float,
                       avg_vol_20d: float) -> dict:
    def _f(val) -> float:
        if hasattr(val, "iloc"):
            return float(val.iloc[0])
        return float(val)

    o, h, l, c, v = _f(row["Open"]), _f(row["High"]), _f(row["Low"]), _f(row["Close"]), _f(row["Volume"])
    # 注：typical_price是(H+L+C)/3，用于计算VWAP的单根K线代表价格，
    # 不是VWAP本身。之前版本这里的字段名误写成"vwap"，实际存的是
    # typical_price——这两个是不同的量，VWAP必须是多根K线的成交量加权
    # 累积值，单根K线内部算不出VWAP。这里改名为typical_price，
    # 真正的累积VWAP由calc_cumulative_vwap()单独计算。
    typical_price = (h + l + c) / 3.0
    pct_from_high = round((c / prior_high - 1) * 100, 2) if prior_high else 0.0
    return {
        "time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v,
        "typical_price": typical_price, "pct_from_prior_high": pct_from_high,
    }


def calc_cumulative_vwap(bars: list) -> Optional[float]:
    """
    计算截至当前bar为止，当日累积VWAP（成交量加权均价）。

    公式：Σ(typical_price × volume) / Σ(volume)
    这是VWAP的标准定义，不是单根K线的均价。

    用途：作为限价单入场参考——如果触发信号时的价格明显高于
    当日VWAP，说明相对今天已发生的成交而言在追高，入场风险更高；
    如果接近或低于VWAP，说明入场位置相对当日成交结构是合理的。

    返回None的情况：全部成交量为0（比如SOR.AX那种全天只有
    零星成交甚至零成交的极端流动性股票），此时VWAP无意义。
    """
    total_dollar = sum(b["typical_price"] * b["volume"] for b in bars)
    total_volume = sum(b["volume"] for b in bars)
    if total_volume <= 0:
        return None
    return round(total_dollar / total_volume, 4)


def detect_mode1_breakout(bars: list, prior_high: float, avg_vol_20d: float,
                           min_dollar_vol: float) -> Optional[dict]:
    if len(bars) < 2 or not prior_high:
        return None

    cur, prev = bars[-1], bars[-2]
    if prev["close"] >= prior_high:
        return None
    if cur["close"] < prior_high:
        return None

    same_day_bars = bars[:-1]
    if same_day_bars:
        intraday_avg_vol = sum(b["volume"] for b in same_day_bars) / len(same_day_bars)
    else:
        intraday_avg_vol = avg_vol_20d / 26

    dollar_vol = cur["close"] * cur["volume"]
    if dollar_vol < min_dollar_vol:
        return None

    vol_ratio = cur["volume"] / intraday_avg_vol if intraday_avg_vol > 0 else 0
    if vol_ratio < VOL_SPIKE_RATIO_M1:
        return None

    return {
        "mode": "模式1-突破瞬间买", "state": "breaking",
        "price": cur["close"], "vol_ratio": round(vol_ratio, 2),
        "breakout_level": prior_high,
        "time": cur["time"],
    }


def detect_breakout_confirmation(bars: list, prior_high: float) -> Optional[str]:
    if len(bars) < 2:
        return None
    cur = bars[-1]
    pct_vs_level = (cur["close"] / prior_high - 1) * 100
    if pct_vs_level < BREAKOUT_FAILURE_PCT:
        return "failed"
    if cur["close"] >= prior_high:
        return "confirmed"
    return None


def detect_mode2_pullback_crossday(item: dict, bars: list,
                                    prior_high: float) -> Optional[dict]:
    """
    模式2-回踩确认买（v2：跨天版）。

    v1的问题：download_intraday()每次只返回"今天"的K线，v1在这份
    today-only的bars里搜索"是否发生过突破"，导致只能捕捉"同一天内
    突破又回踩"这种同日快速往返，捕捉不到更常见、更健康的
    "前几天突破，今天缩量回踩"这种跨天走势。

    v2改法：不再从bars里搜索突破点，改成读取
    item['last_breakout_date']/['last_breakout_price']——由
    record_signal()在模式1触发时专门写入watchlist_db.py的独立字段
    （last_breakout_date/last_breakout_price，不会被模式2/3/4自己
    的信号覆盖，见watchlist_db.py注释）。

    回踩深度用"今天"重新锁定的prior_high计算，而不是历史突破那一刻
    的价格——如果突破后继续创新高，prior_high会跟着抬高，用当天
    最新值才是真正在测试的支撑位。

    量能判断退化为日线代理：因为拿不到历史突破那天的15分钟K线
    量能数据（只有当天bars），用avg_vol_20d/26这个"单bar历史基准量"
    做代理，跟模式3已经在用的代理方式保持一致。

    "确认反转"这一步的判断标准维持v1原样：当前bar收盘 > 上一bar
    收盘——按用户明确要求，这次不跟模式4的"收盘位置+放量"标准
    统一，避免影响已经在跑的实盘信号。
    """
    if len(bars) < 2 or not prior_high:
        return None

    last_breakout_date  = item.get("last_breakout_date")
    last_breakout_price = item.get("last_breakout_price")
    if not last_breakout_date or not last_breakout_price:
        return None

    try:
        days_since_breakout = (date.today() - date.fromisoformat(last_breakout_date)).days
    except (TypeError, ValueError):
        return None

    if days_since_breakout <= 0 or days_since_breakout > MODE2_BREAKOUT_LOOKBACK_DAYS:
        # <=0：今天刚突破，属于模式1的地盘，不重复用模式2判断
        # >上限：突破太久以前了，prior_high大概率已经滚动很多，
        #        用这个过旧的"突破事件"触发模式2意义不大
        return None

    cur, prev = bars[-1], bars[-2]
    pullback_depth_pct = (prior_high - prev["low"]) / prior_high * 100

    if prev["close"] >= prior_high:
        return None
    if pullback_depth_pct > MODE2_PULLBACK_MAX_DEPTH_PCT:
        return None
    if cur["close"] < prior_high * (1 - MODE2_PULLBACK_MAX_DEPTH_PCT / 100):
        return None

    avg_vol_20d = item.get("avg_vol_20d") or 0
    bar_avg_vol_baseline = avg_vol_20d / 26 if avg_vol_20d else 0
    if (bar_avg_vol_baseline > 0
            and cur["volume"] > bar_avg_vol_baseline * MODE2_PULLBACK_VOL_SHRINK_RATIO * 1.5):
        return None

    if cur["close"] <= prev["close"]:
        return None

    return {
        "mode": "模式2-回踩确认买", "state": "confirmed",
        "price": cur["close"], "pullback_depth_pct": round(pullback_depth_pct, 2),
        "breakout_level": prior_high, "days_since_breakout": days_since_breakout,
        "time": cur["time"],
    }


def detect_mode4_pullback_confirm(bars: list,
                                   pullback_ref: Optional[dict]) -> Optional[dict]:
    """
    模式4-回调确认买（新）。

    前提：daily_analysis.py已经在盘前把该股票判定为pullback_bottoming
    （截至昨日收盘，已经出现"健康回调+触底反弹"信号）。这里不重新
    判断"是否处于健康回调"或"是否触底"这些多日形态判断——那是
    daily_analysis.py的工作。这里只做两件事：

    1. 硬性止损位检查：今天有没有跌破回调最低点
       （pullback_ref['recent_low']）——这个价位本身就是这笔交易的
       止损参考位，一旦盘中跌破，说明daily_analysis.py昨天的判断
       已经被证伪，不应该再生成买入信号追这只股票。
    2. 当日confirm：今天是否延续强势（收盘位置强 + 温和放量），
       延续daily_analysis.py的is_bottoming判断标准（收盘位置≥60%，
       量比≥1.0），只是从"日线"换算成"15分钟bar"的当日代理版本。
    """
    if len(bars) < 2 or pullback_ref is None:
        return None

    cur, prev = bars[-1], bars[-2]

    if cur["close"] <= pullback_ref["recent_low"]:
        return None

    day_range = cur["high"] - cur["low"]
    close_pos = (cur["close"] - cur["low"]) / day_range if day_range > 0 else 0.5
    if close_pos < MODE4_BOTTOM_CLOSE_POS_MIN:
        return None

    vol_uptick = (cur["volume"] / prev["volume"]) if prev["volume"] > 0 else 1.0
    if vol_uptick < MODE4_BOTTOM_VOL_UPTICK_MIN:
        return None

    return {
        "mode": "模式4-回调确认买", "state": "confirmed",
        "price": cur["close"], "pullback_depth_pct": pullback_ref["depth_pct"],
        "pullback_recent_low": pullback_ref["recent_low"],
        "close_pos": round(close_pos, 2), "vol_uptick": round(vol_uptick, 2),
        "time": cur["time"],
    }


def detect_mode3_late_session(bars: list, day_high: float,
                               avg_vol_20d: float) -> Optional[dict]:
    if len(bars) < 2 or not day_high:
        return None

    cur, prev = bars[-1], bars[-2]
    dist_from_high_pct = (day_high - cur["close"]) / day_high * 100
    if dist_from_high_pct > LATE_SESSION_NEAR_HIGH_PCT:
        return None

    bar_avg_vol = avg_vol_20d / 26 if avg_vol_20d else 0
    is_red_bar = cur["close"] < cur["open"]
    if is_red_bar and bar_avg_vol > 0 and cur["volume"] > bar_avg_vol * 1.5:
        return None

    if bar_avg_vol > 0 and cur["volume"] < bar_avg_vol * LATE_SESSION_MIN_VOL_RATIO:
        return None

    return {
        "mode": "模式3-尾盘确认买", "state": "confirmed",
        "price": cur["close"], "dist_from_high_pct": round(dist_from_high_pct, 2),
        "day_high": day_high,
        "time": cur["time"],
    }


# ════════════════════════════════════════════════════════════
# 7. 单只股票的监测主流程
# ════════════════════════════════════════════════════════════

def monitor_one_ticker(item: dict) -> None:
    ticker = item["ticker"]
    today  = date.today().isoformat()

    item = lock_daily_reference(item)
    if item is None:
        return

    prior_high  = item["prior_high_20d"]
    avg_vol_20d = item["avg_vol_20d"]

    intraday_df = download_intraday(ticker)
    if intraday_df is None or intraday_df.empty:
        log.warning(f"跳过 [{ticker}]：无日内K线")
        return

    try:
        bars = []
        for ts, row in intraday_df.iterrows():
            bars.append(_build_bar_record(ts, row, prior_high, avg_vol_20d))
    except Exception as e:
        log.error(f"K线解析失败 [{ticker}]: {e}")
        return

    if not bars:
        return

    cur_bar  = bars[-1]
    day_high = max(b["high"] for b in bars)

    breakout_state_for_log = "above" if cur_bar["close"] >= prior_high else "below"
    intraday_avg_so_far = (sum(b["volume"] for b in bars[:-1]) / len(bars[:-1])) if len(bars) > 1 else cur_bar["volume"]
    vol_ratio_log = cur_bar["volume"] / intraday_avg_so_far if intraday_avg_so_far > 0 else 1.0

    # 计算真正的累积VWAP（之前这里误用了典型价格，见calc_cumulative_vwap注释）
    cumulative_vwap = calc_cumulative_vwap(bars)
    # calc_cumulative_vwap在全天零成交时返回None，数据库vwap字段
    # 用典型价格兜底（总比存NULL让下游代码要处理None更简单，
    # 且这种极端情况下典型价格和真实价格的偏差本身也没有太大意义）
    vwap_for_db = cumulative_vwap if cumulative_vwap is not None else cur_bar["typical_price"]

    wdb.save_snapshot(
        ticker=ticker,
        snapshot_time=cur_bar["time"].isoformat(),
        trading_date=today,
        price=cur_bar["close"], high=cur_bar["high"], low=cur_bar["low"],
        volume=cur_bar["volume"], vwap=vwap_for_db,
        pct_from_prior_high=cur_bar["pct_from_prior_high"],
        vol_vs_avg_ratio=round(vol_ratio_log, 2),
        breakout_state=breakout_state_for_log,
    )

    dollar_vol = cur_bar["close"] * cur_bar["volume"]
    if dollar_vol < MIN_DOLLAR_VOLUME_INTRADAY:
        log.debug(f"[{ticker}] 本bar成交额{dollar_vol:,.0f}不达标，跳过信号判断")
        return

    # ── 跨日状态前置过滤（v2：按状态分两条轨道路由）──────────
    # 突破轨道（模式1/2/3）配合daily_analysis.py的"ready"状态；
    # 回调轨道（模式4）配合"pullback_bottoming"状态。这两个状态
    # 在daily_analysis.py里是elif互斥关系，同一天同一只股票只会
    # 是其中一个，两条轨道不会同时对同一只股票触发。
    #
    # v1的bug：状态门禁只认ready/watch/caution/accumulating，
    # pullback_healthy/pullback_bottoming不在任何一个分支里，
    # 会掉进"unknown/stale"分支，被错误地当成"分析层不可用"处理，
    # Telegram文案也会显示"跨日因子分析不可用（pullback_bottoming）"
    # 这种自相矛盾的措辞。v2显式处理这两个新状态。
    #
    # 设计仍然是"软性过滤"而非硬性阻断（对unknown/stale）：
    # 如果today_status是unknown（daily_analysis.py从未跑过这只股票）
    # 或stale（当天因故障未运行），不会直接跳过信号判断，而是
    # 降级只跑突破轨道（模式1/2/3）——这是为了避免"分析层单点故障
    # 导致监测层完全失效"。降级时不跑模式4，因为模式4依赖当天
    # 确认的pullback_bottoming状态才有意义，分析层不可用时没有
    # 这个前提。
    #
    # 若要改成硬性阻断（unknown/stale时也跳过），
    # 将 STRICT_STATUS_GATE 改为 True。
    STRICT_STATUS_GATE = False

    status_info = wdb.get_today_status(ticker)
    status = status_info["status"]

    run_breakout_modes = False   # 模式1/2/3
    run_pullback_mode  = False   # 模式4

    if status == "ready":
        run_breakout_modes = True
    elif status == "pullback_bottoming":
        run_pullback_mode = True
    elif status in ("watch", "caution", "accumulating", "pullback_healthy"):
        log.debug(f"[{ticker}] 跨日状态={status}（非ready/pullback_bottoming），"
                 f"跳过信号判断，继续积累数据")
        return
    else:
        # unknown / stale
        msg = f"[{ticker}] 跨日状态不可用（{status}，原始值:{status_info.get('raw_status')}）"
        if STRICT_STATUS_GATE:
            log.warning(f"{msg}，严格模式下跳过信号判断")
            return
        else:
            log.warning(f"{msg}，降级为不做跨日过滤，仍运行突破轨道判断")
            run_breakout_modes = True

    already_signaled_today = (item.get("last_signal_date") == today)

    signal = None

    if run_breakout_modes:
        m1 = detect_mode1_breakout(bars, prior_high, avg_vol_20d, MIN_DOLLAR_VOLUME_INTRADAY)
        if m1 and not already_signaled_today:
            signal = m1
            signal["execution_window"] = "当前/今日内尽快（突破刚发生，确认窗口很短）"

        if signal is None:
            m2 = detect_mode2_pullback_crossday(item, bars, prior_high)
            if m2 and item.get("last_signal_mode") != "模式2-回踩确认买":
                signal = m2
                signal["execution_window"] = "当前/今日内（回踩企稳确认）"

        if signal is None and is_late_session_window():
            if item.get("last_signal_mode") != "模式3-尾盘确认买" or item.get("last_signal_date") != today:
                m3 = detect_mode3_late_session(bars, day_high, avg_vol_20d)
                if m3:
                    signal = m3
                    signal["execution_window"] = "今日收盘前后买入，次日开盘附近了结（T+1，非多日持仓）"

    elif run_pullback_mode:
        m4 = detect_mode4_pullback_confirm(bars, item.get("_pullback_ref"))
        if m4 and not (item.get("last_signal_mode") == "模式4-回调确认买"
                       and item.get("last_signal_date") == today):
            signal = m4
            signal["execution_window"] = "今日/次日择机分批入场（回调确认，非追高，可分批建仓）"

    if signal is None:
        return

    price = signal["price"]
    is_t1_trade = (signal["mode"] == "模式3-尾盘确认买")
    atr14 = item.get("atr14")
    used_atr_stop = False

    if signal["mode"] == "模式1-突破瞬间买":
        if atr14:
            stop_loss = round(prior_high - ATR_STOP_MULTIPLIER * atr14, 3)
            stop_logic = f"跌破突破位ATR止损 ${stop_loss:.3f}（突破位${prior_high:.3f} − {ATR_STOP_MULTIPLIER}×ATR14）立即离场"
            used_atr_stop = True
        else:
            stop_loss = round(prior_high * 0.995, 3)
            stop_logic = f"跌破突破位 ${prior_high:.3f} 立即离场（ATR数据不足，退回固定比例止损）"
    elif signal["mode"] == "模式2-回踩确认买":
        recent_low = min(b["low"] for b in bars[-6:])
        if atr14:
            stop_loss = round(recent_low - ATR_STOP_MULTIPLIER * atr14, 3)
            stop_logic = f"跌破回踩低点ATR止损 ${stop_loss:.3f}（回踩低点${recent_low:.3f} − {ATR_STOP_MULTIPLIER}×ATR14）立即离场"
            used_atr_stop = True
        else:
            stop_loss = round(recent_low * 0.995, 3)
            stop_logic = f"跌破回踩低点 ${recent_low:.3f} 立即离场（ATR数据不足，退回固定比例止损）"
    elif signal["mode"] == "模式4-回调确认买":
        # 维持不变：止损就是回调最低点本身，daily_analysis.py原本
        # 设计就没加缓冲（跌破即视为判断证伪），不在这里另加ATR缓冲
        stop_loss = round(signal["pullback_recent_low"], 3)
        stop_logic = f"跌破回调最低点 ${stop_loss:.3f}（判断证伪）立即离场"
    else:
        # 模式3-尾盘确认买：T+1单日持仓，不是swing多日持仓，
        # 止损逻辑改成"次日不及预期就直接离场"，不再用突破位
        # 立即离场这套（本来就不追求突破位站稳，追求的是隔夜动能延续）
        stop_loss = round(price * 0.99, 3)
        stop_logic = "次日开盘若明显低开/走弱，不追、直接在开盘附近离场"

    # 风险收益比目标价：用prior_high与prior_low_20d的差值 / 20
    # 作为"日均波幅"的粗略代理，不是真实ATR14。
    #
    # 为什么不现场下载日线数据算真实ATR：
    # 信号触发是时间敏感事件，现场再发一次yfinance请求算ATR会拖慢
    # 响应速度（之前daily_analysis.py下载ATR平均耗时1-2秒，
    # 38只股票轮询本来就要跑约1分钟，不应该为了这个精度提升
    # 再增加延迟）。lock_daily_reference()已经算出的
    # prior_high_20d/prior_low_20d本身就来自20日窗口，
    # 用二者差值/20作为波幅代理，量级上是合理的近似，
    # 但明确不等于ATR14（ATR是基于真实波幅TR的滚动均值，
    # 这里只是用价格区间宽度做了简化）。
    #
    # 模式3是T+1单日持仓，2-3倍风险的swing目标价框架跟"次日开盘
    # 附近了结"这个持仓周期对不上——之前v1版本两者混用是个措辞
    # 层面的问题，v2改成不显示R倍数目标价，直接给T+1语境下的说法。
    stop_distance = abs(price - stop_loss)
    if is_t1_trade:
        target_line = (
            "🎯 无固定目标价——T+1单日持仓，计划在次日开盘附近了结，"
            "不是多日swing"
        )
    else:
        target_1r = round(price + stop_distance * 2, 3)
        target_2r = round(price + stop_distance * 3, 3)
        if used_atr_stop:
            target_note = "（止损距离×2/×3，止损距离基于真实ATR14）"
        elif signal["mode"] == "模式4-回调确认买":
            target_note = "（止损距离×2/×3，止损为回调实际低点，非ATR）"
        else:
            target_note = "（止损距离×2/×3，ATR数据不足退回固定比例，仅供参考）"
        target_line = f"🎯 目标价：${target_1r}（2倍风险）/ ${target_2r}（3倍风险）{target_note}"

    # v3新增：用这个模式实际的止损距离重新算仓位，跟daily_analysis.py
    # 盘前用ATR算出来的position_advice保持"止损距离和仓位大小对得上"
    # 这个一致性（同一份0.8%风险预算，不能被两个不同的止损距离
    # 各自套用一遍）。T+1（模式3）不显示仓位建议——它止损逻辑本身
    # 是"次日不及预期就离场"而非固定价位止损，用stop_distance硬算
    # 仓位意义不大。
    position_advice = None if is_t1_trade else calculate_position_intraday(price, stop_distance)
    if position_advice:
        position_line = (
            f"💼 仓位建议：{position_advice['shares']}股 | "
            f"${position_advice['position_value']}（总资金{position_advice['position_pct']}%）| "
            f"预估手续费${position_advice['commission_est']} | "
            f"总风险${position_advice['actual_risk']}（{position_advice['actual_risk_pct']}%）"
        )
    else:
        position_line = None

    # 当日累积VWAP，作为限价单入场参考
    cum_vwap = calc_cumulative_vwap(bars)
    if cum_vwap is not None:
        vwap_deviation_pct = round((price / cum_vwap - 1) * 100, 2)
        if vwap_deviation_pct > 2.0:
            vwap_note = f"⚠️ 当前价高于VWAP {vwap_deviation_pct}%，追高风险较高"
        elif vwap_deviation_pct < -1.0:
            vwap_note = f"当前价低于VWAP {abs(vwap_deviation_pct)}%，入场位置相对合理"
        else:
            vwap_note = "当前价接近VWAP，入场位置合理"
    else:
        vwap_deviation_pct = None
        vwap_note = "今日成交量过低，VWAP参考意义有限"

    wdb.record_signal(ticker, signal["mode"], price, stop_loss)

    extra_lines = []
    if "vol_ratio" in signal:
        extra_lines.append(f"量比：{signal['vol_ratio']}x（vs当日均量）")
    if "pullback_depth_pct" in signal:
        depth_label = "回调深度" if signal["mode"] == "模式4-回调确认买" else "回踩深度"
        extra_lines.append(f"{depth_label}：{signal['pullback_depth_pct']}%")
    if "dist_from_high_pct" in signal:
        extra_lines.append(f"距今日高点：{signal['dist_from_high_pct']}%")
    if "close_pos" in signal:
        extra_lines.append(f"收盘位置：{signal['close_pos']*100:.0f}%（当根K线高低区间内）")
    if "vol_uptick" in signal:
        extra_lines.append(f"量比：{signal['vol_uptick']}x（vs上一根K线）")
    if "days_since_breakout" in signal:
        extra_lines.append(f"距突破：{signal['days_since_breakout']}个交易日")
    extra_text = "\n".join(extra_lines)

    if item.get("source") == "manual":
        source_line = (
            f"🔎 来源：手动添加监测 "
            f"(已监测{item.get('days_elapsed', 0)}/{item.get('total_days', 0)}天"
            f"{'，第' + str(item['reselect_count']) + '次续期' if item.get('reselect_count') else ''})"
        )
    else:
        source_line = (
            f"🔎 来源：{item.get('tier_label') or 'EOD'} 筛选 "
            f"(综合评分:{item.get('composite_score') if item.get('composite_score') is not None else 'N/A'}，"
            f"已监测{item.get('days_elapsed', 0)}/{item.get('total_days', 0)}天)"
        )

    # 跨日状态说明：让用户能区分信号可信度——
    # "ready"/"pullback_bottoming"都是经过daily_analysis.py盘前跨日
    # 因子确认的高质量信号（只是分属突破/回调两条轨道），
    # "unknown/stale"是跨日分析层不可用时降级运行、未经跨日过滤的
    # 信号，可信度不同，必须让用户知道差异，不能用同样的措辞混在一起。
    if status_info["status"] == "ready":
        status_line = "✅ 中线判断：已通过跨日因子分析确认（今日ready，突破轨道，值得建仓）"
    elif status_info["status"] == "pullback_bottoming":
        status_line = "✅ 中线判断：健康回调+触底反弹已通过跨日因子分析确认（今日pullback_bottoming，回调轨道）"
    else:
        status_line = (
            f"⚠️ 中线判断：跨日因子分析不可用（{status_info['status']}），"
            f"本信号未经跨日质量过滤，可信度低于常规信号"
        )

    vwap_line = (
        f"📊 当日VWAP：${cum_vwap:.3f}（{vwap_note}）"
        if cum_vwap is not None else f"📊 {vwap_note}"
    )

    header = "🚨 <b>T+1入场信号触发</b>" if is_t1_trade else "🚨 <b>入场信号触发</b>"

    position_block = f"{position_line}\n" if position_line else ""

    msg = (
        f"{header}\n\n"
        f"<b>{item.get('company_name', ticker)}</b> ({ticker})\n"
        f"{status_line}\n"
        f"📍 短线择时：<b>{signal['mode']}</b>\n"
        f"⏱ 触发时间：{signal['time'].strftime('%H:%M')}\n"
        f"💰 触发价：${price:.3f}\n"
        f"{vwap_line}\n"
        f"{extra_text}\n\n"
        f"{target_line}\n"
        f"🛑 止损逻辑：{stop_logic}（止损价参考 ${stop_loss:.3f}）\n"
        f"{position_block}\n"
        f"📌 建议执行窗口：{signal['execution_window']}\n\n"
        f"{source_line}\n"
        f"⚠️ 15分钟K线级别确认，非逐笔实时信号，请结合实时盘口核实再操作"
    )
    send_telegram(msg)
    log.info(f"信号推送 [{ticker}] {signal['mode']} @ ${price:.3f} "
             f"（跨日状态:{status_info['status']}）")


# ════════════════════════════════════════════════════════════
# 8. 收盘后日终处理
# ════════════════════════════════════════════════════════════

def run_end_of_day_maintenance() -> None:
    log.info("=== 收盘后队列维护开始 ===")
    watchlist = wdb.get_active_watchlist()
    for item in watchlist:
        ticker = item["ticker"]
        daily_df = download_daily_reference(ticker)
        if daily_df is None:
            log.warning(f"队列维护跳过 [{ticker}]：日K线获取失败")
            continue

        healthy, reason = check_health(ticker, daily_df)
        wdb.increment_day_elapsed(ticker)

        if not healthy:
            wdb.exit_watchlist(ticker, f"健康度不达标：{reason}")
            send_telegram(
                f"📉 <b>移出监测队列</b>\n{item.get('company_name', ticker)} ({ticker})\n"
                f"原因：{reason}\n已监测{item['days_elapsed'] + 1}天"
            )
            continue

        new_elapsed = item["days_elapsed"] + 1
        if new_elapsed >= item["total_days"]:
            wdb.exit_watchlist(ticker, "监测天数耗尽")
            log.info(f"队列维护 [{ticker}]：监测天数耗尽，正常清出")

    log.info("=== 收盘后队列维护完成 ===")


# ════════════════════════════════════════════════════════════
# 9. 主入口
# ════════════════════════════════════════════════════════════

def main() -> None:
    wdb.init_watchlist_db()

    n = now_syd()
    log.info(f"intraday_monitor 触发 [{n.strftime('%Y-%m-%d %H:%M:%S %Z')}]")

    if not is_trading_day_and_time():
        log.info("非交易时段，跳过本次轮询")
        return

    watchlist = wdb.get_active_watchlist()
    if not watchlist:
        log.info("监测队列为空，跳过")
        return

    log.info(f"本次监测 {len(watchlist)} 只股票")
    for item in watchlist:
        try:
            monitor_one_ticker(item)
        except Exception as e:
            log.error(f"监测异常 [{item['ticker']}]: {e}")
        time.sleep(1.0)

    t = n.strftime("%H:%M")
    if "15:45" <= t <= "16:00":
        run_end_of_day_maintenance()

    log.info("intraday_monitor 本次轮询完成")


if __name__ == "__main__":
    main()
