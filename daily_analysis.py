# ============================================================
# daily_analysis.py  v3.2
#
# v3.2修复（相对v3.1）：
#   - v3.1修了"整体price_slope掩盖回调判断"的问题，但遗留了
#     同一类问题的两个残留场景：
#     ① 回撤深度>25%（PULLBACK_MAX_DEPTH_PCT，可能已是真实反转
#        而非健康回调）时，calc_pullback_health()返回is_pullback
#        =False，若整体窗口price_slope仍为正，会被误判为
#        "价格重心上移✅"，掩盖深跌风险。
#     ② 回撤幅度在8%-25%区间但跌破MA50时，同样is_pullback=False，
#        同样可能被误判为"价格重心上移✅"，掩盖破位风险。
#   - 修复：calc_pullback_health()返回值新增broke_ma50字段标记
#     "跌破MA50提前返回"这一具体原因；evaluate_ticker()在
#     "回调但非缩量"判断之后、"价格重心上移"判断之前，新增两个
#     显式分支分别捕获"深跌"和"跌破MA50"两种情况，计入warnings
#     （不强制改变ready/watch/caution状态阈值，只是不再被
#     笼统地标记为"价格重心上移✅"）。
#   - 已用合成数据做四组回归验证：深跌35%场景、回撤14%但破MA50
#     场景均正确产生警告；v3.1修复的触底反弹场景、以及正常
#     无回调上涨场景，行为均保持不变。
#
# v3.1修复（相对v3）：
#   - evaluate_ticker()中`if price_f["price_slope"] > 0`被用作
#     是否检查回调的前置门槛，但price_slope是对整个70天窗口做
#     线性回归，一次"暴涨后小幅回调"（例如前50天暴涨、最近
#     10-20天回调）的整体斜率几乎总是仍为正，导致回调判断分支
#     永远不会被触发，pullback_healthy/pullback_bottoming状态
#     形同虚设。
#   - 修复：不再用price_slope的正负作为回调检查的前置门槛，
#     而是始终调用calc_pullback_health()（内部用最近20天高点
#     独立判断回撤幅度，不依赖整体窗口斜率），回调判断优先于
#     笼统的价格重心判断。已用合成数据验证：price_slope修复前后
#     均为正值，修复前判定为"价格重心上移✅"（信号被掩盖），
#     修复后正确判定为"健康回调中"/"触底反弹信号"。
#
# v3新增（相对v2）：
#   - calc_pullback_health()：判断确认趋势中的回调是否健康
#     不是反转/均值回归策略，是趋势跟随体系内部的回调入场
#     （Pullback Entry Within Uptrend）。前提仍然是这只股票
#     处于确认过的上升趋势（能进入watchlist本身就证明过
#     trend_strength_score达标），只是在回调低点入场，
#     而不是追高点突破。
#   - evaluate_ticker()：新增pullback_healthy/pullback_bottoming
#     两个状态，介于原有ready/watch/caution/accumulating之间
#   - format_premarket_report()/format_postmarket_report()：
#     展示新增的两个状态
#
# 参数（用户确认，基于ASX中小盘股波动率）：
#   回调幅度区间：8%-25%（相对最近20日高点）
#   关键均线：MA50（比MA20更宽松，给更大回调空间）
#   止损参考：这次回调的实际最低点（不是ATR倍数）
#
# v2功能保留：
#   - 整合了 backfill_snapshots.py 的历史数据补齐功能
#   - 新股票（数据 < BACKFILL_THRESHOLD 天）自动补齐60天历史数据
#   - 已有足够数据的股票直接进入因子分析
#
# 运行方式（crontab）：
#   盘前：UTC 23:00（前日）= 悉尼09:00
#         python3 daily_analysis.py --mode premarket
#   盘后：UTC 06:35        = 悉尼16:35
#         python3 daily_analysis.py --mode postmarket
#
# 数据来源：
#   - watchlist.db（intraday_snapshots，自积累 + 历史补齐）
#   - yfinance（15分钟历史K线 + 日线ATR + 当前价格）
#
# 风控参数基线：
#   $50,000 资金 | CMC 0.11%/最低$7 | 单笔风险0.8% | ATR×1.5止损
# ============================================================

import os
import sys
import sqlite3
import logging
import argparse
import time
from datetime import date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd
import numpy as np
import requests as req

import watchlist_db as wdb

# ════════════════════════════════════════════════════════════
# 0. 日志
# ════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ubuntu/logs/daily_analysis.log",
                            encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SYD_TZ = ZoneInfo("Australia/Sydney")

# ════════════════════════════════════════════════════════════
# 1. 常量
# ════════════════════════════════════════════════════════════

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")

# 资金与风控
TOTAL_CAPITAL       = 50_000
RISK_PER_TRADE      = 0.008     # 0.8% = $400
ATR_STOP_MULTIPLIER = 1.5
MIN_POSITION_VALUE  = 6_500
MAX_POSITION_PCT    = 0.20
CMC_RATE            = 0.0011
CMC_MIN_FEE         = 7.0

# 数据补齐
BACKFILL_THRESHOLD   = 45       # 不足此天数时触发历史补齐
BACKFILL_PERIOD      = "60d"    # yfinance 15分钟最多回溯60天
PRIOR_HIGH_WINDOW    = 20       # prior_high_20d 回溯窗口
VOL_AVG_WINDOW       = 20       # 时段均量回溯窗口
SESSION_START        = "10:00"  # 交易时段起点（跳过开盘集合竞价）
SESSION_END          = "16:00"  # 交易时段终点
MIN_DOLLAR_VOL          = 300_000  # 盘中监测实时信号用（严格）
BACKFILL_MIN_DOLLAR_VOL =  50_000  # 历史补齐用（宽松）
# 两个门槛分开的理由：
# 盘中监测需要确保流动性足够支撑实际下单，$300K是合理下限
# 历史补齐只是为了因子分析积累价格/量能数据，不涉及实际交易，
# $50K避免小盘股历史数据被大量过滤导致因子分析数据基础不足

# 因子分析
MIN_DAYS_FOR_ANALYSIS   = 5
MIN_DAYS_FOR_EXHAUSTION = 10
VOL_SPIKE_THRESHOLD     = 1.8
VOL_SHRINK_SLOPE_MAX    = -0.02
AMPLITUDE_SHRINK_SLOPE  = -0.001
CLOSE_POS_MIN           = 0.65

# v3新增：回调健康度判断（趋势跟随体系内部的回调入场，非反转策略）
# 设计前提：这只股票必须已经通过screener.py的trend_strength_score
# 筛选进入watchlist（本身就证明过趋势强度），这里只是在它出现
# 回调时进一步判断"这次回调是健康的（缩量、未破MA50）还是危险的
# （放量下跌）"，不是凭空判断任意股票是否触底反弹。
PULLBACK_MIN_DEPTH_PCT    = 8.0   # 回调幅度下限（相对20日高点）
PULLBACK_MAX_DEPTH_PCT    = 25.0  # 回调幅度上限，超过视为可能真反转，不再按回调处理
PULLBACK_VOL_SHRINK_RATIO = 0.8   # 回调期间日均量/回调前10日均量，低于此值算缩量（健康）
PULLBACK_VOL_DANGER_RATIO = 1.3   # 高于此值判定为放量下跌（危险）
PULLBACK_LOOKBACK_DAYS    = 20    # 回溯高点窗口，与prior_high_20d概念一致
BOTTOM_CLOSE_POS_MIN      = 0.60  # 触底信号：当日收盘位置需在日内区间60%以上
BOTTOM_VOL_UPTICK_MIN     = 1.0   # 触底信号：当日成交量需>=前一日（温和放量）

# ════════════════════════════════════════════════════════════
# 2. Telegram
# ════════════════════════════════════════════════════════════

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            r = req.post(url, json={
                "chat_id": CHAT_ID, "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=10)
            r.raise_for_status()
        except Exception as e:
            log.error(f"Telegram推送失败: {e}")
        time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 3. 历史数据补齐层（原 backfill_snapshots.py）
# ════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    """兼容yfinance返回单元素Series和标量两种情况"""
    try:
        if hasattr(val, "iloc"):
            return float(val.iloc[0])
        return float(val)
    except (TypeError, ValueError):
        return None


def get_existing_days(ticker: str) -> int:
    """
    查询该股票在intraday_snapshots里已有的完整交易日数。

    口径和load_daily_summaries保持一致：
    每天至少有8根K线才算"完整交易日"（约2小时数据）。
    之前用COUNT(DISTINCT trading_date)不加此条件，
    会把只有1-2根K线的不完整日也算进去，导致日志里显示的
    "天数"和实际因子分析可用的天数不一致，引起混淆。
    """
    try:
        with sqlite3.connect(wdb.WATCHLIST_DB_PATH) as conn:
            row = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT trading_date
                    FROM intraday_snapshots
                    WHERE ticker = ?
                    GROUP BY trading_date
                    HAVING COUNT(*) >= 8
                )
            """, (ticker,)).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


def download_15m_history(ticker: str) -> Optional[pd.DataFrame]:
    """
    下载60天历史15分钟K线，只保留交易时段内的K线。
    开盘集合竞价（09:30-10:00）噪音较大，统一跳过。
    """
    try:
        df = yf.download(ticker, period=BACKFILL_PERIOD,
                         interval="15m", progress=False,
                         prepost=False, auto_adjust=True)
        if df is None or df.empty:
            log.warning(f"[{ticker}] 15分钟数据为空")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(SYD_TZ)
        else:
            df.index = df.index.tz_convert(SYD_TZ)

        times = df.index.strftime("%H:%M")
        df    = df[(times >= SESSION_START) & (times <= SESSION_END)].copy()

        if df.empty:
            log.warning(f"[{ticker}] 过滤交易时段后无数据")
            return None

        log.info(f"[{ticker}] 15分钟数据：{len(df)}根K线，"
                 f"{df.index[0].date()} 至 {df.index[-1].date()}")
        return df

    except Exception as e:
        log.error(f"[{ticker}] 15分钟数据下载失败: {e}")
        return None


def download_daily_history(ticker: str) -> Optional[pd.DataFrame]:
    """下载120天日线数据，用于计算每日prior_high基准位"""
    try:
        df = yf.download(ticker, period="120d", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception as e:
        log.error(f"[{ticker}] 日线数据下载失败: {e}")
        return None


def build_prior_high_map(daily_df: pd.DataFrame) -> dict:
    """
    为每个日期计算prior_high_20d（该日期之前20个交易日的最高价）。

    关键设计：严格用"那一天之前"的数据，不含当天。
    这是防止未来函数的核心——不能用今天的最高价去判断今天的突破。
    """
    result    = {}
    high_s    = daily_df["High"].squeeze()
    dates     = daily_df.index

    for i, dt in enumerate(dates):
        start = max(0, i - PRIOR_HIGH_WINDOW)
        window = high_s.iloc[start:i]   # 不含第i天（当天）
        if len(window) > 0:
            result[str(dt.date())] = float(window.max())

    return result


def build_vol_avg_map(df_15m: pd.DataFrame) -> dict:
    """
    为每个时段（如"10:15"）计算过去20个交易日的历史均量。
    返回 {(date_str, time_str): avg_volume}。

    用时段均量而非当日均量的理由：
    ASX早盘（10:00-11:00）成交量结构性高于下午，
    用当日均量会让早盘的"正常成交量"被误判为"放量"，
    导致大量假信号。时段均量消除了这种结构性偏差。
    """
    result = {}

    df = df_15m.copy()
    df["date_str"] = df.index.strftime("%Y-%m-%d")
    df["time_str"] = df.index.strftime("%H:%M")
    df["vol_f"]    = df["Volume"].apply(
        lambda x: float(x.iloc[0]) if hasattr(x, "iloc") else float(x)
    )

    trading_dates = sorted(df["date_str"].unique())
    time_slots    = sorted(df["time_str"].unique())

    for i, dt in enumerate(trading_dates):
        lookback = trading_dates[max(0, i - VOL_AVG_WINDOW):i]
        for ts in time_slots:
            if not lookback:
                result[(dt, ts)] = None
                continue
            hist = df[
                df["date_str"].isin(lookback) & (df["time_str"] == ts)
            ]["vol_f"].values
            result[(dt, ts)] = float(np.mean(hist)) if len(hist) > 0 else None

    return result


def backfill_ticker_to_db(ticker: str,
                           df_15m: pd.DataFrame,
                           prior_high_map: dict,
                           vol_avg_map: dict) -> int:
    """
    将历史15分钟K线写入intraday_snapshots表。
    INSERT OR IGNORE：已存在的快照自动跳过，保证幂等性。
    返回实际新写入的行数。
    """
    rows = []

    for ts, row in df_15m.iterrows():
        date_str = ts.strftime("%Y-%m-%d")
        time_str = ts.strftime("%H:%M")
        snap_iso = ts.isoformat()

        h = _safe_float(row.get("High"))
        l = _safe_float(row.get("Low"))
        c = _safe_float(row.get("Close"))
        v = _safe_float(row.get("Volume"))

        if any(x is None for x in [h, l, c, v]):
            continue
        if c <= 0 or v < 0:
            continue
        if c * v < BACKFILL_MIN_DOLLAR_VOL:
            continue

        vwap            = (h + l + c) / 3.0
        prior_high      = prior_high_map.get(date_str)
        pct_from_high   = (round((c / prior_high - 1) * 100, 2)
                           if prior_high and prior_high > 0 else None)
        hist_avg_vol    = vol_avg_map.get((date_str, time_str))
        vol_ratio       = (round(v / hist_avg_vol, 2)
                           if hist_avg_vol and hist_avg_vol > 0 else None)
        breakout_state  = ("above" if (prior_high and c >= prior_high)
                           else "below" if prior_high else "unknown")

        rows.append((
            ticker, snap_iso, date_str,
            round(c, 4), round(h, 4), round(l, 4),
            round(v, 0), round(vwap, 4),
            pct_from_high, vol_ratio, breakout_state,
        ))

    if not rows:
        return 0

    try:
        with sqlite3.connect(wdb.WATCHLIST_DB_PATH) as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO intraday_snapshots
                    (ticker, snapshot_time, trading_date,
                     price, high, low, volume, vwap,
                     pct_from_prior_high, vol_vs_avg_ratio, breakout_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
            written = conn.execute("SELECT changes()").fetchone()[0]
        return written
    except Exception as e:
        log.error(f"[{ticker}] 写入数据库失败: {e}")
        return 0


def ensure_sufficient_data(ticker: str) -> int:
    """
    检查数据天数，不足BACKFILL_THRESHOLD天时自动补齐。
    返回补齐后的实际天数。
    这个函数在每次evaluate_ticker之前调用，
    确保新加入的股票从第一次分析起就有充足历史数据。
    """
    existing = get_existing_days(ticker)

    if existing >= BACKFILL_THRESHOLD:
        return existing

    log.info(f"[{ticker}] 数据不足（{existing}天 < {BACKFILL_THRESHOLD}天），"
             f"开始历史补齐...")

    df_15m = download_15m_history(ticker)
    if df_15m is None:
        log.error(f"[{ticker}] 补齐失败：无法获取15分钟数据")
        return existing

    df_daily = download_daily_history(ticker)
    if df_daily is None:
        log.error(f"[{ticker}] 补齐失败：无法获取日线数据")
        return existing

    prior_high_map = build_prior_high_map(df_daily)
    vol_avg_map    = build_vol_avg_map(df_15m)
    written        = backfill_ticker_to_db(ticker, df_15m,
                                           prior_high_map, vol_avg_map)

    new_days = get_existing_days(ticker)
    log.info(f"[{ticker}] 补齐完成：{existing}天 → {new_days}天"
             f"（新写入{written}行）")
    return new_days

# ════════════════════════════════════════════════════════════
# 4. 数据读取层
# ════════════════════════════════════════════════════════════

def load_daily_summaries(ticker: str,
                         lookback_days: int = 70) -> pd.DataFrame:
    """
    从intraday_snapshots按trading_date聚合为日摘要。
    lookback_days设为70以覆盖补齐后的完整60天数据。

    每日摘要包含：
    - 日最高/最低价（用于振幅计算）
    - 日总成交量（用于量能斜率）
    - 收盘代理价（取每日15:xx时段最后快照）
    - 收盘时段成交量比（close_vol_ratio）
    """
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    try:
        with sqlite3.connect(wdb.WATCHLIST_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT
                    trading_date,
                    MAX(price)   AS day_high,
                    MIN(price)   AS day_low,
                    SUM(volume)  AS day_volume,
                    MAX(CASE WHEN snapshot_time LIKE '%T15:%'
                             THEN price END)           AS close_proxy,
                    MAX(CASE WHEN snapshot_time LIKE '%T15:%'
                             THEN vol_vs_avg_ratio END) AS close_vol_ratio,
                    MAX(pct_from_prior_high)            AS max_pct_from_prior_high,
                    COUNT(*)                            AS snapshot_count
                FROM intraday_snapshots
                WHERE ticker = ? AND trading_date >= ?
                GROUP BY trading_date
                ORDER BY trading_date ASC
            """, (ticker, cutoff)).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "trading_date", "day_high", "day_low", "day_volume",
            "close_proxy", "close_vol_ratio",
            "max_pct_from_prior_high", "snapshot_count",
        ])
        df["trading_date"] = pd.to_datetime(df["trading_date"])
        df = df[df["snapshot_count"] >= 8].copy()
        df = df.dropna(subset=["close_proxy"]).reset_index(drop=True)
        return df

    except Exception as e:
        log.error(f"load_daily_summaries失败 [{ticker}]: {e}")
        return pd.DataFrame()


def load_atr(ticker: str, period: int = 14) -> Optional[float]:
    """
    用日线数据计算ATR14。
    ATR基于日线定义，用15分钟数据算出的ATR是不同量纲，不可用于日线止损。
    """
    try:
        df = yf.download(ticker, period="2mo", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < period + 1:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        high  = df["High"].squeeze()
        low   = df["Low"].squeeze()
        close = df["Close"].squeeze()
        prev  = close.shift(1)
        tr    = pd.concat([
            high - low,
            (high - prev).abs(),
            (low  - prev).abs(),
        ], axis=1).max(axis=1)
        atr = float(tr.rolling(period).mean().iloc[-1])
        return round(atr, 4) if atr > 0 else None
    except Exception as e:
        log.error(f"ATR计算失败 [{ticker}]: {e}")
        return None


def load_current_price(ticker: str) -> Optional[float]:
    """取最新收盘价，用于仓位计算"""
    try:
        df = yf.download(ticker, period="3d", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return round(float(df["Close"].squeeze().iloc[-1]), 3)
    except Exception as e:
        log.error(f"获取当前价格失败 [{ticker}]: {e}")
        return None

# ════════════════════════════════════════════════════════════
# 5. 因子计算层
# ════════════════════════════════════════════════════════════

def _linreg_slope(series: pd.Series) -> float:
    """
    线性回归斜率，归一化到序列均值。
    归一化后的斜率含义是"每日相对变化率"，
    消除了不同股票绝对价格/成交量差异带来的量纲问题，
    使不同股票之间的斜率可以直接比较。
    """
    if len(series) < 3:
        return 0.0
    try:
        x    = np.arange(len(series), dtype=float)
        y    = series.values.astype(float)
        mask = ~np.isnan(y)
        if mask.sum() < 3:
            return 0.0
        slope = np.polyfit(x[mask], y[mask], 1)[0]
        mean  = np.nanmean(y)
        return float(slope / mean) if abs(mean) > 1e-10 else 0.0
    except Exception:
        return 0.0


def calc_volume_factor(df: pd.DataFrame) -> dict:
    """
    量能结构因子：
    vol_slope            — 成交量趋势斜率（负=缩量整理）
    consecutive_shrink   — 从最近一天往前连续缩量天数
    recent_spike         — 近3日是否出现异常放量（>历史均量1.8倍）
    spike_direction      — 放量时价格方向（up/down/none）
    """
    result = {
        "vol_slope": 0.0, "recent_spike": False,
        "spike_direction": "none", "consecutive_shrink": 0,
        "data_days": len(df),
    }
    if len(df) < 3:
        return result

    result["vol_slope"] = round(_linreg_slope(df["day_volume"]), 4)

    recent     = df.tail(3)
    spike_rows = recent[recent["close_vol_ratio"].fillna(0)
                        >= VOL_SPIKE_THRESHOLD]
    if not spike_rows.empty:
        result["recent_spike"] = True
        spike_day  = spike_rows.iloc[-1]
        day_range  = spike_day["day_high"] - spike_day["day_low"]
        if day_range > 0:
            close_pos = ((spike_day["close_proxy"] - spike_day["day_low"])
                         / day_range)
            result["spike_direction"] = "up" if close_pos >= 0.6 else "down"

    vols        = df["day_volume"].values
    shrink      = 0
    for i in range(len(vols) - 1, 0, -1):
        if vols[i] < vols[i - 1]:
            shrink += 1
        else:
            break
    result["consecutive_shrink"] = shrink
    return result


def calc_amplitude_factor(df: pd.DataFrame) -> dict:
    """
    振幅收窄因子：
    amplitude_slope   — 日内振幅斜率（负=收窄）
    is_shrinking      — 是否有效收窄
    avg_amplitude_pct — 近期平均振幅百分比
    confidence        — 数据天数对应的置信度（5天=low，10天=medium，20天+=high）
    """
    result = {
        "amplitude_slope": 0.0, "is_shrinking": False,
        "avg_amplitude_pct": 0.0, "confidence": "low",
    }
    if len(df) < 3:
        return result

    df = df.copy()
    df["amplitude"] = (df["day_high"] - df["day_low"]) / df["close_proxy"]
    amp_s = df["amplitude"].dropna()

    result["amplitude_slope"]   = round(_linreg_slope(amp_s), 4)
    result["avg_amplitude_pct"] = round(float(amp_s.mean()) * 100, 2)
    result["is_shrinking"]      = result["amplitude_slope"] < AMPLITUDE_SHRINK_SLOPE
    n = len(df)
    result["confidence"] = ("high" if n >= 20
                            else "medium" if n >= 10 else "low")
    return result


def calc_price_structure(df: pd.DataFrame) -> dict:
    """
    价格结构因子：
    price_slope               — 收盘价重心斜率（正=上移）
    avg_close_pos             — 平均收盘位置（在日内区间的百分位）
    above_threshold_days_pct  — 收盘位置在65%以上的天数占比
    resistance_tests          — 压力位测试次数（需≥10天数据，否则None）
    """
    result = {
        "price_slope": 0.0, "avg_close_pos": 0.0,
        "above_threshold_days_pct": 0.0, "resistance_tests": None,
    }
    if len(df) < 3:
        return result

    result["price_slope"] = round(_linreg_slope(df["close_proxy"]), 4)

    day_range = df["day_high"] - df["day_low"]
    valid     = day_range > 0
    if valid.any():
        cp = ((df["close_proxy"] - df["day_low"]) / day_range)[valid]
        result["avg_close_pos"]            = round(float(cp.mean()), 3)
        result["above_threshold_days_pct"] = round(
            float((cp >= CLOSE_POS_MIN).sum() / len(cp)), 3
        )

    if len(df) >= MIN_DAYS_FOR_EXHAUSTION:
        recent_high = float(df["day_high"].max())
        threshold   = recent_high * 0.98
        tests = sum(
            1 for _, r in df.iterrows()
            if r["day_high"] >= threshold and r["close_proxy"] < threshold
        )
        result["resistance_tests"] = tests

    return result


def calc_momentum_factor(df: pd.DataFrame) -> dict:
    """
    动能因子：
    first_volume_spike  — 是否出现"第一次放量"
                          （前N天低于均量，最近一天突然放大）
                          这是整理结束、动能启动的早期信号
    price_acceleration  — 价格变化是否在加速（二阶差分为正）
    """
    result = {"first_volume_spike": False, "price_acceleration": False}
    if len(df) < 5:
        return result

    vols     = df["day_volume"].values
    base_vol = np.mean(vols[:-1])
    if (all(v <= base_vol * 1.2 for v in vols[:-1])
            and vols[-1] > base_vol * VOL_SPIKE_THRESHOLD):
        result["first_volume_spike"] = True

    closes = df["close_proxy"].dropna().values
    if len(closes) >= 4:
        d2 = np.diff(np.diff(closes))
        if len(d2) >= 2 and d2[-1] > 0 and d2[-2] > 0:
            result["price_acceleration"] = True

    return result


def calc_pullback_health(daily_df: pd.DataFrame, item: dict) -> dict:
    """
    v3新增：判断该股票是否处于"确认趋势中的健康回调"，
    以及是否出现触底信号。

    设计前提：这不是反转/均值回归策略，是趋势跟随体系内部的
    回调入场（Pullback Entry Within Uptrend）。这只股票已经
    通过screener.py的trend_strength_score筛选进入watchlist，
    本身就证明过趋势强度——这里只是在它出现价格重心未上移时，
    进一步判断这次下跌是"健康回调"（缩量、未破MA50，等待反弹）
    还是"真正走坏"（放量下跌、跌破关键均线）。

    参数（用户确认，基于ASX中小盘股波动率校准）：
        回调幅度区间：8%-25%（相对最近20日高点）
        关键均线：MA50
        止损参考：本次回调的实际最低点

    返回dict：
        is_pullback: bool，是否处于回调区间（8%-25%回撤，未破MA50）
        is_healthy: bool，回调是否健康（缩量，不是放量下跌）
        is_bottoming: bool，是否出现触底反弹信号（在is_healthy基础上）
        depth_pct: float，当前回撤幅度
        recent_low: float，本次回调的最低点（用作止损参考）
        vol_ratio: float，回调期间量比（供日志/推送参考）
    """
    result = {
        "is_pullback": False, "is_healthy": False, "is_bottoming": False,
        "depth_pct": 0.0, "recent_low": None, "vol_ratio": None,
        "broke_ma50": False,
    }

    if len(daily_df) < PULLBACK_LOOKBACK_DAYS:
        return result

    high_col   = daily_df["day_high"]
    low_col    = daily_df["day_low"]
    close_col  = daily_df["close_proxy"]
    vol_col    = daily_df["day_volume"]

    recent_high = float(high_col.iloc[-PULLBACK_LOOKBACK_DAYS:].max())
    if recent_high <= 0:
        return result

    current_close = float(close_col.iloc[-1])
    depth_pct = round((recent_high - current_close) / recent_high * 100, 2)
    result["depth_pct"] = depth_pct

    if not (PULLBACK_MIN_DEPTH_PCT <= depth_pct <= PULLBACK_MAX_DEPTH_PCT):
        return result

    if len(close_col) >= 50:
        ma50 = float(close_col.rolling(50).mean().iloc[-1])
        if current_close < ma50:
            result["broke_ma50"] = True
            return result

    result["is_pullback"] = True

    high_idx_pos = high_col.iloc[-PULLBACK_LOOKBACK_DAYS:].values.argmax()
    pullback_start_idx = len(daily_df) - PULLBACK_LOOKBACK_DAYS + high_idx_pos

    if pullback_start_idx >= len(daily_df) - 1:
        return result

    pullback_period_low  = low_col.iloc[pullback_start_idx:]
    pullback_period_vol  = vol_col.iloc[pullback_start_idx:]
    pre_pullback_vol     = vol_col.iloc[max(0, pullback_start_idx - 10):pullback_start_idx]

    result["recent_low"] = round(float(pullback_period_low.min()), 4)

    if len(pre_pullback_vol) > 0:
        pre_avg = float(pre_pullback_vol.mean())
        during_avg = float(pullback_period_vol.mean())
        if pre_avg > 0:
            vol_ratio = round(during_avg / pre_avg, 3)
            result["vol_ratio"] = vol_ratio

            if vol_ratio <= PULLBACK_VOL_SHRINK_RATIO:
                result["is_healthy"] = True
            elif vol_ratio >= PULLBACK_VOL_DANGER_RATIO:
                result["is_healthy"] = False

    if result["is_healthy"]:
        today_high  = float(high_col.iloc[-1])
        today_low   = float(low_col.iloc[-1])
        today_close = current_close
        today_vol   = float(vol_col.iloc[-1])
        prev_vol    = float(vol_col.iloc[-2]) if len(vol_col) >= 2 else today_vol

        day_range = today_high - today_low
        close_pos = (today_close - today_low) / day_range if day_range > 0 else 0.5

        vol_uptick = (today_vol / prev_vol) if prev_vol > 0 else 1.0

        if close_pos >= BOTTOM_CLOSE_POS_MIN and vol_uptick >= BOTTOM_VOL_UPTICK_MIN:
            result["is_bottoming"] = True

    return result

# ════════════════════════════════════════════════════════════
# 6. 仓位计算
# ════════════════════════════════════════════════════════════

def calculate_position(entry_price: float, atr14: float) -> dict:
    """
    Fixed Fractional仓位计算。

    止损距离用ATR×1.5而非固定百分比的理由：
    不同波动率的股票，8%止损的实际风险暴露完全不同。
    ATR捕捉了该股票当前的真实波动特性，止损距离自适应匹配，
    不会在高波动股票上频繁被洗出，也不会在低波动股票上止损太宽。
    """
    max_loss      = TOTAL_CAPITAL * RISK_PER_TRADE
    stop_dist     = ATR_STOP_MULTIPLIER * atr14
    stop_price    = entry_price - stop_dist

    shares_by_risk = int(max_loss / stop_dist) if stop_dist > 0 else 0
    max_by_capital = int(TOTAL_CAPITAL * MAX_POSITION_PCT / entry_price)
    min_by_value   = int(MIN_POSITION_VALUE / entry_price)

    final_shares = min(shares_by_risk, max_by_capital)
    final_shares = max(final_shares, min_by_value)
    final_value  = final_shares * entry_price
    commission   = max(final_value * CMC_RATE, CMC_MIN_FEE) * 2

    return {
        "shares"         : final_shares,
        "position_value" : round(final_value, 2),
        "position_pct"   : round(final_value / TOTAL_CAPITAL * 100, 1),
        "stop_price"     : round(stop_price, 3),
        "stop_dist"      : round(stop_dist, 3),
        "commission_est" : round(commission, 2),
        "actual_risk"    : round(final_shares * stop_dist + commission, 2),
        "actual_risk_pct": round(
            (final_shares * stop_dist + commission) / TOTAL_CAPITAL * 100, 2
        ),
        "target_1r"      : round(entry_price + stop_dist * 2, 3),
        "target_2r"      : round(entry_price + stop_dist * 3, 3),
    }

# ════════════════════════════════════════════════════════════
# 7. 综合评估
# ════════════════════════════════════════════════════════════

def evaluate_ticker(item: dict) -> dict:
    """
    对单只股票完整评估：
    1. 先确保数据充足（不足则自动补齐）
    2. 加载日摘要
    3. 计算五个因子（v3新增回调健康度）
    4. 综合判断状态
    5. 计算仓位建议（ready或pullback_bottoming时）

    v3.1修复：calc_pullback_health()不再依赖price_slope<=0才触发，
    而是始终调用——它内部用最近20天高点独立判断回撤幅度，
    不受70天整体窗口斜率正负的影响。回调判断（更具体）优先于
    笼统的price_slope判断，只有当前不处于回调状态时才退回判断
    "整体价格重心是否上移"。

    v3.2修复：v3.1遗留了同一类问题在"当前不处于回调状态"这个
    退回条件里的两个残留场景——① 回撤深度>PULLBACK_MAX_DEPTH_PCT
    （可能是真实反转而非健康回调）② 回撤在8%-25%区间内但已跌破
    MA50——这两种情况calc_pullback_health()同样返回is_pullback
    =False，此前会被同一个price_slope>0判断误判为"价格重心
    上移✅"。v3.2在退回price_slope判断之前，新增两个显式分支
    分别捕获这两种情况并计入warnings，不再被笼统掩盖。
    """
    ticker = item["ticker"]
    log.info(f"评估 [{ticker}]...")

    data_days = ensure_sufficient_data(ticker)

    if data_days < MIN_DAYS_FOR_ANALYSIS:
        return {
            "ticker"     : ticker,
            "company_name": item.get("company_name", ticker),
            "status"     : "accumulating",
            "data_days"  : data_days,
            "signal_count": 0,
            "signals"    : [],
            "warnings"   : [],
            "message"    : f"数据积累中（{data_days}/{MIN_DAYS_FOR_ANALYSIS}天）",
        }

    daily_df = load_daily_summaries(ticker)
    n_days   = len(daily_df)

    if n_days < MIN_DAYS_FOR_ANALYSIS:
        return {
            "ticker"     : ticker,
            "company_name": item.get("company_name", ticker),
            "status"     : "accumulating",
            "data_days"  : n_days,
            "signal_count": 0,
            "signals"    : [],
            "warnings"   : [],
            "message"    : f"日摘要不足（{n_days}天）",
        }

    vol_f   = calc_volume_factor(daily_df)
    amp_f   = calc_amplitude_factor(daily_df)
    price_f = calc_price_structure(daily_df)
    mom_f   = calc_momentum_factor(daily_df)

    atr14         = load_atr(ticker)
    current_price = load_current_price(ticker)

    signals  = []
    warnings = []

    if (vol_f["vol_slope"] < VOL_SHRINK_SLOPE_MAX
            or vol_f["consecutive_shrink"] >= 3):
        signals.append("量能缩量整理✅")
    else:
        warnings.append("量能未见有效缩量")

    if vol_f["recent_spike"]:
        if vol_f["spike_direction"] == "up":
            signals.append("近期向上放量✅")
        else:
            warnings.append("⚠️ 近期向下放量（可能出货信号）")

    if amp_f["is_shrinking"]:
        conf = "（参考，数据天数有限）" if amp_f["confidence"] == "low" else ""
        signals.append(f"振幅收窄整理✅{conf}")
    else:
        warnings.append("振幅未见收窄")

    # v3.1修复：不再用price_f["price_slope"]的正负作为是否检查回调的
    # 前置门槛。calc_pullback_health()内部用最近20天高点独立判断回撤
    # 幅度，不依赖整体窗口斜率，因此应始终调用；回调判断（更具体、
    # 信息量更大）优先于笼统的整体价格重心判断。
    #
    # 修复前的问题：若一只股票前50天暴涨、最近10-20天回调，
    # 70天窗口的整体price_slope几乎总是仍为正，导致回调判断分支
    # 永远不会被触发，pullback_healthy/pullback_bottoming状态形同虚设。
    # 已用合成数据验证：此类"暴涨后回调"场景下price_slope恒为正，
    # 修复前判定为"价格重心上移✅"（回调信号被完全掩盖），
    # 修复后正确判定为"健康回调中"/"触底反弹信号"。
    pullback_result = calc_pullback_health(daily_df, item)

    if pullback_result["is_bottoming"]:
        signals.append(
            f"🔵 健康回调触底反弹信号✅（回撤{pullback_result['depth_pct']}%，"
            f"量比{pullback_result['vol_ratio']}，未破MA50）"
        )
    elif pullback_result["is_healthy"]:
        signals.append(
            f"🔵 健康回调中（回撤{pullback_result['depth_pct']}%，缩量，"
            f"等待触底信号）"
        )
    elif pullback_result["is_pullback"] and not pullback_result["is_healthy"]:
        warnings.append(
            f"⚠️ 回调但非缩量（回撤{pullback_result['depth_pct']}%，"
            f"量比{pullback_result.get('vol_ratio','N/A')}，非健康回调）"
        )
    elif pullback_result["depth_pct"] > PULLBACK_MAX_DEPTH_PCT:
        warnings.append(
            f"⚠️ 回撤{pullback_result['depth_pct']}%，超过健康回调上限"
            f"（{PULLBACK_MAX_DEPTH_PCT}%），存在趋势反转风险，非趋势内正常回调"
        )
    elif pullback_result["broke_ma50"]:
        warnings.append(
            f"⚠️ 回撤{pullback_result['depth_pct']}%但已跌破MA50，"
            f"非健康回调，警惕趋势走坏"
        )
    elif price_f["price_slope"] > 0:
        signals.append("价格重心上移✅")
    else:
        warnings.append("价格重心未见上移")

    if price_f["above_threshold_days_pct"] >= 0.6:
        signals.append(
            f"收盘持续偏强✅"
            f"（{price_f['above_threshold_days_pct']*100:.0f}%天数收于高位）"
        )

    if price_f["resistance_tests"] is not None:
        t = price_f["resistance_tests"]
        if 2 <= t <= 8:
            signals.append(f"压力位测试{t}次（卖盘耗尽中）✅")
        elif t > 8:
            warnings.append(f"压力位测试次数过多({t}次)，突破难度大")

    if mom_f["first_volume_spike"]:
        signals.append("🔥 第一次放量（整理结束早期信号）")
    if mom_f["price_acceleration"]:
        signals.append("价格加速上涨✅")

    sig_count  = len([s for s in signals if "✅" in s or "🔥" in s])
    warn_count = len(warnings)

    # v3.1：pullback_result现在始终有值（不再是None/dict二选一），
    # 状态判断逻辑不变——仍优先检查触底反弹/健康回调信号，
    # 因为这两个信号比常规ready更需要立刻提醒用户。
    if pullback_result["is_bottoming"]:
        status = "pullback_bottoming"
    elif pullback_result["is_healthy"]:
        status = "pullback_healthy"
    elif sig_count >= 4 and warn_count == 0:
        status = "ready"
    elif sig_count >= 3 and warn_count <= 1:
        status = "watch"
    elif any("向下放量" in w for w in warnings):
        status = "caution"
    else:
        status = "accumulating"

    position_advice = None
    if status in ("ready", "pullback_bottoming") and atr14 and current_price:
        position_advice = calculate_position(current_price, atr14)

    return {
        "ticker"          : ticker,
        "company_name"    : item.get("company_name", ticker),
        "tier_label"      : item.get("tier_label", ""),
        "composite_score" : item.get("composite_score"),
        "data_days"       : n_days,
        "status"          : status,
        "signals"         : signals,
        "warnings"        : warnings,
        "signal_count"    : sig_count,
        "vol_slope"       : vol_f["vol_slope"],
        "consecutive_shrink": vol_f["consecutive_shrink"],
        "recent_spike"    : vol_f["recent_spike"],
        "spike_direction" : vol_f["spike_direction"],
        "amplitude_slope" : amp_f["amplitude_slope"],
        "amplitude_conf"  : amp_f["confidence"],
        "price_slope"     : price_f["price_slope"],
        "avg_close_pos"   : price_f["avg_close_pos"],
        "resistance_tests": price_f["resistance_tests"],
        "first_vol_spike" : mom_f["first_volume_spike"],
        "price_accel"     : mom_f["price_acceleration"],
        "atr14"           : atr14,
        "current_price"   : current_price,
        "position_advice" : position_advice,
        "pullback_depth_pct" : pullback_result["depth_pct"],
        "pullback_recent_low": pullback_result["recent_low"],
        "pullback_vol_ratio" : pullback_result["vol_ratio"],
    }

# ════════════════════════════════════════════════════════════
# 8. 报告格式化
# ════════════════════════════════════════════════════════════

def format_premarket_report(results: list, run_date: str) -> str:
    ready   = [r for r in results if r["status"] == "ready"]
    watch   = [r for r in results if r["status"] == "watch"]
    caution = [r for r in results if r["status"] == "caution"]
    accum   = [r for r in results if r["status"] == "accumulating"]
    bottoming = [r for r in results if r["status"] == "pullback_bottoming"]
    pullback  = [r for r in results if r["status"] == "pullback_healthy"]

    lines = [
        f"📊 <b>盘前分析简报</b> {run_date}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"监测：{len(results)}只 | 🟢今日关注：{len(ready)} | "
        f"🔵触底反弹：{len(bottoming)} | 🔷健康回调：{len(pullback)} | "
        f"🟡观察中：{len(watch)} | 🔴注意：{len(caution)} | "
        f"⏳积累中：{len(accum)}",
        "",
    ]

    if bottoming:
        lines.append("🔵 <b>健康回调触底反弹（趋势内回调入场机会）</b>")
        for r in bottoming:
            lines.append(f"\n<b>{r['ticker']}</b> {r.get('company_name','')}")
            lines.append(f"数据：{r['data_days']}天 | "
                        f"本次回调幅度：{r.get('pullback_depth_pct','N/A')}% "
                        f"（相对最近20日高点）")
            for s in r["signals"]:
                lines.append(f"  • {s}")

            pos = r.get("position_advice")
            if pos:
                lines.append(f"\n  💼 <b>仓位建议</b>（ATR动态计算）")
                lines.append(f"  参考入场价：${r['current_price']}")
                lines.append(f"  建议股数：{pos['shares']}股 | "
                             f"仓位：${pos['position_value']}"
                             f"（总资金{pos['position_pct']}%）")

            recent_low = r.get("pullback_recent_low")
            if recent_low:
                lines.append(f"\n  🛑 止损参考：${recent_low}"
                            f"（本次回调最低点，跌破视为判断证伪）")
            lines.append(f"\n  📌 入场逻辑：确认过的上升趋势中出现健康回调"
                        f"（缩量、未破MA50），今日出现止跌企稳信号"
                        f"（收盘位置强+温和放量）")
            lines.append(f"  ⚠️ 基于{r['data_days']}天历史数据，"
                        f"请结合实时盘口确认后执行\n")

    if pullback:
        lines.append("\n🔷 <b>健康回调中（等待触底信号）</b>")
        for r in pullback:
            lines.append(
                f"  {r['ticker']} {r.get('company_name','')} "
                f"（回撤{r.get('pullback_depth_pct','N/A')}%，缩量中）"
            )

    if ready:
        lines.append("\n🟢 <b>今日重点关注</b>")
        for r in sorted(ready, key=lambda x: x["signal_count"], reverse=True):
            lines.append(f"\n<b>{r['ticker']}</b> {r.get('company_name','')}")
            lines.append(f"数据：{r['data_days']}天 | 信号：{r['signal_count']}个")
            for s in r["signals"]:
                lines.append(f"  • {s}")
            for w in r.get("warnings", []):
                lines.append(f"  ⚠️ {w}")

            pos = r.get("position_advice")
            if pos:
                lines.append(f"\n  💼 <b>仓位建议</b>（ATR动态计算）")
                lines.append(f"  参考入场价：${r['current_price']}")
                lines.append(f"  建议股数：{pos['shares']}股 | "
                             f"仓位：${pos['position_value']}"
                             f"（总资金{pos['position_pct']}%）")
                lines.append(f"  止损价：${pos['stop_price']}"
                             f"（距入场{pos['stop_dist']}，"
                             f"{pos['stop_dist']/r['current_price']*100:.1f}%）")
                lines.append(f"  1:2目标：${pos['target_1r']} | "
                             f"1:3目标：${pos['target_2r']}")
                lines.append(f"  预估手续费：${pos['commission_est']} | "
                             f"总风险：${pos['actual_risk']}"
                             f"（{pos['actual_risk_pct']}%）")

            lines.append(f"\n  📌 <b>入场条件</b>")
            if r.get("first_vol_spike"):
                lines.append("  A. 开盘放量站上昨日高点，"
                             "10:30前回踩VWAP不破则限价入场")
            else:
                lines.append("  A. 出现放量突破整理区间上沿，"
                             "15分钟K线收盘确认后入场")
            lines.append("  B. 今日15:30前收盘在全天高点2%以内，"
                         "尾盘入场，次日持续确认")
            conf_note = ("（振幅因子置信度有限，参考为主）"
                         if r.get("amplitude_conf") == "low" else "")
            lines.append(f"\n  ⚠️ 基于{r['data_days']}天历史数据{conf_note}，"
                         f"请结合实时盘口确认后执行")

    if watch:
        lines.append("\n🟡 <b>观察中</b>")
        for r in watch:
            sc = r.get("composite_score")
            sc_str = f" 评分:{sc}" if sc is not None else ""
            lines.append(f"  {r['ticker']} {r.get('company_name','')}"
                         f"（{r['data_days']}天{sc_str}）"
                         f" — {r['signal_count']}个信号")
            for w in r.get("warnings", []):
                lines.append(f"    ⚠️ {w}")

    if caution:
        lines.append("\n🔴 <b>注意（风险信号）</b>")
        for r in caution:
            lines.append(f"  {r['ticker']} — "
                         + " / ".join(r.get("warnings", [])))

    if accum:
        lines.append(f"\n⏳ 积累中：" +
                     "、".join(r["ticker"] for r in accum))

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "⚠️ 数据源：yfinance（15-20分钟延迟）。"
        "仓位建议须结合实时盘口确认后执行。不构成投资建议。",
    ]
    return "\n".join(lines)


def format_postmarket_report(results: list, run_date: str) -> str:
    status_emoji = {
        "ready": "🟢", "watch": "🟡",
        "caution": "🔴", "accumulating": "⏳",
        "pullback_bottoming": "🔵", "pullback_healthy": "🔷",
    }
    lines = [
        f"📋 <b>收盘后状态更新</b> {run_date}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in results:
        em  = status_emoji.get(r["status"], "❓")
        sc  = r.get("composite_score")
        sc_str = f" 评分:{sc}" if sc is not None else ""
        sig = r.get("signal_count", "-")
        lines.append(
            f"{em} {r['ticker']} {r.get('company_name','')}"
            f"（{r.get('data_days',0)}天{sc_str}）"
            f" — {r.get('message', r['status'])} [{sig}个信号]"
        )
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ════════════════════════════════════════════════════════════
# 9. 主入口
# ════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="ASX每日跨日因子分析")
    parser.add_argument("--mode",
                        choices=["premarket", "postmarket"],
                        default="premarket")
    args     = parser.parse_args()
    run_date = date.today().isoformat()

    log.info(f"=== daily_analysis.py v3.2 [{args.mode}] {run_date} ===")

    wdb.init_watchlist_db()
    watchlist = wdb.get_active_watchlist()

    if not watchlist:
        log.info("监测队列为空，退出")
        send_telegram(
            f"📊 daily_analysis [{args.mode}] {run_date}：监测队列为空"
        )
        return

    log.info(f"待处理股票：{len(watchlist)}只")

    results = []
    for item in watchlist:
        try:
            result = evaluate_ticker(item)
            if result:
                results.append(result)
                if args.mode == "premarket":
                    wdb.update_today_status(
                        result["ticker"],
                        result["status"],
                        signal_count=result.get("signal_count", 0),
                    )
        except Exception as e:
            log.error(f"评估异常 [{item['ticker']}]: {e}")
        time.sleep(1.5)

    if not results:
        log.warning("所有股票评估失败")
        return

    if args.mode == "premarket":
        report = format_premarket_report(results, run_date)
    else:
        report = format_postmarket_report(results, run_date)

    send_telegram(report)

    ready_count = sum(1 for r in results if r.get("status") == "ready")
    bottoming_count = sum(1 for r in results if r.get("status") == "pullback_bottoming")
    log.info(f"=== 完成：{len(results)}只评估，{ready_count}只今日关注，"
             f"{bottoming_count}只触底反弹信号 ===")


if __name__ == "__main__":
    main()
