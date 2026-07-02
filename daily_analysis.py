# ============================================================
# daily_analysis.py  v1
#
# 职责：对watchlist中的股票做跨日因子分析，
#       输出今日是否值得入场、入场条件、仓位建议。
#
# 运行时机（crontab）：
#   盘前：UTC 23:00（前日）= 悉尼09:00，开盘前1小时
#   盘后：UTC 06:30        = 悉尼16:30，screener.py跑完之后
#
# 运行方式：
#   python3 daily_analysis.py --mode premarket
#   python3 daily_analysis.py --mode postmarket
#
# 数据来源：
#   - watchlist.db（intraday_snapshots表，自积累）
#   - yfinance（补充日线数据，用于ATR计算）
#
# 因子覆盖（基于5个交易日最低门槛）：
#   ✅ 量能结构：成交量斜率 + 近日是否出现异常放量
#   ✅ 振幅收窄：日内振幅斜率（5天数据，置信度有限，明确标注）
#   ✅ 价格重心：收盘价线性回归斜率
#   ✅ 相对位置：收盘价在日内区间的位置（close_pos）
#   ❌ 压力位耗尽度：需要≥10天，数据不足时跳过
#   ❌ 跨日回踩识别：需要breakout_state历史，积累中
#
# 风控参数（基于$50,000资金 + CMC手续费0.11%/最低$7）：
#   RISK_PER_TRADE      = 0.8%（$400）
#   ATR_STOP_MULTIPLIER = 1.5
#   MIN_POSITION_VALUE  = $6,500
#   MAX_POSITION_PCT    = 20%
# ============================================================

import os
import sys
import logging
import argparse
import sqlite3
import time
from datetime import date, timedelta
from typing import Optional

import yfinance as yf
import pandas as pd
import numpy as np

import watchlist_db as wdb

# ════════════════════════════════════════════════════════════
# 0. 日志配置
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

# ════════════════════════════════════════════════════════════
# 1. 常量配置
# ════════════════════════════════════════════════════════════

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")

# 资金与风控参数
TOTAL_CAPITAL       = 50_000
RISK_PER_TRADE      = 0.008    # 0.8% = $400
ATR_STOP_MULTIPLIER = 1.5
MIN_POSITION_VALUE  = 6_500
MAX_POSITION_PCT    = 0.20
MAX_SECTOR_PCT      = 0.35
CMC_RATE            = 0.0011   # 0.11%
CMC_MIN_FEE         = 7.0      # 最低$7/笔

# 因子计算参数
MIN_DAYS_FOR_ANALYSIS   = 5    # 最少需要N个完整交易日快照
MIN_DAYS_FOR_EXHAUSTION = 10   # 压力位耗尽度需要的最少天数
DAILY_SNAPSHOT_HOUR     = "15" # 取每日15:xx的快照作为"日收盘代理"
                                # （15:30-15:45是最后有效交易时段）

# 量能异动阈值
VOL_SPIKE_THRESHOLD     = 1.8  # 当日量 > N倍历史均量 = 异常放量
VOL_SHRINK_SLOPE_MAX    = -0.02  # 量能斜率低于此值才算"有效缩量"
AMPLITUDE_SHRINK_SLOPE  = -0.001 # 振幅斜率低于此值才算"有效收窄"
CLOSE_POS_MIN           = 0.65   # 收盘位置需在日内区间上65%以上

# ════════════════════════════════════════════════════════════
# 2. Telegram推送
# ════════════════════════════════════════════════════════════

import requests as req

def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log.warning("Telegram未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            r = req.post(url, json={
                "chat_id": CHAT_ID, "text": chunk,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=10)
            r.raise_for_status()
        except Exception as e:
            log.error(f"Telegram推送失败: {e}")
        time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 3. 数据读取层
# ════════════════════════════════════════════════════════════

def load_daily_summaries(ticker: str, lookback_days: int = 30) -> pd.DataFrame:
    """
    从intraday_snapshots读取历史快照，
    按trading_date聚合成"每日摘要"：
      - 每日最后一条快照（收盘代理）的价格
      - 每日最高价/最低价（日内振幅）
      - 每日总成交量

    为什么不直接用yfinance日线：
      - 日内snapshots是自己积累的、和watchlist完全对齐的数据
      - 不依赖外部API，稳定性更高
      - 包含vol_vs_avg_ratio等已经计算好的字段
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
                    -- 取每日最晚快照的收盘价（15:xx时段）
                    MAX(CASE WHEN snapshot_time LIKE '%T15:%' THEN price END) AS close_proxy,
                    MAX(CASE WHEN snapshot_time LIKE '%T15:%' THEN vol_vs_avg_ratio END) AS close_vol_ratio,
                    MAX(pct_from_prior_high) AS max_pct_from_prior_high,
                    COUNT(*) AS snapshot_count
                FROM intraday_snapshots
                WHERE ticker = ? AND trading_date >= ?
                GROUP BY trading_date
                ORDER BY trading_date ASC
            """, (ticker, cutoff)).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "trading_date", "day_high", "day_low", "day_volume",
            "close_proxy", "close_vol_ratio", "max_pct_from_prior_high",
            "snapshot_count"
        ])
        df["trading_date"] = pd.to_datetime(df["trading_date"])
        # 排除快照数量过少的日期（不完整交易日，比如节假日或系统刚启动那天）
        df = df[df["snapshot_count"] >= 8].copy()
        df = df.dropna(subset=["close_proxy"]).reset_index(drop=True)
        return df

    except Exception as e:
        log.error(f"load_daily_summaries失败 [{ticker}]: {e}")
        return pd.DataFrame()


def load_atr(ticker: str, period: int = 14) -> Optional[float]:
    """
    用yfinance日线数据计算ATR14。
    单独用日线算ATR是正确的——ATR本来就是基于日线定义的，
    用15分钟K线算出来的ATR和日线ATR是完全不同的量纲。
    """
    try:
        df = yf.download(ticker, period="2mo", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < period + 1:
            return None

        high  = df["High"].squeeze()
        low   = df["Low"].squeeze()
        close = df["Close"].squeeze()

        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr = float(tr.rolling(period).mean().iloc[-1])
        return round(atr, 4) if atr > 0 else None
    except Exception as e:
        log.error(f"ATR计算失败 [{ticker}]: {e}")
        return None


def load_current_price(ticker: str) -> Optional[float]:
    """取最新收盘价，用于仓位计算。"""
    try:
        df = yf.download(ticker, period="3d", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        return round(float(df["Close"].squeeze().iloc[-1]), 3)
    except Exception as e:
        log.error(f"获取当前价格失败 [{ticker}]: {e}")
        return None

# ════════════════════════════════════════════════════════════
# 4. 因子计算层
# ════════════════════════════════════════════════════════════

def _linreg_slope(series: pd.Series) -> float:
    """计算序列的线性回归斜率，归一化到均值（避免量纲影响）"""
    if len(series) < 3:
        return 0.0
    try:
        x = np.arange(len(series), dtype=float)
        y = series.values.astype(float)
        # 去除NaN
        mask = ~np.isnan(y)
        if mask.sum() < 3:
            return 0.0
        slope = np.polyfit(x[mask], y[mask], 1)[0]
        mean_y = np.nanmean(y)
        # 归一化：斜率除以均值，得到"每日变化率"
        return float(slope / mean_y) if abs(mean_y) > 1e-10 else 0.0
    except Exception:
        return 0.0


def calc_volume_factor(df: pd.DataFrame) -> dict:
    """
    量能结构因子：
    - vol_slope：成交量线性回归斜率（负=缩量，正=放量）
    - recent_spike：近3日是否出现异常放量
    - spike_direction：放量时价格方向（up/down/none）
    - consecutive_shrink：连续缩量天数

    注：vol_vs_avg_ratio是和历史同时段均量的比值，
    比当日均量更能反映放量的真实意义（已在intraday_monitor中计算并存入DB）
    """
    result = {
        "vol_slope": 0.0,
        "recent_spike": False,
        "spike_direction": "none",
        "consecutive_shrink": 0,
        "data_days": len(df),
    }
    if len(df) < 3:
        return result

    vol_series = df["day_volume"].dropna()
    result["vol_slope"] = round(_linreg_slope(vol_series), 4)

    # 近3日是否出现异常放量（用close_vol_ratio字段，已是历史同时段归一化后的比值）
    recent = df.tail(3)
    spike_rows = recent[recent["close_vol_ratio"] >= VOL_SPIKE_THRESHOLD]
    if not spike_rows.empty:
        result["recent_spike"] = True
        # 判断放量时的价格方向
        spike_day = spike_rows.iloc[-1]
        day_range = spike_day["day_high"] - spike_day["day_low"]
        if day_range > 0:
            close_pos = (spike_day["close_proxy"] - spike_day["day_low"]) / day_range
            result["spike_direction"] = "up" if close_pos >= 0.6 else "down"

    # 连续缩量天数（从最近一天往前数）
    daily_vols = df["day_volume"].values
    shrink_count = 0
    for i in range(len(daily_vols) - 1, 0, -1):
        if daily_vols[i] < daily_vols[i - 1]:
            shrink_count += 1
        else:
            break
    result["consecutive_shrink"] = shrink_count

    return result


def calc_amplitude_factor(df: pd.DataFrame) -> dict:
    """
    振幅收窄因子：
    - amplitude_slope：日内振幅（(high-low)/close）的线性回归斜率
    - is_shrinking：振幅是否在有效收窄
    - avg_amplitude_pct：平均振幅百分比（判断股票本身的波动率水平）

    注：5天数据的斜率置信度有限，结论会明确标注"参考"而非"确定"
    """
    result = {
        "amplitude_slope": 0.0,
        "is_shrinking": False,
        "avg_amplitude_pct": 0.0,
        "confidence": "low",   # 5天=low，10天+=medium，20天+=high
    }
    if len(df) < 3:
        return result

    df = df.copy()
    df["amplitude"] = (df["day_high"] - df["day_low"]) / df["close_proxy"]
    amp_series = df["amplitude"].dropna()

    result["amplitude_slope"]  = round(_linreg_slope(amp_series), 4)
    result["avg_amplitude_pct"] = round(float(amp_series.mean()) * 100, 2)
    result["is_shrinking"]     = result["amplitude_slope"] < AMPLITUDE_SHRINK_SLOPE

    # 置信度基于数据天数
    n = len(df)
    result["confidence"] = "high" if n >= 20 else "medium" if n >= 10 else "low"

    return result


def calc_price_structure(df: pd.DataFrame) -> dict:
    """
    价格结构因子：
    - price_slope：收盘价线性回归斜率（正=价格重心上移）
    - avg_close_pos：平均收盘位置（在日内区间的百分位）
    - above_threshold_days：收盘位置在65%以上的天数占比
    - resistance_tests：价格触及历史高点附近（2%以内）但未突破的次数
                        （仅在数据≥10天时计算，否则返回None）
    """
    result = {
        "price_slope": 0.0,
        "avg_close_pos": 0.0,
        "above_threshold_days_pct": 0.0,
        "resistance_tests": None,  # None = 数据不足，不展示
    }
    if len(df) < 3:
        return result

    df = df.copy()
    close_series = df["close_proxy"].dropna()
    result["price_slope"] = round(_linreg_slope(close_series), 4)

    # 收盘位置
    day_range = df["day_high"] - df["day_low"]
    valid = day_range > 0
    if valid.any():
        close_pos = ((df["close_proxy"] - df["day_low"]) / day_range)[valid]
        result["avg_close_pos"] = round(float(close_pos.mean()), 3)
        result["above_threshold_days_pct"] = round(
            float((close_pos >= CLOSE_POS_MIN).sum() / len(close_pos)), 3
        )

    # 压力位耗尽度（仅数据≥10天时计算）
    if len(df) >= MIN_DAYS_FOR_EXHAUSTION:
        recent_high = float(df["day_high"].max())
        threshold = recent_high * 0.98   # 2%以内视为"触及压力位"
        tests = 0
        for _, row in df.iterrows():
            if row["day_high"] >= threshold and row["close_proxy"] < threshold:
                tests += 1
        result["resistance_tests"] = tests

    return result


def calc_momentum_factor(df: pd.DataFrame) -> dict:
    """
    动能因子：
    - first_volume_spike：是否出现"第一次放量"
      （前N天都低于均量，最近1天突然放大）
      这是整理结束、动能启动的重要早期信号
    - price_acceleration：价格变化率是否在加速（二阶导数为正）
    """
    result = {
        "first_volume_spike": False,
        "price_acceleration": False,
    }
    if len(df) < 5:
        return result

    vols = df["day_volume"].values
    # 前N-1天的均量
    baseline_vol = np.mean(vols[:-1])
    last_vol = vols[-1]
    # 前N-1天都低于均量的1.2倍，最后一天突然>1.5倍均量
    prev_all_low = all(v <= baseline_vol * 1.2 for v in vols[:-1])
    if prev_all_low and last_vol > baseline_vol * VOL_SPIKE_THRESHOLD:
        result["first_volume_spike"] = True

    # 价格加速：用二阶差分（收盘价的"加速度"）
    closes = df["close_proxy"].dropna().values
    if len(closes) >= 4:
        first_diff  = np.diff(closes)
        second_diff = np.diff(first_diff)
        # 最近两次加速度都为正 = 价格在加速上涨
        if len(second_diff) >= 2 and second_diff[-1] > 0 and second_diff[-2] > 0:
            result["price_acceleration"] = True

    return result

# ════════════════════════════════════════════════════════════
# 5. 综合评估与信号生成
# ════════════════════════════════════════════════════════════

def evaluate_ticker(item: dict) -> Optional[dict]:
    """
    对单只股票做完整的跨日因子评估。
    返回评估结果dict，或None（数据不足/错误时）。
    """
    ticker = item["ticker"]
    log.info(f"评估 [{ticker}]...")

    daily_df = load_daily_summaries(ticker)
    n_days   = len(daily_df)

    if n_days < MIN_DAYS_FOR_ANALYSIS:
        log.info(f"  [{ticker}] 数据不足 ({n_days}天 < {MIN_DAYS_FOR_ANALYSIS}天)，跳过")
        return {
            "ticker":     ticker,
            "status":     "accumulating",
            "data_days":  n_days,
            "message":    f"数据积累中（{n_days}/{MIN_DAYS_FOR_ANALYSIS}天）",
        }

    # 计算四个因子
    vol_factor   = calc_volume_factor(daily_df)
    amp_factor   = calc_amplitude_factor(daily_df)
    price_factor = calc_price_structure(daily_df)
    mom_factor   = calc_momentum_factor(daily_df)

    # 获取ATR和当前价格（用于仓位计算）
    atr14         = load_atr(ticker)
    current_price = load_current_price(ticker)

    # 综合信号判断
    signals = []
    warnings = []

    # 因子1：量能结构
    vol_ok = (vol_factor["vol_slope"] < VOL_SHRINK_SLOPE_MAX or
              vol_factor["consecutive_shrink"] >= 3)
    if vol_ok:
        signals.append("量能缩量整理✅")
    else:
        warnings.append("量能未见有效缩量")

    # 放量方向检查（如果出现放量，必须是向上放量才有价值）
    if vol_factor["recent_spike"]:
        if vol_factor["spike_direction"] == "up":
            signals.append("近期出现向上放量✅")
        elif vol_factor["spike_direction"] == "down":
            warnings.append("⚠️ 近期出现向下放量（可能是出货信号）")

    # 因子2：振幅收窄
    if amp_factor["is_shrinking"]:
        conf_note = "（参考，数据天数有限）" if amp_factor["confidence"] == "low" else ""
        signals.append(f"振幅收窄整理✅{conf_note}")
    else:
        warnings.append("振幅未见收窄")

    # 因子3：价格结构
    if price_factor["price_slope"] > 0:
        signals.append("价格重心上移✅")
    else:
        warnings.append("价格重心未见上移")

    if price_factor["above_threshold_days_pct"] >= 0.6:
        signals.append(f"收盘位置持续偏强✅（{price_factor['above_threshold_days_pct']*100:.0f}%天数收在高位）")

    if price_factor["resistance_tests"] is not None:
        tests = price_factor["resistance_tests"]
        if 2 <= tests <= 8:
            signals.append(f"压力位测试{tests}次（卖盘在耗尽）✅")
        elif tests > 8:
            warnings.append(f"压力位测试次数过多({tests}次)，突破阻力极强，谨慎")

    # 因子4：动能
    if mom_factor["first_volume_spike"]:
        signals.append("🔥 第一次放量信号（整理结束早期信号）")
    if mom_factor["price_acceleration"]:
        signals.append("价格加速上涨✅")

    # 综合状态判断
    signal_count = len([s for s in signals if "✅" in s or "🔥" in s])
    warning_count = len(warnings)

    if signal_count >= 4 and warning_count == 0:
        status = "ready"        # 高质量，值得今日重点关注
    elif signal_count >= 3 and warning_count <= 1:
        status = "watch"        # 接近成熟，继续观察
    elif "向下放量" in str(warnings):
        status = "caution"      # 出现危险信号
    else:
        status = "accumulating" # 继续积累

    # 仓位建议（只在status=ready时计算）
    position_advice = None
    if status == "ready" and atr14 and current_price:
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
        "signal_count"    : signal_count,
        # 因子明细（供日志和回测参考）
        "vol_slope"               : vol_factor["vol_slope"],
        "consecutive_shrink"      : vol_factor["consecutive_shrink"],
        "recent_spike"            : vol_factor["recent_spike"],
        "spike_direction"         : vol_factor["spike_direction"],
        "amplitude_slope"         : amp_factor["amplitude_slope"],
        "amplitude_confidence"    : amp_factor["confidence"],
        "avg_amplitude_pct"       : amp_factor["avg_amplitude_pct"],
        "price_slope"             : price_factor["price_slope"],
        "avg_close_pos"           : price_factor["avg_close_pos"],
        "resistance_tests"        : price_factor["resistance_tests"],
        "first_volume_spike"      : mom_factor["first_volume_spike"],
        "price_acceleration"      : mom_factor["price_acceleration"],
        "atr14"                   : atr14,
        "current_price"           : current_price,
        "position_advice"         : position_advice,
    }

# ════════════════════════════════════════════════════════════
# 6. 仓位计算
# ════════════════════════════════════════════════════════════

def calculate_position(entry_price: float, atr14: float) -> dict:
    """
    Fixed Fractional仓位计算。
    所有参数基于：$50,000资金 + CMC手续费0.11%/最低$7。

    为什么用ATR而不是固定8-10%止损：
    不同波动率的股票，8%的止损意义完全不同。
    ATR天然捕捉了该股票的真实波动特性，止损距离和股票本身的
    波动幅度相匹配，不会被日常噪音洗出去，也不会在极端波动时
    损失过大。
    """
    max_loss      = TOTAL_CAPITAL * RISK_PER_TRADE     # $400
    stop_distance = ATR_STOP_MULTIPLIER * atr14        # 1.5 × ATR
    stop_price    = entry_price - stop_distance

    # 基于风险计算的股数
    shares_by_risk = int(max_loss / stop_distance) if stop_distance > 0 else 0

    # 三重约束
    max_by_capital = int(TOTAL_CAPITAL * MAX_POSITION_PCT / entry_price)
    min_by_value   = int(MIN_POSITION_VALUE / entry_price)

    final_shares = min(shares_by_risk, max_by_capital)
    final_shares = max(final_shares, min_by_value)
    final_value  = final_shares * entry_price

    # 手续费（买入+卖出）
    commission = (max(final_value * CMC_RATE, CMC_MIN_FEE) * 2)
    actual_risk = (final_shares * stop_distance) + commission

    return {
        "shares"           : final_shares,
        "position_value"   : round(final_value, 2),
        "position_pct"     : round(final_value / TOTAL_CAPITAL * 100, 1),
        "stop_price"       : round(stop_price, 3),
        "stop_distance"    : round(stop_distance, 3),
        "commission_est"   : round(commission, 2),
        "actual_risk"      : round(actual_risk, 2),
        "actual_risk_pct"  : round(actual_risk / TOTAL_CAPITAL * 100, 2),
        "target_1r"        : round(entry_price + stop_distance * 2, 3),
        "target_2r"        : round(entry_price + stop_distance * 3, 3),
    }

# ════════════════════════════════════════════════════════════
# 7. Telegram报告格式化
# ════════════════════════════════════════════════════════════

STATUS_EMOJI = {
    "ready"       : "🟢",
    "watch"       : "🟡",
    "caution"     : "🔴",
    "accumulating": "⏳",
}

def format_premarket_report(results: list, run_date: str) -> str:
    """盘前简报：今日值得关注的股票 + 入场条件"""

    ready   = [r for r in results if r["status"] == "ready"]
    watch   = [r for r in results if r["status"] == "watch"]
    caution = [r for r in results if r["status"] == "caution"]
    accum   = [r for r in results if r["status"] == "accumulating"]

    lines = [
        f"📊 <b>盘前分析简报</b> {run_date}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
        f"监测股票：{len(results)}只 | "
        f"🟢今日关注：{len(ready)} | "
        f"🟡观察中：{len(watch)} | "
        f"🔴注意：{len(caution)} | "
        f"⏳积累中：{len(accum)}",
        "",
    ]

    # 🟢 今日重点关注（附仓位建议）
    if ready:
        lines.append("🟢 <b>今日重点关注（入场条件成立）</b>")
        for r in sorted(ready, key=lambda x: x["signal_count"], reverse=True):
            lines.append(f"\n<b>{r['ticker']}</b> {r.get('company_name','')}")
            lines.append(f"数据：{r['data_days']}个交易日 | "
                        f"信号：{r['signal_count']}个")

            for s in r["signals"]:
                lines.append(f"  • {s}")
            if r["warnings"]:
                for w in r["warnings"]:
                    lines.append(f"  ⚠️ {w}")

            pos = r.get("position_advice")
            if pos:
                lines.append(f"\n  💼 <b>仓位建议</b>（基于ATR动态计算）")
                lines.append(f"  入场参考价：${r['current_price']}")
                lines.append(f"  建议股数：{pos['shares']}股")
                lines.append(f"  仓位金额：${pos['position_value']} "
                            f"（总资金{pos['position_pct']}%）")
                lines.append(f"  止损价：${pos['stop_price']} "
                            f"（距入场{pos['stop_distance']}，约"
                            f"{pos['stop_distance']/r['current_price']*100:.1f}%）")
                lines.append(f"  1:2目标：${pos['target_1r']} | "
                            f"1:3目标：${pos['target_2r']}")
                lines.append(f"  预估手续费：${pos['commission_est']} | "
                            f"总风险：${pos['actual_risk']}"
                            f"（{pos['actual_risk_pct']}%）")

            lines.append(f"\n  📌 <b>入场条件</b>（满足任一即可考虑入场）")
            if r.get("first_volume_spike"):
                lines.append("  A. 今日开盘后放量站上昨日高点，"
                           "10:30前回踩VWAP不破则限价入场")
            else:
                lines.append("  A. 今日出现放量突破整理区间上沿，"
                           "15分钟K线确认收盘站稳后入场")
            lines.append("  B. 今日收盘前30分钟（15:30）价格仍在全天高点2%以内，"
                        "尾盘入场，次日持续观察")
            lines.append(f"\n  ⚠️ 以上基于{r['data_days']}天历史数据，"
                        f"{'振幅因子置信度有限（参考）' if r.get('amplitude_confidence')=='low' else ''}请结合实时盘口确认")

    # 🟡 观察中
    if watch:
        lines.append("\n🟡 <b>观察中（继续积累）</b>")
        for r in watch:
            pos_str = ""
            if r.get("composite_score") is not None:
                pos_str = f" | 评分:{r['composite_score']}"
            lines.append(f"  {r['ticker']} {r.get('company_name','')} "
                        f"({r['data_days']}天{pos_str}) — "
                        f"{r['signal_count']}个信号")
            for w in r.get("warnings", []):
                lines.append(f"    ⚠️ {w}")

    # 🔴 注意
    if caution:
        lines.append("\n🔴 <b>注意（出现风险信号）</b>")
        for r in caution:
            lines.append(f"  {r['ticker']} — "
                        + " / ".join(r.get("warnings", [])))

    # ⏳ 积累中
    if accum:
        lines.append(f"\n⏳ 积累中：" +
                     "、".join(r["ticker"] for r in accum))

    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️ 以上分析基于yfinance延迟数据（15-20分钟），"
                "仓位建议须结合实时盘口确认后执行。"
                "不构成投资建议。")

    return "\n".join(lines)


def format_postmarket_report(results: list, run_date: str) -> str:
    """收盘后更新：状态变化摘要"""
    lines = [
        f"📋 <b>收盘后状态更新</b> {run_date}",
        f"━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for r in results:
        emoji = STATUS_EMOJI.get(r["status"], "❓")
        score_str = (f" 评分:{r['composite_score']}"
                    if r.get("composite_score") is not None else "")
        lines.append(
            f"{emoji} {r['ticker']} {r.get('company_name','')} "
            f"({r['data_days']}天{score_str}) — "
            f"{r.get('message', r['status'])} "
            f"[{r['signal_count'] if 'signal_count' in r else '-'}个信号]"
        )
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ════════════════════════════════════════════════════════════
# 8. 主入口
# ════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="ASX每日跨日因子分析")
    parser.add_argument("--mode", choices=["premarket", "postmarket"],
                        default="premarket",
                        help="premarket=盘前简报，postmarket=收盘后更新")
    args = parser.parse_args()

    run_date = date.today().isoformat()
    log.info(f"=== daily_analysis.py 启动 [{args.mode}] {run_date} ===")

    # 读取watchlist
    wdb.init_watchlist_db()
    watchlist = wdb.get_active_watchlist()

    if not watchlist:
        log.info("监测队列为空，退出")
        send_telegram(f"📊 daily_analysis [{args.mode}] {run_date}：监测队列为空")
        return

    log.info(f"待分析股票：{len(watchlist)}只")

    # 逐只评估
    results = []
    for item in watchlist:
        try:
            result = evaluate_ticker(item)
            if result:
                results.append(result)
        except Exception as e:
            log.error(f"评估异常 [{item['ticker']}]: {e}")
        time.sleep(1.0)   # 避免yfinance请求过于密集

    if not results:
        log.warning("所有股票评估失败或数据不足")
        return

    # 格式化并推送
    if args.mode == "premarket":
        report = format_premarket_report(results, run_date)
    else:
        report = format_postmarket_report(results, run_date)

    send_telegram(report)

    # 打印摘要到日志
    ready_count = sum(1 for r in results if r.get("status") == "ready")
    log.info(f"=== 完成：{len(results)}只评估，{ready_count}只今日关注 ===")


if __name__ == "__main__":
    main()