# ============================================================
# ASX SYSTEM — intraday_monitor.py  v1
#
# 长期盘中监测：监测 screener.py (EOD) 筛选出的股票，
# 在监测期内（按筛选等级T1-T4分配天数，可累加）每15分钟扫描一次，
# 判断三种入场模式是否触发，仅在有信号时推送Telegram。
#
# 三种模式（均为15分钟K线级别的"代理判断"，非逐笔tick级）：
#   模式1 突破瞬间买：15分钟K线收盘突破prior_high_20d + 放量 + 未被砸回
#   模式2 回踩确认买：突破后回踩缩量企稳，重新拉升的那一根K线
#   模式3 尾盘确认买：15:30-15:45时段维持强势，无明显抛压
#
# 设计基线（与screener.py保持一致的风控/数据规范）：
#   - 跳过开盘集合竞价噪音（09:30前）和收盘集合竞价（16:00后）
#   - 流动性门槛与screener.py一致（避免低基数下的"虚假放量"）
#   - 所有基准位（前高/量能均值）在每个交易日开盘后锁定一次，全天不变，防未来函数
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
PULLBACK_MAX_DEPTH_PCT   = 4.0
PULLBACK_VOL_SHRINK_RATIO = 0.7
LATE_SESSION_NEAR_HIGH_PCT = 1.5
LATE_SESSION_MIN_VOL_RATIO = 0.8

HEALTH_MIN_RS_VS_XJO = 0.97
HEALTH_BELOW_MA50_GRACE_DAYS = 2

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
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, period="1d", interval="15m",
                              progress=False, prepost=False)
            if df is None or df.empty:
                log.warning(f"日内K线为空 [{ticker}] 第{attempt}次")
                time.sleep(2)
                continue
            return df
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

def lock_daily_reference(item: dict) -> Optional[dict]:
    ticker = item["ticker"]
    today  = date.today().isoformat()

    if item.get("ref_date") == today and item.get("prior_high_20d"):
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

        wdb.update_daily_reference(ticker, today, prior_high, prior_low, avg_vol)
        item.update({
            "ref_date": today, "prior_high_20d": prior_high,
            "prior_low_20d": prior_low, "avg_vol_20d": avg_vol,
        })
        log.info(f"基准锁定 [{ticker}]：前高{prior_high:.3f} 前低{prior_low:.3f} 均量{avg_vol:,.0f}")
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
    vwap = (h + l + c) / 3.0
    pct_from_high = round((c / prior_high - 1) * 100, 2) if prior_high else 0.0
    return {
        "time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v,
        "vwap": vwap, "pct_from_prior_high": pct_from_high,
    }


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


def detect_mode2_pullback(bars: list, prior_high: float) -> Optional[dict]:
    if len(bars) < 4 or not prior_high:
        return None

    breakout_idx = None
    for i, b in enumerate(bars):
        if b["close"] >= prior_high:
            breakout_idx = i
            break
    if breakout_idx is None or breakout_idx >= len(bars) - 2:
        return None

    post_breakout = bars[breakout_idx:]
    breakout_vol_avg = sum(b["volume"] for b in post_breakout[:2]) / min(2, len(post_breakout))

    cur, prev = bars[-1], bars[-2]
    pullback_depth_pct = (prior_high - prev["low"]) / prior_high * 100

    if prev["close"] >= prior_high:
        return None
    if pullback_depth_pct > PULLBACK_MAX_DEPTH_PCT:
        return None

    if cur["volume"] > breakout_vol_avg * PULLBACK_VOL_SHRINK_RATIO * 1.5:
        return None

    if cur["close"] <= prev["close"]:
        return None
    if cur["close"] < prior_high * (1 - PULLBACK_MAX_DEPTH_PCT / 100):
        return None

    return {
        "mode": "模式2-回踩确认买", "state": "confirmed",
        "price": cur["close"], "pullback_depth_pct": round(pullback_depth_pct, 2),
        "breakout_level": prior_high,
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
    wdb.save_snapshot(
        ticker=ticker,
        snapshot_time=cur_bar["time"].isoformat(),
        trading_date=today,
        price=cur_bar["close"], high=cur_bar["high"], low=cur_bar["low"],
        volume=cur_bar["volume"], vwap=cur_bar["vwap"],
        pct_from_prior_high=cur_bar["pct_from_prior_high"],
        vol_vs_avg_ratio=round(vol_ratio_log, 2),
        breakout_state=breakout_state_for_log,
    )

    dollar_vol = cur_bar["close"] * cur_bar["volume"]
    if dollar_vol < MIN_DOLLAR_VOLUME_INTRADAY:
        log.debug(f"[{ticker}] 本bar成交额{dollar_vol:,.0f}不达标，跳过信号判断")
        return

    # ── 跨日状态前置过滤 ─────────────────────────────────────
    # 只对daily_analysis.py盘前判定为"ready"（跨日因子达标）的股票
    # 运行三种日内模式判断，避免对整理不充分/量能未达标的股票
    # 产生大量低质量信号。
    #
    # 设计为"软性过滤"而非硬性阻断：
    # 如果today_status是unknown（daily_analysis.py从未跑过这只股票，
    # 比如刚通过/watch手动加入还没到下次盘前分析）或stale
    # （daily_analysis.py当天因故障未运行，比如VM重启错过crontab），
    # 不会直接跳过信号判断，而是记录警告日志后继续按原逻辑运行。
    # 这是为了避免"分析层单点故障导致监测层完全失效"——
    # 跨日过滤是质量优化，不应该成为系统可用性的单点风险。
    #
    # 若要改成硬性阻断（unknown/stale时也跳过），
    # 将 STRICT_STATUS_GATE 改为 True。
    STRICT_STATUS_GATE = False

    status_info = wdb.get_today_status(ticker)
    if status_info["status"] == "ready":
        pass  # 正常进入信号判断
    elif status_info["status"] in ("watch", "caution", "accumulating"):
        log.debug(f"[{ticker}] 跨日状态={status_info['status']}（非ready），"
                 f"跳过信号判断，继续积累数据")
        return
    else:
        # unknown / stale
        msg = (f"[{ticker}] 跨日状态不可用（{status_info['status']}，"
              f"原始值:{status_info.get('raw_status')}）")
        if STRICT_STATUS_GATE:
            log.warning(f"{msg}，严格模式下跳过信号判断")
            return
        else:
            log.warning(f"{msg}，降级为不做跨日过滤，仍运行信号判断")

    already_signaled_today = (item.get("last_signal_date") == today)

    signal = None

    m1 = detect_mode1_breakout(bars, prior_high, avg_vol_20d, MIN_DOLLAR_VOLUME_INTRADAY)
    if m1 and not already_signaled_today:
        signal = m1
        signal["execution_window"] = "当前/今日内尽快（突破刚发生，确认窗口很短）"

    if signal is None:
        m2 = detect_mode2_pullback(bars, prior_high)
        if m2 and item.get("last_signal_mode") != "模式2-回踩确认买":
            signal = m2
            signal["execution_window"] = "当前/今日内（回踩企稳确认）"

    if signal is None and is_late_session_window():
        if item.get("last_signal_mode") != "模式3-尾盘确认买" or item.get("last_signal_date") != today:
            m3 = detect_mode3_late_session(bars, day_high, avg_vol_20d)
            if m3:
                signal = m3
                signal["execution_window"] = "次日开盘附近（尾盘锁仓确认，今日已收盘）"

    if signal is None:
        return

    price = signal["price"]
    if signal["mode"] == "模式1-突破瞬间买":
        stop_loss = round(prior_high * 0.995, 3)
        stop_logic = f"跌破突破位 ${prior_high:.3f} 立即离场"
    elif signal["mode"] == "模式2-回踩确认买":
        recent_low = min(b["low"] for b in bars[-6:])
        stop_loss = round(recent_low * 0.995, 3)
        stop_logic = f"跌破回踩低点 ${recent_low:.3f} 立即离场"
    else:
        stop_loss = round(prior_high * 0.99, 3)
        stop_logic = f"次日跌破今日突破位 ${prior_high:.3f} 视为信号失败"

    wdb.record_signal(ticker, signal["mode"], price, stop_loss)

    extra_lines = []
    if "vol_ratio" in signal:
        extra_lines.append(f"量比：{signal['vol_ratio']}x（vs当日均量）")
    if "pullback_depth_pct" in signal:
        extra_lines.append(f"回踩深度：{signal['pullback_depth_pct']}%")
    if "dist_from_high_pct" in signal:
        extra_lines.append(f"距今日高点：{signal['dist_from_high_pct']}%")
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
    # "ready"是经过daily_analysis.py盘前跨日因子确认的高质量信号，
    # "unknown/stale"是跨日分析层不可用时降级运行、未经跨日过滤的信号，
    # 两者可信度不同，必须让用户知道差异，不能用同样的措辞混在一起
    if status_info["status"] == "ready":
        status_line = "✅ 已通过跨日因子分析确认（今日ready状态）"
    else:
        status_line = (
            f"⚠️ 跨日因子分析不可用（{status_info['status']}），"
            f"本信号未经跨日质量过滤，可信度低于常规信号"
        )

    msg = (
        f"🚨 <b>入场信号触发</b>\n\n"
        f"<b>{item.get('company_name', ticker)}</b> ({ticker})\n"
        f"📍 应用策略：<b>{signal['mode']}</b>\n"
        f"⏱ 触发时间：{signal['time'].strftime('%H:%M')}\n"
        f"💰 当前价：${price:.3f}\n"
        f"{extra_text}\n\n"
        f"📌 建议执行窗口：{signal['execution_window']}\n"
        f"🛑 止损逻辑：{stop_logic}（止损价参考 ${stop_loss:.3f}）\n\n"
        f"{status_line}\n"
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
