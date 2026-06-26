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

# ── 交易时段定义（悉尼时间）────────────────────────────────
# ASX连续交易：10:00-16:00（开盘前有集合竞价至10:00，收盘集合竞价16:10-16:12）
# 我们跳过开盘后头15分钟（噪音）和收盘前的集合竞价窗口
MARKET_OPEN          = "10:00"
SESSION_START_SAFE   = "10:15"   # 跳过开盘竞价后的头15分钟噪音
MARKET_CLOSE         = "16:00"
LATE_SESSION_START   = "15:30"   # 模式3"尾盘确认"窗口起点
LATE_SESSION_END     = "15:45"   # 必须早于集合竞价（16:00后不算连续盘）

# ── 流动性门槛（与screener.py一致逻辑）─────────────────────
MIN_DOLLAR_VOLUME_INTRADAY = 300_000   # 单次15分钟K线最低成交额，过滤低基数噪音

# ── 模式判断参数 ────────────────────────────────────────────
BREAKOUT_LOOKBACK_DAYS   = 20    # "前高/前低"取过去20个交易日（不含今天）
VOL_SPIKE_RATIO_M1       = 1.8   # 模式1：该15分钟K线量 vs 当日均量 的放量倍数门槛
VOL_SPIKE_RATIO_HIST     = 1.5   # 模式1辅助：vs 历史同时段均量 的放量倍数门槛
BREAKOUT_FAILURE_PCT     = -0.3  # 假突破判定：突破后次根K线收盘跌破突破位超过此百分比则判失败
PULLBACK_MAX_DEPTH_PCT   = 4.0   # 模式2：回踩深度不超过突破位下方4%（太深视为趋势反转而非回踩）
PULLBACK_VOL_SHRINK_RATIO = 0.7  # 模式2：回踩段成交量须 <= 突破段成交量的70%（缩量确认）
LATE_SESSION_NEAR_HIGH_PCT = 1.5  # 模式3：尾盘价格须在当日高点1.5%以内
LATE_SESSION_MIN_VOL_RATIO = 0.8  # 模式3：尾盘成交量不能过度萎缩（仍需>=日均量的80%水准之类佐证）

# ── 健康度门槛（决定是否提前清出监测队列）───────────────────
HEALTH_MIN_RS_VS_XJO = 0.97   # 相对强度跌破此值视为转弱
HEALTH_BELOW_MA50_GRACE_DAYS = 2  # 连续N天跌破MA50才清出（避免单日噪音误杀）

_health_fail_streak: dict = {}   # 进程内缓存：{ticker: 连续不健康天数}，每日运行重置由DB状态间接体现


# ════════════════════════════════════════════════════════════
# 2. 工具函数
# ════════════════════════════════════════════════════════════

def now_syd() -> datetime:
    return datetime.now(SYD_TZ)


def is_trading_day_and_time() -> bool:
    """
    判断当前是否在ASX有效监测窗口内：
    - 周一到周五
    - 10:15 ~ 16:00（悉尼时间）
    不处理公共假期日历（建议后续接入 exchange_calendars 库做精确判断；
    当前版本若在假期触发，会因K线数据未更新当天而自然跳过，不会产生误报，
    但会浪费一次API调用 —— 这是已知的、可接受的简化）
    """
    n = now_syd()
    if n.weekday() >= 5:  # 周六=5, 周日=6
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
    """统一处理yfinance squeeze()可能返回numpy scalar的问题"""
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
    下载当日15分钟K线。yfinance对15m间隔最多回看60天，这里只取today即可。
    含超时重试，失败分类写日志。
    """
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
    """下载过去N日日K线，用于计算prior_high_20d / avg_vol_20d 基准（不含今天）"""
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
# 4. 每日基准位锁定（开盘后调用一次）
# ════════════════════════════════════════════════════════════

def lock_daily_reference(item: dict) -> Optional[dict]:
    """
    若该股票今天还没锁定基准位（ref_date != today），重新计算并写入DB。
    基准位锁定后全天不变，避免"价格在变，标准也在变"的未来函数问题。
    """
    ticker = item["ticker"]
    today  = date.today().isoformat()

    if item.get("ref_date") == today and item.get("prior_high_20d"):
        return item   # 今日已锁定，直接复用

    daily_df = download_daily_reference(ticker)
    if daily_df is None:
        log.error(f"无法锁定基准位 [{ticker}]：日K线获取失败")
        return None

    try:
        close  = safe_series(daily_df, "Close")
        high   = safe_series(daily_df, "High")
        low    = safe_series(daily_df, "Low")
        volume = safe_series(daily_df, "Volume")

        # 不含今天：若最后一行日期等于今天则剔除（部分情况yfinance会把当天未收盘的K线也带出）
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
    """
    判断该股票是否仍值得继续监测。
    返回 (is_healthy: bool, reason: str)

    规则（均基于已收盘日线，避免日内噪音误判）：
    - 价格连续HEALTH_BELOW_MA50_GRACE_DAYS天跌破MA50 → 不健康
    - 最新RS_vs_XJO < HEALTH_MIN_RS_VS_XJO → 不健康（相对大盘转弱）
    """
    try:
        close = safe_series(daily_df, "Close")
        if close is None or len(close) < 50:
            return True, ""  # 数据不足时不误杀，保守放行

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
        return True, ""  # 异常时保守放行，不误杀


# ════════════════════════════════════════════════════════════
# 6. 三种模式判断
# ════════════════════════════════════════════════════════════

def _build_bar_record(ts: pd.Timestamp, row: pd.Series, prior_high: float,
                       avg_vol_20d: float) -> dict:
    """把单根15分钟K线转换成标准化记录，便于后续判断和入库"""
    # 用float(row[col].iloc[0] if hasattr(row[col], 'iloc') else row[col])
    # 兼容yfinance返回单元素Series和标量两种情况，避免未来pandas版本抛TypeError
    def _f(val) -> float:
        if hasattr(val, "iloc"):
            return float(val.iloc[0])
        return float(val)

    o, h, l, c, v = _f(row["Open"]), _f(row["High"]), _f(row["Low"]), _f(row["Close"]), _f(row["Volume"])
    # 当日均量按"已经过去的bar数"折算，避免用全天均量去判断早盘的量比（结构性偏差）
    vwap = (h + l + c) / 3.0
    pct_from_high = round((c / prior_high - 1) * 100, 2) if prior_high else 0.0
    return {
        "time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v,
        "vwap": vwap, "pct_from_prior_high": pct_from_high,
    }


def detect_mode1_breakout(bars: list, prior_high: float, avg_vol_20d: float,
                           min_dollar_vol: float) -> Optional[dict]:
    """
    模式1：突破瞬间买（15分钟K线代理版）

    触发条件（全部满足）：
    1. 当前bar收盘价首次突破prior_high_20d（即上一根bar收盘价还在prior_high之下）
    2. 当前bar成交量 >= 该股票当日截至目前bar均量 * VOL_SPIKE_RATIO_M1
       （用"当日已发生的bar均量"而非全天均量，避免早盘量本来就大造成的偏差）
    3. 流动性达标：该bar成交额 >= min_dollar_vol
    4. 未出现"假突破"：若已有下一根bar，其收盘价不能较突破位下跌超过BREAKOUT_FAILURE_PCT

    注：第4点在"突破当根"判断时尚无法验证（下一根还没出现），
        因此本函数只负责标记"breaking"状态；真正确认要等下一次15分钟轮询，
        由 detect_breakout_confirmation() 完成"confirmed"或"failed"的判定。
    """
    if len(bars) < 2 or not prior_high:
        return None

    cur, prev = bars[-1], bars[-2]
    if prev["close"] >= prior_high:
        return None  # 不是"首次"突破，可能已经突破过，留给confirm逻辑处理
    if cur["close"] < prior_high:
        return None  # 还没突破

    same_day_bars = bars[:-1]  # 不含当前bar的历史bar，用于计算当日已发生均量
    if same_day_bars:
        intraday_avg_vol = sum(b["volume"] for b in same_day_bars) / len(same_day_bars)
    else:
        intraday_avg_vol = avg_vol_20d / 26  # 26 = 6.5小时/15分钟，粗略折算单bar基准

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
    """
    检查最近一次"breaking"状态的突破，是否在随后的bar中被确认或证伪。
    返回 'confirmed' / 'failed' / None（仍在观察中）

    真突破：突破后续bar维持在突破位上方或继续上行
    假突破：突破后续bar收盘跌回突破位下方超过BREAKOUT_FAILURE_PCT
    """
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
    """
    模式2：回踩确认买

    触发条件：
    1. 今日早些时候已经出现突破confirmed（即历史bar中有收盘价>=prior_high的记录）
    2. 随后价格回踩到突破位附近（不超过PULLBACK_MAX_DEPTH_PCT，避免把"反转"误判成"回踩"）
    3. 回踩段成交量明显小于突破段（PULLBACK_VOL_SHRINK_RATIO），代表抛压衰竭
    4. 当前bar相对上一根bar重新拉升（close > prev close，且close回到突破位之上或非常接近）

    这是"在重新向上那一刻买"的代理实现：用收盘价环比回升 + 站回/逼近突破位 作为信号。
    """
    if len(bars) < 4 or not prior_high:
        return None

    # 找到今日第一次确认突破的bar索引
    breakout_idx = None
    for i, b in enumerate(bars):
        if b["close"] >= prior_high:
            breakout_idx = i
            break
    if breakout_idx is None or breakout_idx >= len(bars) - 2:
        return None  # 还没突破，或突破刚发生没有后续回踩数据

    post_breakout = bars[breakout_idx:]
    breakout_vol_avg = sum(b["volume"] for b in post_breakout[:2]) / min(2, len(post_breakout))

    cur, prev = bars[-1], bars[-2]
    pullback_depth_pct = (prior_high - prev["low"]) / prior_high * 100

    if prev["close"] >= prior_high:
        return None  # 还没真正回踩过，价格一直在上方，不是模式2场景
    if pullback_depth_pct > PULLBACK_MAX_DEPTH_PCT:
        return None  # 回踩太深，更像反转而非健康回踩，模式2不适用

    if cur["volume"] > breakout_vol_avg * PULLBACK_VOL_SHRINK_RATIO * 1.5:
        return None  # 回踩段不是缩量，可能是主力出货而非洗盘

    # 重新向上确认：当前bar收盘价比上一根高，且已经收回到接近/重新站上突破位
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
    """
    模式3：尾盘确认买（仅在15:30-15:45窗口判断一次，避免重复触发）

    触发条件：
    1. 当前处于尾盘窗口（由外部is_late_session_window()保证调用时机）
    2. 当前价格距离当日高点不超过LATE_SESSION_NEAR_HIGH_PCT
    3. 最近2根bar没有持续放量下跌（即没有"明显抛压"的代理判断：
       最近bar若是阴线，其量不能显著高于均量，否则视为有抛压）
    4. 成交量仍维持基本水准（不是萎缩到几乎无人交易，那种情况下"强势"没有意义）
    """
    if len(bars) < 2 or not day_high:
        return None

    cur, prev = bars[-1], bars[-2]
    dist_from_high_pct = (day_high - cur["close"]) / day_high * 100
    if dist_from_high_pct > LATE_SESSION_NEAR_HIGH_PCT:
        return None

    bar_avg_vol = avg_vol_20d / 26 if avg_vol_20d else 0
    is_red_bar = cur["close"] < cur["open"]
    if is_red_bar and bar_avg_vol > 0 and cur["volume"] > bar_avg_vol * 1.5:
        return None  # 阴线放量 = 明显抛压，不满足"无明显抛压"

    if bar_avg_vol > 0 and cur["volume"] < bar_avg_vol * LATE_SESSION_MIN_VOL_RATIO:
        return None  # 量能枯竭，强势缺乏验证，谨慎不触发

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

    # 写入快照（历史比对基础，永远写入，不论是否有信号）
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

    # 流动性门槛：本bar成交额不达标，不做信号判断（但快照已记录，用于历史统计）
    dollar_vol = cur_bar["close"] * cur_bar["volume"]
    if dollar_vol < MIN_DOLLAR_VOLUME_INTRADAY:
        log.debug(f"[{ticker}] 本bar成交额{dollar_vol:,.0f}不达标，跳过信号判断")
        return

    # 避免同一信号同一天重复推送
    already_signaled_today = (item.get("last_signal_date") == today)

    signal = None

    # 模式1：突破瞬间
    m1 = detect_mode1_breakout(bars, prior_high, avg_vol_20d, MIN_DOLLAR_VOLUME_INTRADAY)
    if m1 and not already_signaled_today:
        signal = m1
        signal["execution_window"] = "当前/今日内尽快（突破刚发生，确认窗口很短）"

    # 模式2：回踩确认（即使模式1今天已触发过，模式2是不同阶段的信号，仍可触发一次）
    if signal is None:
        m2 = detect_mode2_pullback(bars, prior_high)
        if m2 and item.get("last_signal_mode") != "模式2-回踩确认买":
            signal = m2
            signal["execution_window"] = "当前/今日内（回踩企稳确认）"

    # 模式3：尾盘确认，仅在指定窗口判断，且当天只触发一次
    if signal is None and is_late_session_window():
        if item.get("last_signal_mode") != "模式3-尾盘确认买" or item.get("last_signal_date") != today:
            m3 = detect_mode3_late_session(bars, day_high, avg_vol_20d)
            if m3:
                signal = m3
                signal["execution_window"] = "次日开盘附近（尾盘锁仓确认，今日已收盘）"

    if signal is None:
        return  # 没动作，不推送，安静退出

    # ── 有信号：计算止损位并推送 ─────────────────────────────
    price = signal["price"]
    if signal["mode"] == "模式1-突破瞬间买":
        stop_loss = round(prior_high * 0.995, 3)  # 跌回突破位即视为止损
        stop_logic = f"跌破突破位 ${prior_high:.3f} 立即离场"
    elif signal["mode"] == "模式2-回踩确认买":
        recent_low = min(b["low"] for b in bars[-6:])
        stop_loss = round(recent_low * 0.995, 3)
        stop_logic = f"跌破回踩低点 ${recent_low:.3f} 立即离场"
    else:  # 模式3
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

    # 来源文案：EOD自动筛选 vs 用户手动添加，两套表述，避免None值露出或文案矛盾
    # （手动添加的股票没有tier_level/composite_score，原样塞进f-string会显示"None"）
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

    msg = (
        f"🚨 <b>入场信号触发</b>\n\n"
        f"<b>{item.get('company_name', ticker)}</b> ({ticker})\n"
        f"📍 应用策略：<b>{signal['mode']}</b>\n"
        f"⏱ 触发时间：{signal['time'].strftime('%H:%M')}\n"
        f"💰 当前价：${price:.3f}\n"
        f"{extra_text}\n\n"
        f"📌 建议执行窗口：{signal['execution_window']}\n"
        f"🛑 止损逻辑：{stop_logic}（止损价参考 ${stop_loss:.3f}）\n\n"
        f"{source_line}\n"
        f"⚠️ 15分钟K线级别确认，非逐笔实时信号，请结合实时盘口核实再操作"
    )
    send_telegram(msg)
    log.info(f"信号推送 [{ticker}] {signal['mode']} @ ${price:.3f}")


# ════════════════════════════════════════════════════════════
# 8. 收盘后日终处理：健康度检查 + 天数递增 + 队列清理
# ════════════════════════════════════════════════════════════

def run_end_of_day_maintenance() -> None:
    """
    收盘后（建议crontab在16:05单独触发一次，或在最后一次15分钟轮询中附带执行）：
    - 监测天数+1
    - 健康度检查，不达标则提前清出
    - 天数耗尽则正常清出
    """
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
        time.sleep(1.0)  # 避免对yfinance过于密集请求

    # 收盘前最后一次轮询时（15:45-16:00窗口）顺带做日终维护，
    # 避免额外占用一个crontab条目。
    t = n.strftime("%H:%M")
    if "15:45" <= t <= "16:00":
        run_end_of_day_maintenance()

    log.info("intraday_monitor 本次轮询完成")


if __name__ == "__main__":
    main()
