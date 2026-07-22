#!/usr/bin/env python3
"""
backtest_engine.py
====================
ASX Screener 系统 —— EOD选股逻辑历史回测引擎（v2：复用screener.py真实逻辑版）

核心设计:
    本脚本不重新实现打分逻辑，而是直接 `import screener`，复用其中的
    TIERS / _passes_tier() / calc_trend_strength_score() / calc_composite_score() /
    _check_trend_persistence() / calc_confidence()，以及最关键的三个回测基线参数
    BT_STOP_ATR_MULT / BT_TARGET_ATR_MULT / BT_TIMEOUT_DAYS。

    这意味着回测用的止盈止损/超时规则和你线上 signals_history 表完全一致，
    两者理论上可以合并统计（本脚本提供 --merge-live 选项做这件事）。

部署要求:
    本文件必须放在与 screener.py、watchlist_db.py 相同的目录下运行
    （即 ~/asx/ 或你实际的项目目录），因为它需要 `import screener`。
    不需要配置 GEMINI_API_KEY/TELEGRAM_TOKEN（screener.py在这两个环境变量
    缺失时只是把gemini_client设为None，不会报错；本脚本完全不会调用
    Gemini/Telegram/GitHub推送的任何函数）。

输出:
    结果写入独立的 backtest_results.db（不是你的 announcements.db，
    绝不触碰生产数据），表名 signals_history_backtest，
    字段与真实 signals_history 表一一对应，可直接UNION查询。

已知局限（务必读完再解读结果）:
    1. 市值门槛用当前市值做静态代理，不是历史point-in-time市值。
    2. catalyst固定为0——历史公告数据不存在，无法重建。
    3. TREND_SCORE_THRESHOLD是用近期全市场数据校准的，拿去套用更早的历史，
       存在"用后来的信息选参数"的轻微问题，不是信号计算本身泄露未来数据。
    4. 止盈止损假设完美成交（不含跳空穿仓），与线上signals_history假设一致，
       两者都可能比真实可实现的胜率更乐观。
    5. 全市场(~2000只)×多年逐日回测计算量很大，建议先用watchlist或小样本
       跑通逻辑，再决定是否扩大到全市场（那种规模建议nohup挂后台跑）。

稳定性设计:
    - yfinance请求带重试（默认5次，指数退避，最长等待60秒）
    - 连续多天信号计算失败会触发熔断（默认连续5天），自动停止并报警，
      避免系统性bug时整晚空跑
    - 失败的交易日不会被标记为"已完成"，下次用同样参数重跑会自动重新处理
      （不是简单重跑全部——只有真正失败的那些天会被重试）
    - 配置了TELEGRAM_TOKEN/TELEGRAM_CHAT_ID环境变量后，启动/完成/报错
      都会推送Telegram（复用screener.py同一个bot，同一个chat）

用法:
    # 先用watchlist里的股票跑通（几分钟级别，Oracle Free Tier能扛住）
    python3 backtest_engine.py --start 2025-07-01 --end 2026-07-01 --universe watchlist

    # 用自定义股票清单文件（每行一个ticker，如 BHP.AX）
    python3 backtest_engine.py --start 2025-07-01 --end 2026-07-01 --universe file --universe-file my_list.txt

    # 全市场（非常慢，建议 nohup python3 backtest_engine.py ... &）
    python3 backtest_engine.py --start 2025-01-01 --end 2026-07-01 --universe full

    # 跑完后，把历史回测结果和线上signals_history合并统计
    python3 backtest_engine.py --stats-only --merge-live

    # 从GitHub拉下来的参数队列文件里，跑所有还没跑过的实验
    python3 backtest_engine.py --run-queue ~/asx-backtest-configs/queue.txt --max-minutes 700
"""

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    print("缺少 yfinance: pip install yfinance --break-system-packages")
    sys.exit(1)

# ────────────────────────────────────────────────────────────
# 关键一步：把screener.py所在目录加入路径，直接复用真实逻辑
# ────────────────────────────────────────────────────────────
ASX_DIR = os.path.dirname(os.path.abspath(__file__))
if ASX_DIR not in sys.path:
    sys.path.insert(0, ASX_DIR)

try:
    import screener  # noqa: E402  必须和本文件同目录
except ImportError as e:
    print(
        "无法 import screener —— 本脚本必须和 screener.py / watchlist_db.py "
        f"放在同一目录下运行。原始错误: {e}"
    )
    sys.exit(1)

try:
    import watchlist_db as wdb  # noqa: E402  用于 --universe watchlist
except ImportError:
    wdb = None  # 只有选择 --universe watchlist 时才会真正需要，这里不强制退出

# ────────────────────────────────────────────────────────────
# 拍一份screener.py原始默认参数的快照，供--run-queue模式在同一进程里
# 连续跑多个实验之间"重置回默认值再套新覆盖"用。如果没有这份快照，
# 队列里第2个实验会不小心继承第1个实验残留的参数覆盖——因为
# apply_param_overrides()是在"当前值"上做局部覆盖，不是每次都从
# 干净的默认值出发。单次运行（不用--run-queue）不受影响，因为
# 每次调用都是全新的Python进程，screener模块本来就是干净的。
# ────────────────────────────────────────────────────────────
_SCREENER_PRISTINE_DEFAULTS = {
    "SCORE_WEIGHTS": dict(screener.SCORE_WEIGHTS),
    "TIER_BONUS": dict(screener.TIER_BONUS),
    "TREND_SCORE_THRESHOLD": dict(screener.TREND_SCORE_THRESHOLD),
    "TIERS": [dict(t) for t in screener.TIERS],
    "BT_STOP_ATR_MULT": screener.BT_STOP_ATR_MULT,
    "BT_TARGET_ATR_MULT": screener.BT_TARGET_ATR_MULT,
    "BT_TIMEOUT_DAYS": screener.BT_TIMEOUT_DAYS,
}


def reset_screener_to_defaults() -> None:
    """把screener模块的可调参数恢复到进程启动时的原始默认值。"""
    d = _SCREENER_PRISTINE_DEFAULTS
    screener.SCORE_WEIGHTS = dict(d["SCORE_WEIGHTS"])
    screener.TIER_BONUS = dict(d["TIER_BONUS"])
    screener.TREND_SCORE_THRESHOLD = dict(d["TREND_SCORE_THRESHOLD"])
    screener.TIERS = [dict(t) for t in d["TIERS"]]
    screener.BT_STOP_ATR_MULT = d["BT_STOP_ATR_MULT"]
    screener.BT_TARGET_ATR_MULT = d["BT_TARGET_ATR_MULT"]
    screener.BT_TIMEOUT_DAYS = d["BT_TIMEOUT_DAYS"]


# ════════════════════════════════════════════════════════════
# Telegram 推送 —— 复用screener.py同一个bot/chat（同样从环境变量读取）
# ════════════════════════════════════════════════════════════

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_MAX_RETRIES = 3
TELEGRAM_CHUNK_SIZE = 3500  # 留出余量，Telegram单条消息上限约4096字符


def send_telegram(text: str, logger: Optional[logging.Logger] = None) -> None:
    """
    推送到Telegram。没配置TELEGRAM_TOKEN/TELEGRAM_CHAT_ID时静默跳过
    （只记一条warning日志，不影响回测本身运行），配置了但网络失败时重试，
    重试完还失败也只记日志，绝不能因为Telegram推送失败而让整个回测崩溃——
    推送是锦上添花，不是回测能不能跑的前提条件。
    """
    log = logger or logging.getLogger("backtest")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram未配置（TELEGRAM_TOKEN/TELEGRAM_CHAT_ID），跳过推送")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [text[i:i + TELEGRAM_CHUNK_SIZE] for i in range(0, len(text), TELEGRAM_CHUNK_SIZE)] or [text]

    for chunk in chunks:
        for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
            try:
                r = requests.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID, "text": chunk,
                    "parse_mode": "HTML", "disable_web_page_preview": True,
                }, timeout=10)
                r.raise_for_status()
                break
            except Exception as e:
                if attempt < TELEGRAM_MAX_RETRIES:
                    time.sleep(2 * attempt)
                else:
                    log.error(f"Telegram推送失败（已重试{TELEGRAM_MAX_RETRIES}次）: {e}")
        time.sleep(0.4)


def send_telegram_document(file_path: str, caption: str = "",
                           logger: Optional[logging.Logger] = None) -> None:
    """
    推送文件附件到Telegram（比如CSV导出），用于需要深挖分析的场景——
    纯文字摘要给日常查看用，文件附件给"要拿去做进一步定量分析"用。
    同样是配置了才推，失败了只记日志，不影响回测主流程。
    """
    log = logger or logging.getLogger("backtest")
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram未配置，跳过文件推送")
        return
    if not os.path.exists(file_path):
        log.error(f"要推送的文件不存在: {file_path}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            with open(file_path, "rb") as f:
                r = requests.post(
                    url,
                    data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                    files={"document": (os.path.basename(file_path), f)},
                    timeout=30,
                )
            r.raise_for_status()
            log.info(f"文件已推送Telegram: {file_path}")
            return
        except Exception as e:
            if attempt < TELEGRAM_MAX_RETRIES:
                time.sleep(2 * attempt)
            else:
                log.error(f"文件推送失败（已重试{TELEGRAM_MAX_RETRIES}次）[{file_path}]: {e}")


# ════════════════════════════════════════════════════════════
# 配置
# ════════════════════════════════════════════════════════════

@dataclass
class BacktestConfig:
    start_date: str = "2025-07-01"
    end_date: str = "2026-07-01"

    universe_source: str = "watchlist"     # watchlist | file | full
    universe_file: str = ""

    min_market_cap: float = 50_000_000.0   # 与select_top3()里的硬编码门槛一致
    min_history_days: int = 60             # 与download_ohlcv()的有效性门槛一致

    # 技术指标热身缓冲：MA200/52周高点这些指标需要至少约252个交易日的
    # 滚动窗口。如果直接从start_date开始下载数据，回测最早约1年的信号会
    # 因为MA200还没"攒够"数据而系统性失真（ma200=None、w52_hi用不完整窗口）。
    # 实际下载区间会往前多拉400个日历日（覆盖约260+个交易日），
    # 但只对>=start_date的交易日生成信号，热身期本身不产出信号。
    # 拉长到5-10年回测时，这个缓冲期占比很小，但对回测最早一段的
    # 信号质量影响很大，必须加。
    warmup_calendar_days: int = 400

    db_path: str = os.path.join(ASX_DIR, "backtest_results.db")
    log_path: str = os.path.join(ASX_DIR, "backtest.log")

    benchmark_ticker: str = "^AXJO"

    param_set: str = "baseline"  # 本次实验的参数集标签，用于多轮参数对比

    # ── daily_analysis.py 跨日健康度层的近似参数（默认值与真实daily_analysis.py
    # 逐个对应，全部可通过--params-file的DAILY_HEALTH字段覆盖）──────────────
    health_vol_spike_threshold: float = 1.8      # VOL_SPIKE_THRESHOLD
    health_vol_shrink_slope_max: float = -0.02   # VOL_SHRINK_SLOPE_MAX
    health_amplitude_shrink_slope: float = -0.001  # AMPLITUDE_SHRINK_SLOPE
    health_close_pos_min: float = 0.65           # CLOSE_POS_MIN
    health_min_days_analysis: int = 5            # MIN_DAYS_FOR_ANALYSIS
    health_min_days_exhaustion: int = 10         # MIN_DAYS_FOR_EXHAUSTION
    health_lookback_days: int = 70               # load_daily_summaries的lookback_days

    # 供"补充性、非可比"的真实成本估算层使用（不影响与signals_history可比的核心统计）
    commission_pct: float = 0.0011
    commission_min_aud: float = 7.0
    slippage_bps: float = 5.0

    # 稳定性：连续多少个交易日算信号失败就熔断停止（避免系统性bug时空跑一整晚）
    max_consecutive_errors: int = 5
    push_telegram: bool = True  # 配置了TELEGRAM_TOKEN/CHAT_ID时是否推送通知


def setup_logging(log_path: str) -> logging.Logger:
    logger = logging.getLogger("backtest")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # 避免和screener.py的root logger重复写日志
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ════════════════════════════════════════════════════════════
# 数据层
# ════════════════════════════════════════════════════════════

class DataLayer:
    """历史OHLCV拉取 + 清洗 + point-in-time切片。所有网络调用带重试。"""

    def __init__(self, logger: logging.Logger, max_retries: int = 5):
        self.logger = logger
        self.max_retries = max_retries
        self._cache: dict[str, pd.DataFrame] = {}

    @staticmethod
    def _backoff_seconds(attempt: int) -> float:
        """指数退避，封顶60秒。整晚运行更需要能扛住偶发的限流/网络抖动，
        而不是像交互式场景那样追求快速失败。"""
        return min(60.0, 3.0 * (2 ** (attempt - 1)))

    def fetch(self, ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
        key = f"{ticker}|{start}|{end}"
        if key in self._cache:
            return self._cache[key]

        for attempt in range(1, self.max_retries + 1):
            try:
                df = yf.download(
                    ticker, start=start, end=end,
                    auto_adjust=True, progress=False, threads=False,
                )
                if df is None or df.empty:
                    self.logger.warning(f"{ticker}: 返回空数据 attempt={attempt}/{self.max_retries}")
                    time.sleep(self._backoff_seconds(attempt))
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df[~df.index.duplicated(keep="first")]
                df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])

                bad = df["Close"].pct_change().abs() > 0.80
                if bad.sum() > 0:
                    self.logger.warning(f"{ticker}: 剔除{bad.sum()}个疑似异常价格bar")
                    df = df[~bad]

                if len(df) < 30:
                    self.logger.warning(f"{ticker}: 有效交易日不足30天，跳过")
                    return None

                self._cache[key] = df
                return df

            except Exception as e:
                self.logger.warning(f"{ticker}: 拉取失败 attempt={attempt}/{self.max_retries} error={e}")
                time.sleep(self._backoff_seconds(attempt))

        self.logger.error(f"{ticker}: {self.max_retries}次重试后仍失败，跳过")
        return None

    @staticmethod
    def slice_up_to(df: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        """严格point-in-time切片，杜绝未来函数。"""
        return df[df.index <= as_of]


# ════════════════════════════════════════════════════════════
# Universe 解析
# ════════════════════════════════════════════════════════════

def resolve_universe(cfg: BacktestConfig, logger: logging.Logger) -> list[str]:
    if cfg.universe_source == "watchlist":
        if wdb is None:
            logger.error("watchlist_db 不可用，无法使用 --universe watchlist")
            return []
        wdb.init_watchlist_db()
        items = wdb.get_active_watchlist()
        tickers = sorted({it["ticker"] for it in items})
        logger.info(f"universe=watchlist：{len(tickers)} 只")
        return tickers

    if cfg.universe_source == "file":
        if not cfg.universe_file or not os.path.exists(cfg.universe_file):
            logger.error(f"universe文件不存在: {cfg.universe_file}")
            return []
        with open(cfg.universe_file, encoding="utf-8") as f:
            tickers = [ln.strip() for ln in f if ln.strip()]
        logger.info(f"universe=file：{len(tickers)} 只（来自 {cfg.universe_file}）")
        return tickers

    if cfg.universe_source == "full":
        logger.warning("universe=full：全市场回测计算量很大，建议先小样本验证逻辑再跑这个")
        tickers = screener.get_asx_universe()
        logger.info(f"universe=full：{len(tickers)} 只")
        return tickers

    logger.error(f"未知的universe_source: {cfg.universe_source}")
    return []


# ════════════════════════════════════════════════════════════
# 信号生成层 —— 完整复用 screener.py 的真实筛选/打分逻辑
# ════════════════════════════════════════════════════════════

class SignalGenerator:
    """
    严格复刻 screener.select_top3() 的核心逻辑（去掉Gemini/Telegram/GitHub/
    公告抓取这些I/O部分），逐日在point-in-time数据上重放：

      for tier in screener.TIERS（T1→T2→T3→T4，先到先得，一只股票只归属一个tier）:
          for ticker not in seen_tickers:
              tech = screener.build_tech_summary(df_切片, xjo_切片)
              if screener._passes_tier(tech, tier):
                  记录该ticker，附加persistence_score/composite_score/confidence

      按composite_score排序取Top10 → 市值门槛过滤 → Top3

    与真实系统的差异只有两点：
      1. catalyst固定为0（历史公告数据不存在）
      2. 市值用当前值做静态代理（yfinance无历史市值API）
    其余（TIERS阈值、trend_strength_score公式、composite_score权重、
    persistence_score计算）与screener.py逐行一致。
    """

    def __init__(self, cfg: BacktestConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger
        self._shares_cache: dict[str, float] = {}

    def _get_shares_outstanding(self, ticker: str) -> float:
        """
        当前流通股数（缓存，整个回测期间只查一次）。

        市值历史代理的改进：不用"当前总市值"直接套用到历史每一天
        （那样会把股价上涨/下跌的全部影响错误地摊到历史市值上），
        改用"当前股数 × 那一天的历史收盘价"。

        这样只剩一个残余误差来源——公司在历史区间内做过配股/回购导致
        股数变化（比如10年前股数只有现在的60%），这个误差通常比
        "直接用现在的总市值"小得多，尤其是对没有大幅稀释历史的公司。
        不是完美的point-in-time市值，但比之前的版本诚实很多。
        """
        if ticker in self._shares_cache:
            return self._shares_cache[ticker]

        shares = 0.0
        for attempt in range(1, 4):
            try:
                info = yf.Ticker(ticker).info
                shares = float(info.get("sharesOutstanding", 0) or 0)
                if shares > 0:
                    break
                self.logger.debug(f"股数获取为0 [{ticker}] attempt={attempt}/3，重试")
            except Exception as e:
                self.logger.debug(f"股数获取失败 [{ticker}] attempt={attempt}/3: {e}")
            time.sleep(2 * attempt)

        if shares <= 0:
            self.logger.warning(f"{ticker}: 3次重试后股数仍为0/获取失败，"
                                 f"该股票本次运行将无法通过市值门槛")
        self._shares_cache[ticker] = shares
        return shares

    def _market_cap_proxy(self, ticker: str, price_at_date: float) -> float:
        shares = self._get_shares_outstanding(ticker)
        if shares <= 0:
            return 0.0
        return shares * price_at_date

    def scan_day(
        self,
        as_of: pd.Timestamp,
        history: dict[str, pd.DataFrame],
        xjo_full: Optional[pd.Series],
    ) -> tuple[list[dict], list[dict], int, int]:
        """
        返回 (raw_top10, selected_top3, error_count, attempted_count)。

        error_count/attempted_count是为了让外层熔断机制能识别"系统性故障"：
        单只股票的build_tech_summary/_passes_tier异常在这里就地捕获+跳过
        （一只股票数据有问题不该拖累整天的其他股票），但如果这一天
        attempted_count>0且error_count==attempted_count（也就是"今天
        尝试评估的股票全部出错"），说明大概率不是个别股票的数据问题，
        而是参数/代码层面的系统性bug——这种情况下"没有信号"和"筛选逻辑
        本身在跑但今天恰好没股票达标"是完全不同的两件事，必须让外层
        区分开，否则系统性bug会被伪装成"今天正常，只是没信号"，
        安安静静地跑完整个回测却一条有效数据都没产出。
        """
        xjo_slice = xjo_full[xjo_full.index <= as_of] if xjo_full is not None else None
        seen: dict[str, dict] = {}
        error_count = 0
        attempted_count = 0

        for tier in screener.TIERS:
            for ticker, df in history.items():
                if ticker in seen:
                    continue
                pit_df = self.__class__._slice(df, as_of)
                if len(pit_df) < self.cfg.min_history_days:
                    continue

                attempted_count += 1
                try:
                    tech = screener.build_tech_summary(pit_df, xjo_slice)
                except Exception as e:
                    error_count += 1
                    self.logger.debug(f"build_tech_summary异常 [{ticker}] {as_of.date()}: {e}")
                    continue

                try:
                    passed = screener._passes_tier(tech, tier)
                except Exception as e:
                    error_count += 1
                    self.logger.debug(f"_passes_tier异常 [{ticker}] {as_of.date()}: {e}")
                    continue

                if not passed:
                    continue

                tech["ticker"] = ticker
                tech["tier_level"] = tier["level"]
                tech["tier_label"] = tier["label"]
                try:
                    tech["persistence_score"] = screener._check_trend_persistence(
                        tech["_close"], tech["_adx_s"], tech["_pdi_s"], tech["_mdi_s"]
                    )
                except Exception:
                    tech["persistence_score"] = 0.0

                tech["catalyst"] = 0.0  # 历史回测无法重建公告驱动因子，诚实置0
                seen[ticker] = tech

        if not seen:
            return [], [], error_count, attempted_count

        raw_signals = list(seen.values())
        for s in raw_signals:
            s["composite_score"] = screener.calc_composite_score(s)
        raw_signals.sort(key=lambda x: x["composite_score"], reverse=True)
        raw_top10 = raw_signals[:10]

        filtered_pool = []
        for s in raw_top10:
            cap = self._market_cap_proxy(s["ticker"], s["price"])
            if cap < self.cfg.min_market_cap:
                continue
            s["market_cap_m"] = round(cap / 1e6, 1)
            s["confidence"] = screener.calc_confidence(s, s["tier_level"])
            filtered_pool.append(s)

        selected_top3 = filtered_pool[:3]
        return raw_top10, selected_top3, error_count, attempted_count

    @staticmethod
    def _slice(df: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        return df[df.index <= as_of]


# ════════════════════════════════════════════════════════════
# 跨日健康度层 —— 近似复刻 daily_analysis.py 的 ready/watch/caution/accumulating 判断
# ════════════════════════════════════════════════════════════

class DailyHealthEvaluator:
    """
    近似复刻 daily_analysis.py 的跨日健康度判断，改用纯日线OHLCV实现
    （真实系统读的是intraday_snapshots表，那是15分钟数据聚合出来的，
    只能回溯60天，没法做多年历史回测——这是绕开这个限制的近似方案）。

    没有直接 `import daily_analysis` 复用：该模块在导入时会执行
    `logging.basicConfig(handlers=[FileHandler("/home/ubuntu/logs/daily_analysis.log")])`，
    路径写死了，你VM上如果这个目录不存在或用户不是ubuntu，import会直接
    FileNotFoundError把整个回测进程带崩。所以这里是把四个核心因子函数
    （它们本身是纯计算，不做I/O）逐行照抄，而不是运行时依赖那个文件。
    如果你之后把daily_analysis.py的日志路径改成可配置，可以换成真正
    import复用，避免两份代码以后走岔。

    与真实版本的核心差异（只有一处，其余字段定义/阈值/组合逻辑逐行一致）：
      close_vol_ratio —— 真实定义是"当天最后一个15分钟时段的成交量，
      相对这个具体时段历史均值的比值"，本质是"尾盘有没有放量"。
      这里用"当天总成交量 / 20日平均总成交量"代替，丢失了"是不是尾盘
      放量"这个时间维度，只保留"这天有没有放量"这个更粗的信号。
      如果某天放量发生在开盘但尾盘已萎缩，这个代理会误判为recent_spike，
      真实系统不会——这是唯一的系统性偏差来源。
    """

    def __init__(self, cfg: BacktestConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger

    @staticmethod
    def _linreg_slope(series: pd.Series) -> float:
        """与daily_analysis.py的_linreg_slope()逐行一致。"""
        if len(series) < 3:
            return 0.0
        try:
            x = np.arange(len(series), dtype=float)
            y = series.values.astype(float)
            mask = ~np.isnan(y)
            if mask.sum() < 3:
                return 0.0
            slope = np.polyfit(x[mask], y[mask], 1)[0]
            mean = np.nanmean(y)
            return float(slope / mean) if abs(mean) > 1e-10 else 0.0
        except Exception:
            return 0.0

    def evaluate(self, pit_df: pd.DataFrame, as_of: pd.Timestamp) -> dict:
        """
        pit_df: 已经point-in-time截止到as_of的完整日线历史（用于rolling(20)
                有足够的热身数据，不是只传最近70天）
        返回health_status(ready/watch/caution/accumulating)及诊断字段。
        """
        cfg = self.cfg
        cutoff = as_of - pd.Timedelta(days=cfg.health_lookback_days)
        window = pit_df[(pit_df.index > cutoff) & (pit_df.index <= as_of)].copy()
        n_days = len(window)

        empty_result = {
            "health_status": "accumulating", "health_data_days": n_days,
            "health_signal_count": 0, "health_warn_count": 0,
        }
        if n_days < cfg.health_min_days_analysis:
            return empty_result

        try:
            day_high = window["High"].astype(float)
            day_low = window["Low"].astype(float)
            day_volume = window["Volume"].astype(float)
            close_proxy = window["Close"].astype(float)

            vol_ma20_full = pit_df["Volume"].astype(float).rolling(20).mean()
            vol_ratio_proxy = (window["Volume"].astype(float)
                                / vol_ma20_full.reindex(window.index)).fillna(0)

            signals: list[str] = []
            warnings: list[str] = []

            # ── 量能因子 ──
            vol_slope = round(self._linreg_slope(day_volume), 4)
            vols = day_volume.values
            shrink = 0
            for i in range(len(vols) - 1, 0, -1):
                if vols[i] < vols[i - 1]:
                    shrink += 1
                else:
                    break

            if vol_slope < cfg.health_vol_shrink_slope_max or shrink >= 3:
                signals.append("量能缩量整理")
            else:
                warnings.append("量能未见有效缩量")

            recent_ratio = vol_ratio_proxy.tail(3)
            spike_idx = recent_ratio[recent_ratio >= cfg.health_vol_spike_threshold].index
            if len(spike_idx) > 0:
                spike_date = spike_idx[-1]
                h, l, c = float(day_high.loc[spike_date]), float(day_low.loc[spike_date]), \
                          float(close_proxy.loc[spike_date])
                rng = h - l
                spike_direction = "none"
                if rng > 0:
                    pos = (c - l) / rng
                    spike_direction = "up" if pos >= 0.6 else "down"
                if spike_direction == "up":
                    signals.append("近期向上放量")
                elif spike_direction == "down":
                    warnings.append("近期向下放量(可能出货信号)")

            # ── 振幅因子 ──
            amplitude = ((day_high - day_low) / close_proxy).dropna()
            amp_slope = round(self._linreg_slope(amplitude), 4)
            if amp_slope < cfg.health_amplitude_shrink_slope:
                signals.append("振幅收窄整理")
            else:
                warnings.append("振幅未见收窄")

            # ── 价格结构因子 ──
            price_slope = round(self._linreg_slope(close_proxy), 4)
            if price_slope > 0:
                signals.append("价格重心上移")
            else:
                warnings.append("价格重心未见上移")

            day_range = day_high - day_low
            valid = day_range > 0
            if valid.any():
                cp = ((close_proxy - day_low) / day_range)[valid]
                above_pct = float((cp >= cfg.health_close_pos_min).sum() / len(cp))
                if above_pct >= 0.6:
                    signals.append("收盘持续偏强")

            if n_days >= cfg.health_min_days_exhaustion:
                recent_high = float(day_high.max())
                threshold = recent_high * 0.98
                tests = int(sum(
                    1 for h, c in zip(day_high.values, close_proxy.values)
                    if h >= threshold and c < threshold
                ))
                if 2 <= tests <= 8:
                    signals.append(f"压力位测试{tests}次")
                elif tests > 8:
                    warnings.append(f"压力位测试次数过多({tests}次)")

            # ── 动能因子 ──
            if n_days >= 5:
                base_vol = float(np.mean(vols[:-1])) if len(vols) > 1 else 0.0
                if (base_vol > 0 and all(v <= base_vol * 1.2 for v in vols[:-1])
                        and vols[-1] > base_vol * cfg.health_vol_spike_threshold):
                    signals.append("第一次放量")
                closes_arr = close_proxy.dropna().values
                if len(closes_arr) >= 4:
                    d2 = np.diff(np.diff(closes_arr))
                    if len(d2) >= 2 and d2[-1] > 0 and d2[-2] > 0:
                        signals.append("价格加速上涨")

            sig_count = len(signals)
            warn_count = len(warnings)

            if sig_count >= 4 and warn_count == 0:
                status = "ready"
            elif sig_count >= 3 and warn_count <= 1:
                status = "watch"
            elif any("向下放量" in w for w in warnings):
                status = "caution"
            else:
                status = "accumulating"

            return {
                "health_status": status, "health_data_days": n_days,
                "health_signal_count": sig_count, "health_warn_count": warn_count,
            }
        except Exception as e:
            self.logger.debug(f"健康度评估异常 as_of={as_of.date()}: {e}")
            return empty_result


# ════════════════════════════════════════════════════════════
# 出场模拟层 —— 完全复刻 screener.update_signal_outcomes() 的判定规则
# ════════════════════════════════════════════════════════════

class OutcomeSimulator:
    """
    入场价 = 信号当日收盘价（与save_signal_to_history()的entry_price定义一致，
    这是一个理论价位，当天收盘后其实还没法真正下单——这个理想化假设
    你线上signals_history同样存在，不是本回测独有的乐观假设）。

    止损 = entry - BT_STOP_ATR_MULT × ATR14
    止盈 = entry + BT_TARGET_ATR_MULT × ATR14
    出场：未来交易日中，先碰到低点<=止损 → LOSS(按止损价成交)；
          先碰到高点>=止盈 → WIN(按止盈价成交)；
          同一天两者都触发 → 按LOSS处理（与screener.py顺序一致，保守假设）；
          BT_TIMEOUT_DAYS个交易日内都没触发 → TIMEOUT(按最后一天收盘价成交)。
    """

    def __init__(self, cfg: BacktestConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger
        self.stop_mult = screener.BT_STOP_ATR_MULT
        self.target_mult = screener.BT_TARGET_ATR_MULT
        self.timeout_days = screener.BT_TIMEOUT_DAYS

    def simulate(self, ticker: str, df_full: pd.DataFrame, signal_date: pd.Timestamp,
                 entry_price: float, atr14_pct: float) -> Optional[dict]:
        atr = entry_price * atr14_pct / 100.0
        stop_loss = round(entry_price - self.stop_mult * atr, 4)
        take_profit = round(entry_price + self.target_mult * atr, 4)

        future = df_full[df_full.index > signal_date].iloc[: self.timeout_days]
        if future.empty:
            return None  # 右侧数据不足（信号太靠近历史数据末尾），视为PENDING，不计入统计

        outcome, out_date, out_price = None, None, None
        for dt, row in future.iterrows():
            low, high = float(row["Low"]), float(row["High"])
            if low <= stop_loss:
                outcome, out_date, out_price = "LOSS", dt, stop_loss
                break
            if high >= take_profit:
                outcome, out_date, out_price = "WIN", dt, take_profit
                break

        if outcome is None:
            if len(future) >= self.timeout_days:
                out_date = future.index[-1]
                out_price = float(future["Close"].iloc[-1])
                outcome = "TIMEOUT"
            else:
                return None  # 交易日数还没到齐，右侧删失，排除出统计（避免虚假提前结算）

        holding_days = len(future.loc[:out_date])
        max_gain_pct = round((float(future["High"].max()) / entry_price - 1) * 100, 2)
        max_loss_pct = round((float(future["Low"].min()) / entry_price - 1) * 100, 2)
        outcome_pct = round((out_price / entry_price - 1) * 100, 2)

        return {
            "outcome": outcome,
            "outcome_date": str(out_date.date()),
            "outcome_price": out_price,
            "outcome_pct": outcome_pct,
            "holding_days": holding_days,
            "max_gain_pct": max_gain_pct,
            "max_loss_pct": max_loss_pct,
            "stop_loss_atr": stop_loss,
            "take_profit_atr": take_profit,
        }


# ════════════════════════════════════════════════════════════
# 主引擎
# ════════════════════════════════════════════════════════════

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals_history_backtest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    signal_date     TEXT    NOT NULL,
    tier_level      TEXT,
    composite_score REAL,
    catalyst        REAL,
    rs_vs_xjo       REAL,
    adx14           REAL,
    vol_consistency INTEGER DEFAULT 0,
    price_pct_1y    REAL,
    dist_52w_hi_pct REAL,
    persistence_score REAL,
    confidence      REAL,
    market_cap_m    REAL,
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
    health_status       TEXT,
    health_data_days    INTEGER,
    health_signal_count INTEGER,
    health_warn_count   INTEGER,
    param_set       TEXT    DEFAULT 'baseline',
    run_timestamp   TEXT,
    UNIQUE(ticker, signal_date, param_set)
)
"""

# 用于判断现有表是不是"跟得上最新schema"的必需列集合。任何一次给
# signals_history_backtest加新字段（比如这次的health_*），都应该把
# 新列名加进这个集合——_init_db()靠这个集合决定要不要把旧表重命名备份。
REQUIRED_COLUMNS = {
    "param_set", "health_status", "health_data_days",
    "health_signal_count", "health_warn_count",
}

# 断点续跑进度表：记录"这一天已经完整跑过"，与signals_history_backtest
# 分开存储的原因——某一天完全没有候选信号是合法结果（T1-T4全部为空），
# 这种情况signals_history_backtest不会写入任何行，如果只靠这张表判断
# "这天有没有跑过"，会把"跑过但无信号"和"还没跑"搞混，导致断点续跑
# 时把已经跑过的空信号日重新跑一遍。
PROGRESS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS backtest_progress (
    run_key     TEXT NOT NULL,
    signal_date TEXT NOT NULL,
    PRIMARY KEY (run_key, signal_date)
)
"""


class BacktestEngine:
    def __init__(self, cfg: BacktestConfig):
        self.cfg = cfg
        self.logger = setup_logging(cfg.log_path)
        self.data_layer = DataLayer(self.logger)
        self.sig_gen = SignalGenerator(cfg, self.logger)
        self.sim = OutcomeSimulator(cfg, self.logger)
        self.health_eval = DailyHealthEvaluator(cfg, self.logger)

    def _init_db(self, conn: sqlite3.Connection):
        """
        建表前先检查现有表是否跟得上最新schema（REQUIRED_COLUMNS）——如果
        你在这些字段上线前已经跑过回测，旧表会被重命名备份而不是删除，
        绝不会丢数据，只是旧数据不会自动出现在新的统计口径里
        （想合并的话可以手动用sqlite3把备份表数据INSERT进新表）。
        """
        cur = conn.execute("PRAGMA table_info(signals_history_backtest)")
        existing_cols = {row[1] for row in cur.fetchall()}
        if existing_cols and not REQUIRED_COLUMNS.issubset(existing_cols):
            backup_name = f"signals_history_backtest_legacy_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.logger.warning(
                f"检测到旧版表结构（缺少字段: {REQUIRED_COLUMNS - existing_cols}），"
                f"已重命名备份为 {backup_name}，"
                f"数据不会丢失，如需合并请手动处理"
            )
            conn.execute(f"ALTER TABLE signals_history_backtest RENAME TO {backup_name}")
        conn.execute(SCHEMA_SQL)
        conn.execute(PROGRESS_SCHEMA_SQL)
        conn.commit()

    def _run_key(self) -> str:
        """
        断点续跑的识别key。加入param_set后，不同参数集下即使日期范围/universe
        完全一样，也会被当成互相独立的"另一次实验"，各自独立续跑、互不覆盖，
        这样才能支持"改参数重跑很多次、每次都能看到独立结果"的工作流。
        """
        raw = f"{self.cfg.start_date}|{self.cfg.end_date}|{self.cfg.universe_source}|" \
              f"{self.cfg.universe_file}|{self.cfg.min_market_cap}|{self.cfg.param_set}"
        return raw

    def run(self, tickers: list[str], max_minutes: Optional[float] = None):
        """
        对外入口：包一层Telegram通知 + 顶层崩溃兜底。真正的回测逻辑在
        _run_inner()里，这样任何未预料到的异常（不只是scan_day那种已知
        会失败的点）都能被这里的except捕获，推送报警而不是让nohup进程
        无声无息地死掉、你隔天才发现日志停在半夜某个时间点。
        """
        cfg = self.cfg
        start_msg = (f"🚀 回测启动\n参数集: {cfg.param_set}\n"
                     f"区间: {cfg.start_date} ~ {cfg.end_date}\n"
                     f"universe: {cfg.universe_source}({len(tickers)}只)\n"
                     f"时间预算: {max_minutes if max_minutes else '不限'}分钟")
        self.logger.info(start_msg.replace("\n", " | "))
        if cfg.push_telegram:
            send_telegram(start_msg, self.logger)

        try:
            summary = self._run_inner(tickers, max_minutes)
        except Exception as e:
            tb = traceback.format_exc()
            self.logger.critical(f"回测进程崩溃: {e}\n{tb}")
            if cfg.push_telegram:
                send_telegram(
                    f"🔴 回测崩溃 [{cfg.param_set}]\n"
                    f"错误: {e}\n\n"
                    f"traceback(截断):\n{tb[-1500:]}",
                    self.logger,
                )
            raise

        if cfg.push_telegram:
            send_telegram(summary, self.logger)

    def _run_inner(self, tickers: list[str], max_minutes: Optional[float]) -> str:
        self.logger.info(f"=== 回测启动 [{self.cfg.param_set}] {self.cfg.start_date} ~ "
                          f"{self.cfg.end_date} universe={self.cfg.universe_source}"
                          f"({len(tickers)}只) ===")

        # 下载起点往前多拉warmup_calendar_days天，保证信号生成的第一天
        # MA200/52周高点等长窗口指标已经"热身"完毕，不是从零开始累积
        download_start = str((pd.Timestamp(self.cfg.start_date)
                               - pd.Timedelta(days=self.cfg.warmup_calendar_days)).date())
        self.logger.info(f"数据下载起点(含热身缓冲): {download_start}（正式信号仍从"
                          f"{self.cfg.start_date}开始，缓冲期本身不产出信号）")

        history: dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = self.data_layer.fetch(t, download_start, self.cfg.end_date)
            if df is not None and len(df) >= self.cfg.min_history_days:
                history[t] = df

        self.logger.info(f"有效历史数据：{len(history)}/{len(tickers)} 只")
        if not history:
            msg = f"🔴 回测终止 [{self.cfg.param_set}]：universe拉不到任何有效历史数据"
            self.logger.error(msg)
            return msg

        xjo_full = self.data_layer.fetch(self.cfg.benchmark_ticker,
                                          download_start, self.cfg.end_date)
        xjo_series = xjo_full["Close"].squeeze() if xjo_full is not None else None

        trading_days = sorted(set().union(*[df.index for df in history.values()]))
        trading_days = [d for d in trading_days if d >= pd.Timestamp(self.cfg.start_date)]
        self.logger.info(f"回测交易日数（总计，已扣除热身期）：{len(trading_days)}")

        conn = sqlite3.connect(self.cfg.db_path)
        self._init_db(conn)
        run_ts = datetime.now().isoformat()
        run_key = self._run_key()

        # ── 断点续跑：跳过本次run_key下已经完整处理过的交易日 ──────────
        done_rows = conn.execute(
            "SELECT signal_date FROM backtest_progress WHERE run_key = ?", (run_key,)
        ).fetchall()
        done_days = {r[0] for r in done_rows}
        remaining_days = [d for d in trading_days if str(d.date()) not in done_days]

        if done_days:
            self.logger.info(f"检测到断点：已完成 {len(done_days)} 天，"
                              f"本次继续剩余 {len(remaining_days)} 天")
        if not remaining_days:
            msg = f"✅ 回测 [{self.cfg.param_set}] 全部交易日已在此前运行中完成，无需再跑"
            self.logger.info(msg)
            conn.close()
            return msg

        total_written, total_selected = 0, 0
        processed_count = 0  # 本次运行里真正跑完的天数，独立于循环变量i，
                              # 避免break/continue路径下的off-by-one混淆
        consecutive_errors = 0  # 熔断计数器：连续失败达到阈值就停止，
                                 # 避免系统性bug时把整晚算力浪费在无效重复报错上
        circuit_broken = False
        start_time = time.time()

        for i, day in enumerate(remaining_days):
            if max_minutes is not None:
                elapsed_min = (time.time() - start_time) / 60
                if elapsed_min >= max_minutes:
                    left = len(remaining_days) - processed_count
                    self.logger.info(
                        f"达到时间预算({max_minutes}分钟)，本次运行提前结束。"
                        f"已处理{processed_count}/{len(remaining_days)}天，剩余{left}天，"
                        f"下次用相同参数重新运行会自动从这里继续（断点续跑）。"
                    )
                    break

            if processed_count % 10 == 0 and processed_count > 0:
                elapsed = time.time() - start_time
                rate = elapsed / processed_count
                eta_min = rate * (len(remaining_days) - processed_count) / 60
                self.logger.info(
                    f"进度 {processed_count}/{len(remaining_days)} ({day.date()}) "
                    f"已用{elapsed/60:.1f}分钟 预计还需{eta_min:.1f}分钟 "
                    f"累计写入{total_written}条 已选出{total_selected}笔Top3信号"
                )

            try:
                raw_top10, selected, error_count, attempted_count = self.sig_gen.scan_day(
                    day, history, xjo_series
                )
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(
                    f"scan_day整体异常 {day.date()}（连续失败{consecutive_errors}/"
                    f"{self.cfg.max_consecutive_errors}）: {e}"
                )
                if consecutive_errors >= self.cfg.max_consecutive_errors:
                    alert = (
                        f"🔴 回测熔断 [{self.cfg.param_set}]\n"
                        f"连续{consecutive_errors}天scan_day整体异常，最新错误: {e}\n"
                        f"大概率是系统性问题（参数错误/代码bug），已提前停止，"
                        f"没有标记为完成的交易日下次会自动重试，请先检查backtest.log"
                    )
                    self.logger.critical(alert)
                    if self.cfg.push_telegram:
                        send_telegram(alert, self.logger)
                    circuit_broken = True
                    break
                continue

            # 关键：单只股票的build_tech_summary/_passes_tier异常已经在scan_day
            # 内部被吞掉（避免一只股票拖累整天），但如果"今天尝试评估的股票
            # 全部出错"（attempted_count>0且error_count==attempted_count），
            # 这不是"今天正常但没有信号"，而是系统性故障的强烈信号——必须在这里
            # 单独识别出来，否则会被"if not raw_top10"那条路径误判为
            # 合法的"今天没有信号"，悄悄标记完成，让熔断机制形同虚设。
            if attempted_count > 0 and error_count == attempted_count:
                consecutive_errors += 1
                self.logger.error(
                    f"scan_day当天全部{attempted_count}次评估均失败 {day.date()}"
                    f"（连续失败{consecutive_errors}/{self.cfg.max_consecutive_errors}），"
                    f"这天不标记为完成，等修复后重试"
                )
                if consecutive_errors >= self.cfg.max_consecutive_errors:
                    alert = (
                        f"🔴 回测熔断 [{self.cfg.param_set}]\n"
                        f"连续{consecutive_errors}天所有股票的信号计算全部失败\n"
                        f"大概率是系统性问题（参数错误/代码bug），已提前停止，"
                        f"没有标记为完成的交易日下次会自动重试，请先检查backtest.log"
                    )
                    self.logger.critical(alert)
                    if self.cfg.push_telegram:
                        send_telegram(alert, self.logger)
                    circuit_broken = True
                    break
                continue

            consecutive_errors = 0  # 这天有实质性进展（哪怕没有信号，只要不是全员出错），清零熔断计数器

            if not raw_top10:
                conn.execute("INSERT OR IGNORE INTO backtest_progress VALUES (?, ?)",
                             (run_key, str(day.date())))
                conn.commit()
                processed_count += 1
                continue

            selected_tickers = {s["ticker"] for s in selected}
            rows = []
            for s in raw_top10:
                ticker = s["ticker"]
                entry_price = float(s["price"])
                atr14_pct = float(s.get("atr14_pct", 2.0))

                try:
                    pit_for_health = history[ticker][history[ticker].index <= day]
                    health = self.health_eval.evaluate(pit_for_health, day)
                except Exception as e:
                    self.logger.debug(f"健康度评估异常 [{ticker}] {day.date()}: {e}")
                    health = {"health_status": None, "health_data_days": None,
                              "health_signal_count": None, "health_warn_count": None}

                outcome_result = None
                try:
                    outcome_result = self.sim.simulate(
                        ticker, history[ticker], day, entry_price, atr14_pct
                    )
                except Exception as e:
                    self.logger.debug(f"simulate异常 [{ticker}] {day.date()}: {e}")

                if outcome_result is None:
                    # 右侧数据不足，无法评估结果，仍然把信号本身记下来（outcome=PENDING），
                    # 保持和真实signals_history一样"PENDING"的语义，供以后数据补齐后重跑
                    row = (
                        ticker, str(day.date()), s.get("tier_level"), s.get("composite_score"),
                        s.get("catalyst", 0.0), s.get("rs_vs_xjo"), s.get("adx14"),
                        1 if s.get("vol_consistency") else 0, s.get("price_pct_1y"),
                        s.get("dist_52w_hi_pct"), s.get("persistence_score"),
                        s.get("confidence"), s.get("market_cap_m"),
                        entry_price, None, None,
                        1 if ticker in selected_tickers else 0,
                        "PENDING", None, None, None, None, None, None,
                        health.get("health_status"), health.get("health_data_days"),
                        health.get("health_signal_count"), health.get("health_warn_count"),
                        self.cfg.param_set, run_ts,
                    )
                else:
                    row = (
                        ticker, str(day.date()), s.get("tier_level"), s.get("composite_score"),
                        s.get("catalyst", 0.0), s.get("rs_vs_xjo"), s.get("adx14"),
                        1 if s.get("vol_consistency") else 0, s.get("price_pct_1y"),
                        s.get("dist_52w_hi_pct"), s.get("persistence_score"),
                        s.get("confidence"), s.get("market_cap_m"),
                        entry_price, outcome_result["stop_loss_atr"], outcome_result["take_profit_atr"],
                        1 if ticker in selected_tickers else 0,
                        outcome_result["outcome"], outcome_result["outcome_date"],
                        outcome_result["outcome_price"], outcome_result["outcome_pct"],
                        outcome_result["holding_days"], outcome_result["max_gain_pct"],
                        outcome_result["max_loss_pct"],
                        health.get("health_status"), health.get("health_data_days"),
                        health.get("health_signal_count"), health.get("health_warn_count"),
                        self.cfg.param_set, run_ts,
                    )
                rows.append(row)

            if rows:
                conn.executemany(f"""
                    INSERT OR IGNORE INTO signals_history_backtest (
                        ticker, signal_date, tier_level, composite_score, catalyst,
                        rs_vs_xjo, adx14, vol_consistency, price_pct_1y, dist_52w_hi_pct,
                        persistence_score, confidence, market_cap_m,
                        entry_price, stop_loss_atr, take_profit_atr, is_selected,
                        outcome, outcome_date, outcome_price, outcome_pct,
                        holding_days, max_gain_pct, max_loss_pct,
                        health_status, health_data_days, health_signal_count, health_warn_count,
                        param_set, run_timestamp
                    ) VALUES ({",".join(["?"] * 30)})
                """, rows)
                total_written += len(rows)
                total_selected += len(selected)

            # 标记这一天已完整处理（无论有没有信号），供下次断点续跑判断
            conn.execute("INSERT OR IGNORE INTO backtest_progress VALUES (?, ?)",
                         (run_key, str(day.date())))
            conn.commit()
            processed_count += 1

        total_done = len(done_days) + processed_count
        left = len(trading_days) - total_done
        conn.close()

        status_line = "🛑 因熔断提前停止" if circuit_broken else (
            "✅ 全部完成，可以看统计报告了" if left <= 0 else "⏸ 已按时间预算暂停"
        )
        summary = (
            f"{status_line} [参数集: {self.cfg.param_set}]\n"
            f"本次新处理: {processed_count}天\n"
            f"写入候选记录: {total_written}条（其中Top3精选信号{total_selected}笔）\n"
            f"累计总进度: {total_done}/{len(trading_days)}天"
            + (f"，剩余{left}天，下次用相同参数重跑会自动续上" if left > 0 else "")
        )
        self.logger.info("=== " + summary.replace("\n", " | ") + " ===")
        return summary


# ════════════════════════════════════════════════════════════
# 参数覆盖系统 —— 让"改参数重跑"不需要碰screener.py这个生产文件
# ════════════════════════════════════════════════════════════
#
# 设计动机：SCORE_WEIGHTS/TIERS/TREND_SCORE_THRESHOLD这些参数硬编码在
# screener.py里，你要测试新参数组合，理论上得去改这个正在生产环境跑的
# 文件——风险高（改错一个逗号线上就崩），也没法保留每次实验的记录做对比。
#
# 这里用"猴子补丁"（运行时给screener模块的属性重新赋值）解决：
#   - 完全不碰screener.py这个文件本身，磁盘上的文件一个字节都不会变
#   - 只在backtest_engine.py这个独立进程的内存里生效，不影响任何正在
#     跑的screener.py / daily_analysis.py / intraday_monitor.py 生产进程
#   - screener.py里所有函数（_passes_tier / calc_composite_score等）
#     引用这些常量时都是"调用时从模块里现查"，不是"定义时就写死"，
#     所以运行时改了之后，后续调用会自动用上新值——这是Python的正常
#     行为，不是什么特殊技巧
#
# 使用方式（对应你说的"改参数→重跑→出结果→再改"这个循环）：
#   1. 先导出一份当前默认参数模板：
#        python3 backtest_engine.py --export-params baseline_params.json
#   2. 复制一份改名（比如 exp1_higher_adx.json），只改你想测的字段
#   3. 跑：
#        python3 backtest_engine.py --start ... --end ... \
#            --params-file exp1_higher_adx.json --param-set-name exp1_higher_adx
#   4. 反复第2-3步，每次换个--param-set-name，结果都存在同一个
#      backtest_results.db里，互不覆盖
#   5. 跑完多轮后：
#        python3 backtest_engine.py --stats-only --leaderboard
#      一次性看到所有实验的胜率/盈亏对比排行榜

def export_default_params(path: str, cfg: BacktestConfig, logger: logging.Logger) -> None:
    """导出screener.py当前的默认参数 + 健康度层默认参数，作为JSON模板供你复制修改。"""
    payload = {
        "SCORE_WEIGHTS": dict(screener.SCORE_WEIGHTS),
        "TIER_BONUS": dict(screener.TIER_BONUS),
        "TREND_SCORE_THRESHOLD": dict(screener.TREND_SCORE_THRESHOLD),
        "TIERS": {
            t["level"]: {k: v for k, v in t.items() if k not in ("level", "label", "note")}
            for t in screener.TIERS
        },
        "BT_STOP_ATR_MULT": screener.BT_STOP_ATR_MULT,
        "BT_TARGET_ATR_MULT": screener.BT_TARGET_ATR_MULT,
        "BT_TIMEOUT_DAYS": screener.BT_TIMEOUT_DAYS,
        "DAILY_HEALTH": {
            "VOL_SPIKE_THRESHOLD": cfg.health_vol_spike_threshold,
            "VOL_SHRINK_SLOPE_MAX": cfg.health_vol_shrink_slope_max,
            "AMPLITUDE_SHRINK_SLOPE": cfg.health_amplitude_shrink_slope,
            "CLOSE_POS_MIN": cfg.health_close_pos_min,
            "MIN_DAYS_FOR_ANALYSIS": cfg.health_min_days_analysis,
            "MIN_DAYS_FOR_EXHAUSTION": cfg.health_min_days_exhaustion,
            "LOOKBACK_DAYS": cfg.health_lookback_days,
        },
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"默认参数模板已导出: {path}")
    print(f"默认参数模板已导出到 {path}，复制一份改名后编辑你想测试的字段即可")


def apply_param_overrides(overrides: dict, cfg: BacktestConfig, logger: logging.Logger) -> None:
    """
    把JSON里出现的字段，以"部分覆盖"（不是整体替换）的方式打到screener模块
    和cfg对象上。没在JSON里出现的字段保持原始默认值不变。

    必须在构建BacktestEngine/SignalGenerator/OutcomeSimulator/
    DailyHealthEvaluator之前调用——OutcomeSimulator.__init__会把
    BT_STOP_ATR_MULT等缓存成实例属性，构建完之后再改screener模块的值
    不会生效。main()里已经保证了调用顺序。
    """
    if "SCORE_WEIGHTS" in overrides:
        screener.SCORE_WEIGHTS = {**screener.SCORE_WEIGHTS, **overrides["SCORE_WEIGHTS"]}
        logger.info(f"覆盖 SCORE_WEIGHTS -> {screener.SCORE_WEIGHTS}")

    if "TIER_BONUS" in overrides:
        screener.TIER_BONUS = {**screener.TIER_BONUS, **overrides["TIER_BONUS"]}
        logger.info(f"覆盖 TIER_BONUS -> {screener.TIER_BONUS}")

    if "TREND_SCORE_THRESHOLD" in overrides:
        screener.TREND_SCORE_THRESHOLD = {**screener.TREND_SCORE_THRESHOLD,
                                          **overrides["TREND_SCORE_THRESHOLD"]}
        logger.info(f"覆盖 TREND_SCORE_THRESHOLD -> {screener.TREND_SCORE_THRESHOLD}")

    if "TIERS" in overrides:
        new_tiers = []
        for tier in screener.TIERS:
            patch = overrides["TIERS"].get(tier["level"], {})
            new_tiers.append({**tier, **patch})
        screener.TIERS = new_tiers
        logger.info(f"覆盖 TIERS -> " +
                    "; ".join(f"{t['level']}:{overrides['TIERS'].get(t['level'], {})}"
                             for t in new_tiers if t["level"] in overrides["TIERS"]))

    for scalar_key in ("BT_STOP_ATR_MULT", "BT_TARGET_ATR_MULT", "BT_TIMEOUT_DAYS"):
        if scalar_key in overrides:
            setattr(screener, scalar_key, overrides[scalar_key])
            logger.info(f"覆盖 {scalar_key} -> {overrides[scalar_key]}")

    if "DAILY_HEALTH" in overrides:
        health_field_map = {
            "VOL_SPIKE_THRESHOLD": "health_vol_spike_threshold",
            "VOL_SHRINK_SLOPE_MAX": "health_vol_shrink_slope_max",
            "AMPLITUDE_SHRINK_SLOPE": "health_amplitude_shrink_slope",
            "CLOSE_POS_MIN": "health_close_pos_min",
            "MIN_DAYS_FOR_ANALYSIS": "health_min_days_analysis",
            "MIN_DAYS_FOR_EXHAUSTION": "health_min_days_exhaustion",
            "LOOKBACK_DAYS": "health_lookback_days",
        }
        for k, v in overrides["DAILY_HEALTH"].items():
            field = health_field_map.get(k)
            if field is None:
                logger.warning(f"DAILY_HEALTH里的未知字段 {k}，忽略")
                continue
            setattr(cfg, field, v)
            logger.info(f"覆盖 DAILY_HEALTH.{k} -> {v}")


def resolve_param_set_name(params_file: str, explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    if params_file and os.path.exists(params_file):
        with open(params_file, encoding="utf-8") as f:
            content = f.read()
        h = hashlib.md5(content.encode("utf-8")).hexdigest()[:8]
        return f"auto-{h}"
    return "baseline"


# ════════════════════════════════════════════════════════════
# 实验档案 —— 把每次实际用的参数内容+对应git commit存下来，
# 不用靠记忆或者翻git log去追溯"这个param_set当时到底测的什么"
# ════════════════════════════════════════════════════════════

EXPERIMENT_METADATA_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS experiment_metadata (
    param_set     TEXT PRIMARY KEY,
    params_json   TEXT,
    params_file   TEXT,
    git_commit    TEXT,
    first_seen_at TEXT
)
"""


def _get_git_commit(file_path: str) -> Optional[str]:
    """
    尽力而为获取参数文件所在git仓库的当前commit短hash。
    拿不到（不是git仓库/没装git/其他任何原因）就返回None，
    绝不能因为这个附加功能失败就影响回测主流程。
    """
    if not file_path or not os.path.exists(file_path):
        return None
    try:
        repo_dir = os.path.dirname(os.path.abspath(file_path))
        result = subprocess.run(
            ["git", "-C", repo_dir, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            commit = result.stdout.strip()
            # 顺便看一下有没有未commit的本地改动，这种情况下commit hash
            # 不能完全代表实际用的内容，需要提醒
            dirty = subprocess.run(
                ["git", "-C", repo_dir, "status", "--porcelain"],
                capture_output=True, text=True, timeout=5,
            )
            if dirty.returncode == 0 and dirty.stdout.strip():
                return f"{commit}(有未提交的本地改动，commit不完全代表实际内容)"
            return commit
    except Exception:
        pass
    return None


def record_experiment_metadata(db_path: str, param_set: str, params_file: str,
                               overrides: dict, logger: logging.Logger) -> None:
    """
    把这次实验实际用的参数内容 + 对应git commit记录到db里（同一个param_set
    只记第一次，后面重跑同样的param_set不会覆盖——因为按定义，
    同样的param_set名字理应对应同样的参数内容）。
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(EXPERIMENT_METADATA_SCHEMA_SQL)
        git_commit = _get_git_commit(params_file) if params_file else None
        params_json = (json.dumps(overrides, ensure_ascii=False, indent=2)
                       if overrides else "(baseline，未传--params-file)")
        conn.execute(
            "INSERT OR IGNORE INTO experiment_metadata "
            "(param_set, params_json, params_file, git_commit, first_seen_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (param_set, params_json, params_file or "", git_commit or "",
             datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()
        logger.info(f"实验档案已记录: param_set={param_set} git_commit={git_commit or 'N/A'}")
    except Exception as e:
        logger.warning(f"记录实验档案失败（不影响回测本身继续运行）: {e}")


def show_param_set(db_path: str, param_set: str) -> None:
    """查询某个param_set当时实际用的参数内容，供追溯用。"""
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT params_json, params_file, git_commit, first_seen_at "
            "FROM experiment_metadata WHERE param_set = ?",
            (param_set,),
        ).fetchone()
    except Exception as e:
        print(f"查询失败（可能还没有实验档案表）: {e}")
        conn.close()
        return
    conn.close()

    if not row:
        print(f"没有找到 param_set={param_set} 的实验档案记录")
        return

    params_json, params_file, git_commit, first_seen_at = row
    print(f"参数集      : {param_set}")
    print(f"首次运行时间: {first_seen_at}")
    print(f"参数文件路径: {params_file or '(baseline，未使用参数文件)'}")
    print(f"git commit  : {git_commit or '(未知/当时不在git仓库里)'}")
    print(f"实际参数内容:\n{params_json}")


# ════════════════════════════════════════════════════════════
# 统计报告层
# ════════════════════════════════════════════════════════════

class StatsReporter:
    def __init__(self, cfg: BacktestConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger

    def leaderboard(self, push_telegram: bool = False):
        """
        一次性列出db里所有跑过的参数集实验，按胜率排序——
        这是支撑"反复改参数、每次都想知道谁更好"这个工作流的核心视图。
        """
        buffer: list[str] = []

        def emit(text: str) -> None:
            print(text)
            buffer.append(text)

        if not os.path.exists(self.cfg.db_path):
            emit("回测数据库还不存在，先跑一次backtest_engine.py")
            if push_telegram:
                send_telegram("\n".join(buffer), self.logger)
            return
        conn = sqlite3.connect(self.cfg.db_path)
        conn.execute(EXPERIMENT_METADATA_SCHEMA_SQL)  # 确保表存在，旧db也不会查询报错
        try:
            df = pd.read_sql_query("""
                SELECT t.param_set,
                       COUNT(*) AS n,
                       SUM(CASE WHEN t.outcome='WIN' THEN 1 ELSE 0 END) AS wins,
                       AVG(t.outcome_pct) AS avg_pct,
                       MIN(t.signal_date) AS date_from,
                       MAX(t.signal_date) AS date_to,
                       MAX(m.git_commit) AS git_commit
                FROM signals_history_backtest t
                LEFT JOIN experiment_metadata m ON t.param_set = m.param_set
                WHERE t.outcome != 'PENDING' AND t.is_selected = 1
                GROUP BY t.param_set
                ORDER BY (wins * 1.0 / n) DESC
            """, conn)
        except Exception as e:
            emit(f"排行榜查询失败（可能是旧表结构还没跑过新参数系统）: {e}")
            conn.close()
            if push_telegram:
                send_telegram("\n".join(buffer), self.logger)
            return
        conn.close()

        if df.empty:
            emit("暂无已完成的实验记录（outcome全是PENDING，或者还没跑过任何数据）")
            if push_telegram:
                send_telegram("\n".join(buffer), self.logger)
            return

        emit("\n" + "=" * 70)
        emit("参数实验排行榜（按Top3精选信号胜率排序，只统计有结果的交易）")
        emit("=" * 70)
        for _, row in df.iterrows():
            wr = row["wins"] / row["n"] if row["n"] else 0
            commit_note = f"  commit:{row['git_commit']}" if row.get("git_commit") else ""
            emit(f"  {row['param_set']:<24s} 样本{int(row['n']):>4d}笔  "
                 f"胜率{wr:>6.1%}  平均单笔{row['avg_pct']:>+6.2f}%  "
                 f"覆盖{row['date_from']}~{row['date_to']}{commit_note}")
        emit("=" * 70)
        emit("样本量差距较大的实验之间直接比胜率会有误导性，"
             "建议同时看样本数，样本差太多的先别下结论")
        emit("用 --show-param-set <名字> 可以查看某个实验当时实际用的完整参数内容")

        if push_telegram:
            send_telegram("\n".join(buffer), self.logger)

    def _bootstrap_ci(self, wins: np.ndarray, n_iter: int = 2000) -> tuple[float, float]:
        if len(wins) == 0:
            return (0.0, 0.0)
        rng = np.random.default_rng(42)
        rates = [rng.choice(wins, size=len(wins), replace=True).mean() for _ in range(n_iter)]
        return float(np.percentile(rates, 5)), float(np.percentile(rates, 95))

    def _load_backtest_df(self, only_selected: bool, tier: Optional[str]) -> pd.DataFrame:
        if not os.path.exists(self.cfg.db_path):
            return pd.DataFrame()
        conn = sqlite3.connect(self.cfg.db_path)
        query = ("SELECT *, 'backtest' AS source FROM signals_history_backtest "
                 "WHERE outcome != 'PENDING' AND param_set = ?")
        params = [self.cfg.param_set]
        if only_selected:
            query += " AND is_selected = 1"
        if tier:
            query += " AND tier_level = ?"
            params.append(tier)
        try:
            df = pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            self.logger.warning(f"读取回测结果失败: {e}")
            df = pd.DataFrame()
        conn.close()
        return df

    def _load_live_df(self, live_db: str, only_selected: bool, tier: Optional[str]) -> pd.DataFrame:
        if not os.path.exists(live_db):
            self.logger.warning(f"线上数据库不存在: {live_db}，跳过合并")
            return pd.DataFrame()
        conn = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True)
        query = ("SELECT ticker, signal_date, tier_level, composite_score, catalyst, "
                  "rs_vs_xjo, adx14, vol_consistency, price_pct_1y, dist_52w_hi_pct, "
                  "entry_price, stop_loss_atr, take_profit_atr, is_selected, "
                  "outcome, outcome_date, outcome_price, outcome_pct, holding_days, "
                  "max_gain_pct, max_loss_pct, 'live' AS source "
                  "FROM signals_history WHERE outcome != 'PENDING'")
        params: list = []
        if only_selected:
            query += " AND is_selected = 1"
        if tier:
            query += " AND tier_level = ?"
            params.append(tier)
        try:
            df = pd.read_sql_query(query, conn, params=params)
        finally:
            conn.close()
        return df

    def _benchmark_return(self) -> Optional[float]:
        try:
            df = yf.download(self.cfg.benchmark_ticker, start=self.cfg.start_date,
                              end=self.cfg.end_date, auto_adjust=True, progress=False)
            if df is None or df.empty:
                return None
            c = df["Close"].squeeze()
            return float(c.iloc[-1] / c.iloc[0] - 1)
        except Exception as e:
            self.logger.warning(f"基准指数获取失败: {e}")
            return None

    def _emit(self, text: str) -> None:
        """print的同时收进buffer，供report()结束时整体推送Telegram用。"""
        print(text)
        self._buffer.append(text)

    def report(self, only_selected: bool = True, tier: Optional[str] = None,
               merge_live: bool = False, live_db: str = "",
               health_status: Optional[str] = None, push_telegram: bool = False,
               export_csv: Optional[str] = None):
        self._buffer: list[str] = []

        bt_df = self._load_backtest_df(only_selected, tier)
        frames = [bt_df]
        if merge_live:
            live_df = self._load_live_df(live_db or os.path.join(ASX_DIR, "announcements.db"),
                                          only_selected, tier)
            if health_status and not live_df.empty:
                self.logger.warning(
                    "注意：线上signals_history表没有health_status字段（真实系统里"
                    "健康度状态存在watchlist_db，不在announcements.db），"
                    "--merge-live + --health-status同时使用时，线上数据会被health_status"
                    "过滤条件排除（NaN不匹配任何具体状态），只剩本地回测数据参与统计"
                )
            frames.append(live_df)

        combined = pd.concat(frames, ignore_index=True) if any(len(f) for f in frames) else pd.DataFrame()

        if health_status:
            combined = combined[combined["health_status"] == health_status]

        if combined.empty:
            self._emit("无可用交易记录（outcome全部为PENDING，或数据库为空，或health_status过滤后为空）")
            if push_telegram:
                send_telegram("\n".join(self._buffer), self.logger)
            return

        source_counts = combined["source"].value_counts().to_dict() if "source" in combined else {}
        wins = (combined["outcome"] == "WIN").astype(int).values
        pcts = combined["outcome_pct"].astype(float).values

        win_rate = wins.mean()
        ci_lo, ci_hi = self._bootstrap_ci(wins)
        avg_win = pcts[pcts > 0].mean() if (pcts > 0).any() else 0.0
        avg_loss = pcts[pcts < 0].mean() if (pcts < 0).any() else 0.0
        gross_profit = pcts[pcts > 0].sum()
        gross_loss = abs(pcts[pcts < 0].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        cum = np.cumsum(pcts)
        running_max = np.maximum.accumulate(cum)
        max_dd = float((cum - running_max).min())

        bench = self._benchmark_return()

        health_note = f"（仅健康度={health_status}）" if health_status else ""
        tier_note = f"（仅{tier}层级）" if tier else "（全部T1-T4层级）"
        scope_note = "仅Top3精选信号" if only_selected else "T1-T4全部候选（含未入选）"

        lines = [
            "\n" + "=" * 58,
            f"回测统计报告 [参数集: {self.cfg.param_set}] {tier_note}{health_note} — {scope_note}",
            "=" * 58,
            f"样本来源       : {source_counts}",
            f"交易笔数       : {len(combined)}",
            f"胜率           : {win_rate:.1%}  (90% CI: {ci_lo:.1%} ~ {ci_hi:.1%})",
            f"平均盈利/平均亏损: {avg_win:+.2f}% / {avg_loss:+.2f}%",
            f"盈亏比 Profit Factor: {profit_factor:.2f}",
            f"累计收益率(单笔等权简单累加，非复利): {cum[-1] if len(cum) else 0:+.2f}%",
            f"最大回撤(等权累加口径)             : {max_dd:+.2f}%",
            f"ASX200同期涨跌幅                   : "
            f"{f'{bench:+.1%}' if bench is not None else '获取失败'}",
            "-" * 58,
            "⚠️ 局限提醒（务必结合解读）:",
            "  - 市值用「当前股数×历史当日股价」做代理，仍非完美point-in-time市值",
            "    （公司历史上若做过大额配股/回购，会有残余误差）",
            "  - catalyst固定为0，本结果只反映趋势/技术面逻辑，不含公告驱动信号",
            "  - 止盈止损假设完美成交，不含跳空穿仓风险，实际胜率可能更保守",
            "  - TREND_SCORE_THRESHOLD由近期数据校准，套用早期历史存在轻微前视偏差",
            "  - health_status是daily_analysis.py的近似复刻（日线数据版），"
            "唯一的系统性偏差在于用「当日总量比」代替「尾盘时段量比」",
            "=" * 58,
        ]
        report_text = "\n".join(lines)
        self._emit(report_text)
        self.logger.info(report_text)

        if "outcome" in combined.columns:
            self._emit("\n【按出场原因拆解】WIN=触发止盈 / LOSS=触发止损 / TIMEOUT=到期强平（都没触发）")
            for oc in ["WIN", "LOSS", "TIMEOUT"]:
                g = combined[combined["outcome"] == oc]
                if len(g) == 0:
                    continue
                avg_pct = g["outcome_pct"].astype(float).mean()
                self._emit(f"  {oc:<8s}: 样本{len(g)}笔  占比{len(g)/len(combined):.1%}  平均单笔收益{avg_pct:+.2f}%")
            self._emit("  → 「胜率」只统计WIN这一类；TIMEOUT里如果正收益占多数，"
                       "会拉高盈亏比但不会拉高胜率数字，两个指标不矛盾，只是统计口径不同")

        if "tier_level" in combined.columns and combined["tier_level"].nunique() > 1:
            self._emit("\n【分层级胜率对比（合并样本）】")
            for lv, g in combined.groupby("tier_level"):
                w = (g["outcome"] == "WIN").mean()
                avg_pct = g["outcome_pct"].astype(float).mean()
                self._emit(f"  {lv}: 样本{len(g)}笔  胜率{w:.1%}  平均单笔收益{avg_pct:+.2f}%")

        if "health_status" in combined.columns and combined["health_status"].notna().any():
            self._emit("\n【跨日健康度分层胜率对比】—— 直接回答"
                       "「daily_analysis.py这层过滤到底有没有用」")
            for hs, g in combined.dropna(subset=["health_status"]).groupby("health_status"):
                w = (g["outcome"] == "WIN").mean()
                avg_pct = g["outcome_pct"].astype(float).mean()
                self._emit(f"  {hs:<12s}: 样本{len(g)}笔  胜率{w:.1%}  平均单笔收益{avg_pct:+.2f}%")
            self._emit("  → 如果ready组明显比其他组胜率高，说明健康度过滤确实有增量价值，"
                       "值得在intraday_monitor.py里继续坚持这道门槛；如果差不多甚至更低，"
                       "说明这层过滤没有实际筛选力，可以考虑简化掉")

        self._analyze_score_predictiveness(combined)

        if export_csv:
            try:
                # 导出前把不适合放CSV的内部对象列去掉（如果有的话），
                # 保留人类/Claude都能直接用pandas读的干净表格
                export_df = combined.copy()
                export_df.to_csv(export_csv, index=False, encoding="utf-8-sig")
                self.logger.info(f"CSV已导出: {export_csv}（{len(export_df)}行）")
                self._emit(f"\n📄 CSV已导出: {export_csv}（{len(export_df)}行，"
                           f"包含每笔交易的完整字段，适合做进一步定量分析）")
                if push_telegram:
                    send_telegram_document(
                        export_csv,
                        caption=f"{self.cfg.param_set} 回测明细（{len(export_df)}笔交易）",
                        logger=self.logger,
                    )
            except Exception as e:
                self.logger.error(f"CSV导出失败: {e}")
                self._emit(f"⚠️ CSV导出失败: {e}")

        if push_telegram:
            send_telegram("\n".join(self._buffer), self.logger)

    def _analyze_score_predictiveness(self, combined: pd.DataFrame, n_buckets: int = 4):
        """
        回答"评分排得高是不是真的赢面更大"——这是判断调参方向的核心依据。

        做法：按composite_score把全部交易分成n_buckets组（默认4等分），
        分别看每组的胜率和平均单笔收益。

        怎么解读：
          - 如果分数越高的组，胜率/平均收益确实越高 → 说明composite_score
            本身有效，可以考虑抬高入选门槛（牺牲信号数量换胜率），
            或者干脆把Top3改成Top1/Top2，只吃最高分那一档
          - 如果各组胜率几乎没差别，甚至和分数排序倒挂 → 说明当前的
            SCORE_WEIGHTS权重分配（trend_strength/persistence/catalyst/
            price_pct_1y）不是真正驱动胜负的因素，硬拉高门槛不会提升
            胜率，需要回头看看权重设计或者_passes_tier()里的硬性条件
            是不是筛掉了错误的股票

        样本量不足20笔时不做分组（分组后每组可能只有个位数样本，
        结论没有意义，容易把噪音当规律）。
        """
        if len(combined) < 20 or "composite_score" not in combined.columns:
            self._emit("\n【评分预测力分析】样本不足20笔或缺少composite_score字段，暂不分组分析")
            return

        df = combined.dropna(subset=["composite_score", "outcome_pct"]).copy()
        if len(df) < 20:
            return

        try:
            df["score_bucket"] = pd.qcut(df["composite_score"], n_buckets, duplicates="drop")
        except Exception as e:
            self.logger.warning(f"评分分桶失败（可能分数分布过于集中）: {e}")
            return

        if df["score_bucket"].isna().all():
            self._emit("\n【评分预测力分析】composite_score分桶后全部为空值（可能取值种类过少），暂不分组分析")
            return

        rows = []
        for bucket, g in df.groupby("score_bucket", observed=True):
            if len(g) == 0:
                continue
            win_rate = (g["outcome"] == "WIN").mean()
            avg_pct = g["outcome_pct"].astype(float).mean()
            rows.append((str(bucket), len(g), win_rate, avg_pct))

        if len(rows) < 2:
            self._emit(f"\n【评分预测力分析】composite_score的取值种类太少（分桶后只有{len(rows)}组），"
                      f"没法看出分数和胜负的关系。这通常是因为当前样本里入选股票的层级过于单一"
                      f"（比如全部来自同一个tier），先积累更多样本或换更宽的universe再看")
            return

        self._emit(f"\n【评分预测力分析】composite_score从低到高分{len(rows)}组，"
                   f"看分数是否真的和胜负相关：")
        for bucket_label, n, wr, avg_pct in rows:
            self._emit(f"  分数区间 {bucket_label}: 样本{n}笔  胜率{wr:.1%}  平均单笔收益{avg_pct:+.2f}%")

        win_rates = [r[2] for r in rows]
        monotonic_up = all(win_rates[i] <= win_rates[i + 1] for i in range(len(win_rates) - 1))
        if monotonic_up and win_rates[-1] > win_rates[0]:
            self._emit("  → 分数越高胜率越高，呈现单调递增，composite_score有实际预测力，"
                      "可以考虑抬高入选门槛换胜率")
        elif win_rates[-1] <= win_rates[0]:
            self._emit("  → 最高分组胜率并不比最低分组好（甚至更差），composite_score当前的"
                      "权重设计可能没有真正抓住驱动胜负的因素，建议先查权重/硬性条件，"
                      "而不是简单抬高分数门槛")
        else:
            self._emit("  → 关系不完全单调，可能是样本量还不够大，建议积累更多样本后再下结论")


# ════════════════════════════════════════════════════════════
# GitHub参数队列工作流 —— 你在本地改队列文件+push，VM这边pull+跑，
# 结果推Telegram，全程不需要SSH进去手动敲命令
# ════════════════════════════════════════════════════════════

def run_queue(queue_path: str, max_minutes: Optional[float], logger: logging.Logger) -> None:
    """
    读一个纯文本队列文件，依次跑里面的实验。

    队列文件格式（逗号分隔，#开头的行和空行忽略）：
        param_set_name,params_file,start_date,end_date,universe[,universe_file]

    params_file留空表示用screener.py当前默认值(baseline)。
    params_file写相对路径时，相对的是队列文件所在目录（方便你把
    队列文件和params/*.json放在同一个git仓库里，路径不用写死绝对路径）。

    典型工作流：
      1. 本地电脑：编辑 queue.txt 加一行新实验 + 编辑/新增对应的
         params/xxx.json，git push
      2. VM这边crontab定时跑：
             cd ~/asx-backtest-configs && git pull
             cd ~/asx && python3 backtest_engine.py \\
                 --run-queue ~/asx-backtest-configs/queue.txt --max-minutes 700
      3. 每个实验跑完（或者达到整体时间预算）都会推一条Telegram，
         内容就是这个实验的完整统计报告
      4. 全程不需要SSH进VM手动敲命令，只需要设置好这一条crontab

    时间预算max_minutes是整个队列共享的，不是每个实验各自一份——
    如果队列有5个实验但今晚时间只够跑2个半，跑得完的跑完，跑不完的
    个实验会在下次处理队列时，靠断点续跑机制自动接着跑，不会重跑
    已经完成的部分。
    """
    if not os.path.exists(queue_path):
        logger.error(f"队列文件不存在: {queue_path}")
        if TELEGRAM_TOKEN:
            send_telegram(f"🔴 队列文件不存在: {queue_path}", logger)
        return

    queue_dir = os.path.dirname(os.path.abspath(queue_path))
    entries = []
    with open(queue_path, encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 5:
                logger.warning(f"队列文件第{line_no}行格式不对（少于5个字段），跳过: {line}")
                continue
            param_set_name, params_file, start_date, end_date, universe = parts[:5]
            universe_file = parts[5] if len(parts) > 5 else ""
            if params_file and not os.path.isabs(params_file):
                params_file = os.path.join(queue_dir, params_file)
            if universe_file and not os.path.isabs(universe_file):
                universe_file = os.path.join(queue_dir, universe_file)
            entries.append({
                "param_set_name": param_set_name, "params_file": params_file,
                "start_date": start_date, "end_date": end_date,
                "universe": universe, "universe_file": universe_file,
            })

    logger.info(f"队列共{len(entries)}个实验待处理")
    overall_start = time.time()
    processed_experiments = 0

    for idx, entry in enumerate(entries, 1):
        if max_minutes is not None:
            elapsed_min = (time.time() - overall_start) / 60
            remaining_min = max_minutes - elapsed_min
            if remaining_min <= 1:
                logger.info(f"队列整体时间预算用完，本次处理了{processed_experiments}/"
                            f"{len(entries)}个实验，剩下的下次继续（各实验内部也有"
                            f"断点续跑，不会丢进度）")
                break
        else:
            remaining_min = None

        logger.info(f"=== 队列 {idx}/{len(entries)}: {entry['param_set_name']} ===")

        # 每个实验开始前先把screener模块参数重置回原始默认值，
        # 避免上一个实验的覆盖残留污染这一个（同一进程内连续跑多个实验的坑）
        reset_screener_to_defaults()

        cfg = BacktestConfig(
            start_date=entry["start_date"], end_date=entry["end_date"],
            universe_source=entry["universe"], universe_file=entry["universe_file"],
        )

        overrides = {}
        if entry["params_file"]:
            if not os.path.exists(entry["params_file"]):
                logger.error(f"实验[{entry['param_set_name']}]的参数文件不存在: "
                             f"{entry['params_file']}，跳过这个实验")
                if TELEGRAM_TOKEN:
                    send_telegram(f"⚠️ 队列实验[{entry['param_set_name']}]跳过："
                                  f"参数文件不存在 {entry['params_file']}", logger)
                continue
            with open(entry["params_file"], encoding="utf-8") as f:
                content = f.read()
            if not content.strip():
                logger.error(f"实验[{entry['param_set_name']}]的参数文件是空文件，跳过这个实验")
                if TELEGRAM_TOKEN:
                    send_telegram(f"⚠️ 队列实验[{entry['param_set_name']}]跳过：参数文件是空文件", logger)
                continue
            try:
                overrides = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"实验[{entry['param_set_name']}]的参数文件不是合法JSON: {e}，跳过这个实验")
                if TELEGRAM_TOKEN:
                    send_telegram(f"⚠️ 队列实验[{entry['param_set_name']}]跳过：JSON格式错误 {e}", logger)
                continue
            apply_param_overrides(overrides, cfg, logger)

        cfg.param_set = entry["param_set_name"]
        record_experiment_metadata(cfg.db_path, cfg.param_set, entry["params_file"], overrides, logger)

        try:
            tickers = resolve_universe(cfg, logger)
            if not tickers:
                logger.error(f"实验[{cfg.param_set}] universe为空，跳过")
                continue

            engine = BacktestEngine(cfg)
            engine.run(tickers, max_minutes=remaining_min)

            reporter = StatsReporter(cfg, logger)
            reporter.report(only_selected=True, push_telegram=cfg.push_telegram)
        except Exception as e:
            tb = traceback.format_exc()
            logger.critical(f"队列实验[{cfg.param_set}]异常: {e}\n{tb}")
            if TELEGRAM_TOKEN:
                send_telegram(f"🔴 队列实验[{cfg.param_set}]崩溃: {e}\n"
                              f"traceback(截断):\n{tb[-1000:]}", logger)
            # 单个实验崩溃不影响队列里其他实验继续跑
            continue

        processed_experiments += 1

    logger.info(f"=== 队列处理完成：本次共处理{processed_experiments}/{len(entries)}个实验 ===")


def main():
    parser = argparse.ArgumentParser(description="ASX Screener EOD历史回测引擎（复用screener.py真实逻辑）")
    parser.add_argument("--start", default="2025-07-01")
    parser.add_argument("--end", default="2026-07-01")
    parser.add_argument("--universe", choices=["watchlist", "file", "full"], default="watchlist")
    parser.add_argument("--universe-file", default="")
    parser.add_argument("--stats-only", action="store_true", help="跳过回测，只对已有backtest_results.db做统计")
    parser.add_argument("--merge-live", action="store_true", help="合并线上signals_history一起统计")
    parser.add_argument("--live-db", default="", help="线上announcements.db路径，默认同目录")
    parser.add_argument("--tier", default="", help="只看某个tier，如 T1")
    parser.add_argument("--all-candidates", action="store_true",
                        help="统计T1-T4全部候选（不只是Top3），能更快积累样本量")
    parser.add_argument("--max-minutes", type=float, default=None,
                        help="本次运行的时间预算（分钟）。到点自动优雅停止并记录断点，"
                             "下次用相同参数重跑会自动续上，适合crontab每晚跑固定时长")
    parser.add_argument("--params-file", default="",
                        help="参数覆盖JSON文件路径，不传则使用screener.py当前默认值（baseline）")
    parser.add_argument("--param-set-name", default="",
                        help="本次实验的标签名，用于结果对比。不传则按params-file内容自动生成，"
                             "都不传则叫baseline")
    parser.add_argument("--export-params", default="",
                        help="导出当前screener.py默认参数为JSON模板到指定路径，导出后直接退出，不跑回测")
    parser.add_argument("--leaderboard", action="store_true",
                        help="列出db里所有参数集实验的胜率对比排行榜，不跑新回测")
    parser.add_argument("--health-status", default="",
                        help="只看某个跨日健康度状态的交易，如 ready/watch/caution/accumulating，"
                             "用来验证daily_analysis.py这层健康度过滤到底有没有增量价值")
    parser.add_argument("--run-queue", default="",
                        help="GitHub工作流用：指定队列文件路径，依次跑里面所有实验，"
                             "适合配合git pull + crontab实现全程不用SSH")
    parser.add_argument("--no-telegram", action="store_true",
                        help="本次运行不推送Telegram（即使配置了TOKEN/CHAT_ID）")
    parser.add_argument("--show-param-set", default="",
                        help="查询某个param_set当时实际用的参数内容+git commit，用于事后追溯")
    parser.add_argument("--export-csv", default="",
                        help="把本次统计的完整交易明细导出为CSV（每笔交易一行），"
                             "配合--no-telegram以外的情况会同时把CSV推送到Telegram，"
                             "适合需要深挖分析时用（比纯文字/md更适合让Claude直接用代码分析）")
    args = parser.parse_args()

    cfg = BacktestConfig(
        start_date=args.start, end_date=args.end,
        universe_source=args.universe, universe_file=args.universe_file,
        push_telegram=not args.no_telegram,
    )
    logger = setup_logging(cfg.log_path)

    # 顶层崩溃兜底：任何没被内层捕获的异常（比如resolve_universe本身出错、
    # 队列文件解析出错等），都在这里兜住，推一条Telegram报警，
    # 再用非0退出码结束进程，让nohup的日志和crontab的邮件/退出码
    # 都能看出"这次跑失败了"，而不是安安静静地在某个时间点消失。
    try:
        if args.show_param_set:
            show_param_set(cfg.db_path, args.show_param_set)
            return

        if args.export_params:
            export_default_params(args.export_params, cfg, logger)
            return

        if args.leaderboard:
            StatsReporter(cfg, logger).leaderboard(push_telegram=cfg.push_telegram)
            return

        if args.run_queue:
            run_queue(args.run_queue, args.max_minutes, logger)
            return

        # 参数覆盖必须在任何BacktestEngine/SignalGenerator/OutcomeSimulator/
        # DailyHealthEvaluator构建之前完成——OutcomeSimulator会在__init__时
        # 缓存BT_STOP_ATR_MULT等值，晚了就不生效，main()这里的顺序就是保证这一点。
        reset_screener_to_defaults()
        overrides = {}
        if args.params_file:
            if not os.path.exists(args.params_file):
                logger.error(f"参数文件不存在: {args.params_file}")
                return
            with open(args.params_file, encoding="utf-8") as f:
                content = f.read()
            if not content.strip():
                msg = (f"参数文件是空文件: {args.params_file}\n"
                       f"空文件不是合法JSON，解析会报错。用下面命令生成一份"
                       f"带默认值的模板再编辑：\n"
                       f"  python3 backtest_engine.py --export-params {args.params_file}")
                logger.error(msg)
                if cfg.push_telegram:
                    send_telegram(f"🔴 {msg}", logger)
                return
            try:
                overrides = json.loads(content)
            except json.JSONDecodeError as e:
                msg = (f"参数文件不是合法JSON: {args.params_file}\n"
                       f"解析错误: {e}\n"
                       f"检查有没有多打/少打逗号、引号，或者干脆用"
                       f"--export-params重新生成一份模板对照修改")
                logger.error(msg)
                if cfg.push_telegram:
                    send_telegram(f"🔴 {msg}", logger)
                return
            apply_param_overrides(overrides, cfg, logger)

        cfg.param_set = resolve_param_set_name(args.params_file, args.param_set_name)
        logger.info(f"本次实验标签: {cfg.param_set}")
        record_experiment_metadata(cfg.db_path, cfg.param_set, args.params_file, overrides, logger)

        if not args.stats_only:
            tickers = resolve_universe(cfg, logger)
            if not tickers:
                msg = f"🔴 回测终止 [{cfg.param_set}]：universe为空"
                logger.error(msg)
                if cfg.push_telegram:
                    send_telegram(msg, logger)
                return
            engine = BacktestEngine(cfg)
            engine.run(tickers, max_minutes=args.max_minutes)

        reporter = StatsReporter(cfg, logger)
        reporter.report(
            only_selected=not args.all_candidates,
            tier=args.tier or None,
            merge_live=args.merge_live,
            live_db=args.live_db,
            health_status=args.health_status or None,
            push_telegram=cfg.push_telegram,
            export_csv=args.export_csv or None,
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.critical(f"main()顶层未捕获异常: {e}\n{tb}")
        if cfg.push_telegram:
            send_telegram(
                f"🔴 backtest_engine.py 进程崩溃\n参数集: {cfg.param_set}\n"
                f"错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                logger,
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
