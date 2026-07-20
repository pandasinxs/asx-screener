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

用法:
    # 先用watchlist里的股票跑通（几分钟级别，Oracle Free Tier能扛住）
    python3 backtest_engine.py --start 2025-07-01 --end 2026-07-01 --universe watchlist

    # 用自定义股票清单文件（每行一个ticker，如 BHP.AX）
    python3 backtest_engine.py --start 2025-07-01 --end 2026-07-01 --universe file --universe-file my_list.txt

    # 全市场（非常慢，建议 nohup python3 backtest_engine.py ... &）
    python3 backtest_engine.py --start 2025-01-01 --end 2026-07-01 --universe full

    # 跑完后，把历史回测结果和线上signals_history合并统计
    python3 backtest_engine.py --stats-only --merge-live
"""

import argparse
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

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

    db_path: str = os.path.join(ASX_DIR, "backtest_results.db")
    log_path: str = os.path.join(ASX_DIR, "backtest.log")

    benchmark_ticker: str = "^AXJO"

    # 供"补充性、非可比"的真实成本估算层使用（不影响与signals_history可比的核心统计）
    commission_pct: float = 0.0011
    commission_min_aud: float = 7.0
    slippage_bps: float = 5.0


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

    def __init__(self, logger: logging.Logger, max_retries: int = 3):
        self.logger = logger
        self.max_retries = max_retries
        self._cache: dict[str, pd.DataFrame] = {}

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
                    self.logger.warning(f"{ticker}: 返回空数据 attempt={attempt}")
                    time.sleep(1.5 * attempt)
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
                self.logger.warning(f"{ticker}: 拉取失败 attempt={attempt} error={e}")
                time.sleep(1.5 * attempt)

        self.logger.error(f"{ticker}: 三次重试后仍失败，跳过")
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
        self._market_cap_cache: dict[str, float] = {}

    def _get_market_cap(self, ticker: str) -> float:
        """当前市值，作为历史各交易日的静态代理（yfinance无历史市值）。"""
        if ticker in self._market_cap_cache:
            return self._market_cap_cache[ticker]
        try:
            info = yf.Ticker(ticker).info
            cap = float(info.get("marketCap", 0) or 0)
        except Exception as e:
            self.logger.debug(f"市值获取失败 [{ticker}]: {e}")
            cap = 0.0
        self._market_cap_cache[ticker] = cap
        return cap

    def scan_day(
        self,
        as_of: pd.Timestamp,
        history: dict[str, pd.DataFrame],
        xjo_full: Optional[pd.Series],
    ) -> tuple[list[dict], list[dict]]:
        """
        返回 (raw_top10, selected_top3)，与screener.select_top3()返回结构对应。
        """
        xjo_slice = xjo_full[xjo_full.index <= as_of] if xjo_full is not None else None
        seen: dict[str, dict] = {}

        for tier in screener.TIERS:
            for ticker, df in history.items():
                if ticker in seen:
                    continue
                pit_df = self.__class__._slice(df, as_of)
                if len(pit_df) < self.cfg.min_history_days:
                    continue
                try:
                    tech = screener.build_tech_summary(pit_df, xjo_slice)
                except Exception as e:
                    self.logger.debug(f"build_tech_summary异常 [{ticker}] {as_of.date()}: {e}")
                    continue

                try:
                    passed = screener._passes_tier(tech, tier)
                except Exception as e:
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
            return [], []

        raw_signals = list(seen.values())
        for s in raw_signals:
            s["composite_score"] = screener.calc_composite_score(s)
        raw_signals.sort(key=lambda x: x["composite_score"], reverse=True)
        raw_top10 = raw_signals[:10]

        filtered_pool = []
        for s in raw_top10:
            cap = self._get_market_cap(s["ticker"])
            if cap < self.cfg.min_market_cap:
                continue
            s["market_cap_m"] = round(cap / 1e6, 1)
            s["confidence"] = screener.calc_confidence(s, s["tier_level"])
            filtered_pool.append(s)

        selected_top3 = filtered_pool[:3]
        return raw_top10, selected_top3

    @staticmethod
    def _slice(df: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        return df[df.index <= as_of]


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
    run_timestamp   TEXT,
    UNIQUE(ticker, signal_date)
)
"""


class BacktestEngine:
    def __init__(self, cfg: BacktestConfig):
        self.cfg = cfg
        self.logger = setup_logging(cfg.log_path)
        self.data_layer = DataLayer(self.logger)
        self.sig_gen = SignalGenerator(cfg, self.logger)
        self.sim = OutcomeSimulator(cfg, self.logger)

    def _init_db(self, conn: sqlite3.Connection):
        conn.execute(SCHEMA_SQL)
        conn.commit()

    def run(self, tickers: list[str]):
        self.logger.info(f"=== 回测启动 {self.cfg.start_date} ~ {self.cfg.end_date} "
                          f"universe={self.cfg.universe_source}({len(tickers)}只) ===")

        history: dict[str, pd.DataFrame] = {}
        for t in tickers:
            df = self.data_layer.fetch(t, self.cfg.start_date, self.cfg.end_date)
            if df is not None and len(df) >= self.cfg.min_history_days:
                history[t] = df

        self.logger.info(f"有效历史数据：{len(history)}/{len(tickers)} 只")
        if not history:
            self.logger.error("无有效数据，终止")
            return

        xjo_full = self.data_layer.fetch(self.cfg.benchmark_ticker,
                                          self.cfg.start_date, self.cfg.end_date)
        xjo_series = xjo_full["Close"].squeeze() if xjo_full is not None else None

        trading_days = sorted(set().union(*[df.index for df in history.values()]))
        trading_days = [d for d in trading_days if d >= pd.Timestamp(self.cfg.start_date)]
        self.logger.info(f"回测交易日数：{len(trading_days)}")

        conn = sqlite3.connect(self.cfg.db_path)
        self._init_db(conn)
        run_ts = datetime.now().isoformat()

        total_written, total_selected = 0, 0

        for i, day in enumerate(trading_days):
            if i % 20 == 0:
                self.logger.info(f"进度 {i}/{len(trading_days)} ({day.date()}) "
                                  f"累计写入{total_written}条 已选出{total_selected}笔Top3信号")

            try:
                raw_top10, selected = self.sig_gen.scan_day(day, history, xjo_series)
            except Exception as e:
                self.logger.error(f"scan_day异常 {day.date()}: {e}")
                continue

            if not raw_top10:
                continue

            selected_tickers = {s["ticker"] for s in selected}
            rows = []
            for s in raw_top10:
                ticker = s["ticker"]
                entry_price = float(s["price"])
                atr14_pct = float(s.get("atr14_pct", 2.0))

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
                        run_ts,
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
                        outcome_result["max_loss_pct"], run_ts,
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
                        holding_days, max_gain_pct, max_loss_pct, run_timestamp
                    ) VALUES ({",".join(["?"] * 25)})
                """, rows)
                conn.commit()
                total_written += len(rows)
                total_selected += len(selected)

        conn.close()
        self.logger.info(f"=== 回测完成：共写入 {total_written} 条候选记录 "
                          f"（其中 {total_selected} 笔为Top3精选信号）===")


# ════════════════════════════════════════════════════════════
# 统计报告层
# ════════════════════════════════════════════════════════════

class StatsReporter:
    def __init__(self, cfg: BacktestConfig, logger: logging.Logger):
        self.cfg = cfg
        self.logger = logger

    def _bootstrap_ci(self, wins: np.ndarray, n_iter: int = 2000) -> tuple[float, float]:
        if len(wins) == 0:
            return (0.0, 0.0)
        rng = np.random.default_rng(42)
        rates = [rng.choice(wins, size=len(wins), replace=True).mean() for _ in range(n_iter)]
        return float(np.percentile(rates, 5)), float(np.percentile(rates, 95))

    def _load_backtest_df(self, only_selected: bool, tier: Optional[str]) -> pd.DataFrame:
        conn = sqlite3.connect(self.cfg.db_path)
        query = "SELECT *, 'backtest' AS source FROM signals_history_backtest WHERE outcome != 'PENDING'"
        if only_selected:
            query += " AND is_selected = 1"
        if tier:
            query += f" AND tier_level = '{tier}'"
        df = pd.read_sql_query(query, conn)
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
        if only_selected:
            query += " AND is_selected = 1"
        if tier:
            query += f" AND tier_level = '{tier}'"
        try:
            df = pd.read_sql_query(query, conn)
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

    def report(self, only_selected: bool = True, tier: Optional[str] = None,
               merge_live: bool = False, live_db: str = ""):
        bt_df = self._load_backtest_df(only_selected, tier)
        frames = [bt_df]
        if merge_live:
            live_df = self._load_live_df(live_db or os.path.join(ASX_DIR, "announcements.db"),
                                          only_selected, tier)
            frames.append(live_df)

        combined = pd.concat(frames, ignore_index=True) if any(len(f) for f in frames) else pd.DataFrame()

        if combined.empty:
            print("无可用交易记录（outcome全部为PENDING，或数据库为空）")
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

        tier_note = f"（仅{tier}层级）" if tier else "（全部T1-T4层级）"
        scope_note = "仅Top3精选信号" if only_selected else "T1-T4全部候选（含未入选）"

        lines = [
            "\n" + "=" * 58,
            f"回测统计报告 {tier_note} — {scope_note}",
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
            "  - 市值门槛用当前市值做历史代理，可能高估早期小盘股的入选概率",
            "  - catalyst固定为0，本结果只反映趋势/技术面逻辑，不含公告驱动信号",
            "  - 止盈止损假设完美成交，不含跳空穿仓风险，实际胜率可能更保守",
            "  - TREND_SCORE_THRESHOLD由近期数据校准，套用早期历史存在轻微前视偏差",
            "=" * 58,
        ]
        report_text = "\n".join(lines)
        print(report_text)
        self.logger.info(report_text)

        if "tier_level" in combined.columns and combined["tier_level"].nunique() > 1:
            print("\n【分层级胜率对比（合并样本）】")
            for lv, g in combined.groupby("tier_level"):
                w = (g["outcome"] == "WIN").mean()
                print(f"  {lv}: 样本{len(g)}笔  胜率{w:.1%}")


# ════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════

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
    args = parser.parse_args()

    cfg = BacktestConfig(
        start_date=args.start, end_date=args.end,
        universe_source=args.universe, universe_file=args.universe_file,
    )
    logger = setup_logging(cfg.log_path)

    if not args.stats_only:
        tickers = resolve_universe(cfg, logger)
        if not tickers:
            logger.error("universe为空，终止")
            return
        engine = BacktestEngine(cfg)
        engine.run(tickers)

    reporter = StatsReporter(cfg, logger)
    reporter.report(
        only_selected=not args.all_candidates,
        tier=args.tier or None,
        merge_live=args.merge_live,
        live_db=args.live_db,
    )


if __name__ == "__main__":
    main()
