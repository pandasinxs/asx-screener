#!/usr/bin/env python3
"""
market_data_cache.py
====================
本地行情数据缓存层 —— Parquet存储 + SQLite清单，供 data_fetcher.py（批量
预热/每周增量抓取）和 backtest_engine.py（回测时按需读取）共用同一套
"查本地缓存 → 只补缺的那一段 → 写回 → 返回"逻辑，避免每次跑backtest都
对yfinance重新请求一遍已经拿到过的数据。

设计:
    每只股票、每种颗粒度（daily/60m/15m）各自一个独立的Parquet文件，
    路径形如 <cache_dir>/<granularity>/<TICKER>.parquet。选择"每票一个
    文件"而不是"全市场一个大文件"，是因为增量更新（每周补15分钟线）
    只需要重写这一只股票的文件，不需要碰其他上千只股票的数据——
    Parquet本身不支持高效的行级追加，"文件按ticker切开"是绕开这个
    限制最简单的办法。

    覆盖范围元数据（每只股票每种颗粒度当前缓存到哪天）另外记在一个
    独立的SQLite清单里（cache_manifest.db），不需要每次都打开Parquet
    文件读取index才能知道"这只股票缓存到几号了"，方便快速审计缺口
    （data_fetcher.py --coverage 用的就是这张表）。

颗粒度对应的yfinance历史深度上限（数据源本身的硬限制，不是本模块的
限制，本模块只是如实反映）：
    daily : 实际很深（yfinance对日线基本能给到上市以来全部历史），
            本模块不对daily做提前的"早于可用窗口"校验
    60m   : 约729天
    15m   : 约60天——每次请求最多只能拿到"距今60天以内"的部分，无法
            补更早的历史。这也是为什么15m需要"按周定期运行
            data_fetcher.py --mode weekly15m"来累积，而不是"跑一次
            就能补全历史"：只要不断档超过~60天，每周新抓的这一段
            会跟上次缓存的末尾无缝衔接，长期下来15m归档范围就单调
            往前滚动扩大。

数据清洗规则跟backtest_engine.py v2.1之前DataLayer里的规则完全一致，
只是从那边搬过来集中管理：
    - 去重（index重复的K线，保留先出现的那条）
    - 丢弃OHLCV任一字段为空的行
    - daily专属：剔除单日涨跌幅绝对值>80%的疑似异常价格bar
      （intraday颗粒度不做这条过滤，因为60分钟/15分钟单根bar的正常
      波动幅度跟日线不是一回事，套用同一个80%阈值没有意义）
    - 60m/15m专属：转换成悉尼当地挂钟时间后去掉tz标记（变成naive
      时间戳），保持跟daily数据的时间戳类型一致，避免tz-aware和
      tz-naive比较时抛TypeError（这个坑backtest_engine.py v2.1修复
      时已经踩过一次，这里从源头上保证不会再发生）

不依赖 backtest_engine.py 或 screener.py，可以被两边独立import，
也可以被 data_fetcher.py 单独调用做批量预热/增量累积。

依赖:
    需要 pyarrow（或 fastparquet）才能读写Parquet文件，VM上如果还没装：
        pip install pyarrow --break-system-packages
"""

import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("缺少 yfinance: pip install yfinance --break-system-packages")
    raise

SYD_TZ = ZoneInfo("Australia/Sydney")

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CACHE_DIR = os.path.join(_MODULE_DIR, "market_data_cache")

# 每种颗粒度对应的yfinance interval字符串 + 历史深度上限（天）。
# max_history_days=None表示不做提前校验（daily实际上限很深，没必要
# 每次请求前都算一遍"是不是早于上限"）。
GRANULARITY_CONFIG = {
    "daily": {"interval": "1d", "max_history_days": None},
    "60m":   {"interval": "60m", "max_history_days": 729},
    "15m":   {"interval": "15m", "max_history_days": 59},  # 留1天安全余量。曾经短暂改成55天，
                                                              # 是误判——把"这只股票15m颗粒度本身
                                                              # 没数据"错当成"窗口边界问题"，后经
                                                              # 交叉验证（同一59天窗口下714只成功、
                                                              # 失败的股票daily/60m数据都完好）证实
                                                              # 与窗口宽度无关，改回59天
}

MANIFEST_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cache_coverage (
    ticker          TEXT NOT NULL,
    granularity     TEXT NOT NULL,
    earliest_date   TEXT,
    latest_date     TEXT,
    row_count       INTEGER,
    last_updated_at TEXT,
    PRIMARY KEY (ticker, granularity)
)
"""


class MarketDataCache:
    def __init__(self, cache_dir: Optional[str] = None,
                 logger: Optional[logging.Logger] = None,
                 max_retries: int = 5):
        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self.manifest_path = os.path.join(self.cache_dir, "cache_manifest.db")
        self.logger = logger or logging.getLogger("market_data_cache")
        self.max_retries = max_retries
        for granularity in GRANULARITY_CONFIG:
            os.makedirs(os.path.join(self.cache_dir, granularity), exist_ok=True)

    @staticmethod
    def _backoff_seconds(attempt: int) -> float:
        return min(60.0, 3.0 * (2 ** (attempt - 1)))

    # ────────────────────────────────────────────────────────
    # 对外接口：三种颗粒度分别的get方法
    # ────────────────────────────────────────────────────────

    def get_daily(self, ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
        return self._get(ticker, "daily", start, end)

    def get_60m(self, ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
        return self._get(ticker, "60m", start, end)

    def get_15m(self, ticker: str, start: str, end: str) -> Optional[pd.DataFrame]:
        return self._get(ticker, "15m", start, end)

    def coverage_summary(self, granularity: Optional[str] = None) -> pd.DataFrame:
        """
        审计用：查看当前缓存覆盖情况，不发任何网络请求。
        granularity不传则返回全部颗粒度。
        """
        if not os.path.exists(self.manifest_path):
            return pd.DataFrame()
        conn = sqlite3.connect(self.manifest_path)
        try:
            query = "SELECT * FROM cache_coverage"
            params: list = []
            if granularity:
                query += " WHERE granularity = ?"
                params.append(granularity)
            query += " ORDER BY ticker"
            df = pd.read_sql_query(query, conn, params=params)
        except Exception:
            df = pd.DataFrame()
        finally:
            conn.close()
        return df

    # ────────────────────────────────────────────────────────
    # 核心：查缓存 → 补缺口 → 存 → 返回
    # ────────────────────────────────────────────────────────

    def _get(self, ticker: str, granularity: str, start: str, end: str) -> Optional[pd.DataFrame]:
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
        cached = self._read_cached(ticker, granularity)
        gaps = self._missing_ranges(cached, start_ts, end_ts)

        for gap_start, gap_end in gaps:
            fresh = self._fetch_from_yf(
                ticker, granularity,
                str(gap_start.date()), str(gap_end.date()),
            )
            cached = self._merge_and_save(ticker, granularity, cached, fresh)

        if cached is None or cached.empty:
            return None
        result = cached[(cached.index >= start_ts) & (cached.index <= end_ts)]
        return result if not result.empty else None

    @staticmethod
    def _missing_ranges(cached: Optional[pd.DataFrame], start: pd.Timestamp,
                        end: pd.Timestamp) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        """
        返回需要额外向yfinance请求的缺口区间（0/1/2段）。

        允许在缺口边界跟已有覆盖范围有1天左右的重叠（比如"前段缺口"
        故意取到cov_start而不是cov_start-1天）——重叠部分在
        _merge_and_save()里会被去重（keep="last"，新拿到的数据覆盖
        旧的），这样处理比精确计算"下一个交易日"简单得多，也不会因为
        节假日/非交易日的边界情况出错。
        """
        if cached is None or cached.empty:
            return [(start, end)]
        cov_start = cached.index.min()
        cov_end = cached.index.max()
        gaps = []
        if start < cov_start:
            gaps.append((start, cov_start))
        if end > cov_end:
            gaps.append((cov_end, end))
        return gaps

    def _merge_and_save(self, ticker: str, granularity: str,
                        cached: Optional[pd.DataFrame],
                        fresh: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        frames = [f for f in (cached, fresh) if f is not None and not f.empty]
        if not frames:
            return cached
        merged = pd.concat(frames)
        merged = merged[~merged.index.duplicated(keep="last")]
        merged = merged.sort_index()

        path = self._ticker_path(granularity, ticker)
        try:
            merged.to_parquet(path)
            self._update_manifest(ticker, granularity, merged)
        except Exception as e:
            self.logger.error(
                f"缓存写入失败 [{ticker}/{granularity}]: {e}"
                f"（本次仍会把刚拿到的数据返回给调用方，只是没有落盘，"
                f"下次调用会重新请求这一段）"
            )
        return merged

    def _update_manifest(self, ticker: str, granularity: str, df: pd.DataFrame) -> None:
        try:
            conn = sqlite3.connect(self.manifest_path)
            conn.execute(MANIFEST_SCHEMA_SQL)
            conn.execute("""
                INSERT INTO cache_coverage
                    (ticker, granularity, earliest_date, latest_date, row_count, last_updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker, granularity) DO UPDATE SET
                    earliest_date=excluded.earliest_date,
                    latest_date=excluded.latest_date,
                    row_count=excluded.row_count,
                    last_updated_at=excluded.last_updated_at
            """, (
                ticker, granularity,
                str(df.index.min()), str(df.index.max()), len(df),
                datetime.now().isoformat(),
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.warning(
                f"缓存清单更新失败 [{ticker}/{granularity}]"
                f"（不影响数据本身已经落盘，只是coverage_summary()会暂时不准）: {e}"
            )

    # ────────────────────────────────────────────────────────
    # 本地读写
    # ────────────────────────────────────────────────────────

    def _ticker_path(self, granularity: str, ticker: str) -> str:
        safe_name = ticker.replace("/", "_")  # ASX代码本身不会有斜杠，防御性处理
        return os.path.join(self.cache_dir, granularity, f"{safe_name}.parquet")

    def _read_cached(self, ticker: str, granularity: str) -> Optional[pd.DataFrame]:
        path = self._ticker_path(granularity, ticker)
        if not os.path.exists(path):
            return None
        try:
            df = pd.read_parquet(path)
            return df if not df.empty else None
        except Exception as e:
            self.logger.error(f"缓存读取失败 [{ticker}/{granularity}]，当作无缓存处理: {e}")
            return None

    # ────────────────────────────────────────────────────────
    # yfinance拉取（带重试），三种颗粒度共用一套逻辑
    # ────────────────────────────────────────────────────────

    def _fetch_from_yf(self, ticker: str, granularity: str,
                       start: str, end: str) -> Optional[pd.DataFrame]:
        cfg = GRANULARITY_CONFIG[granularity]
        interval = cfg["interval"]
        max_days = cfg["max_history_days"]

        if max_days is not None:
            earliest_allowed = pd.Timestamp.now().normalize() - pd.Timedelta(days=max_days)
            if pd.Timestamp(start) < earliest_allowed:
                self.logger.warning(
                    f"{ticker}/{granularity}: 请求起点{start}早于该颗粒度约"
                    f"{max_days}天的可用窗口（约从{earliest_allowed.date()}起），"
                    f"更早的部分yfinance不提供——这是数据源本身的硬限制，不是"
                    f"本模块的bug。{granularity}只能靠按周期定期运行慢慢往前"
                    f"累积，没法一次性回补历史。"
                )

        # 空结果（yfinance成功查询但返回0行）和网络异常，用两套不同的重试预算：
        #   - 网络异常（超时/连接失败等）更可能是瞬时问题，值得用完整的
        #     max_retries次退避重试
        #   - 空结果更可能代表"这只股票在这个颗粒度上确实没有数据"（停牌/
        #     极低流动性/yfinance数据源缺口），重试更多次大概率还是空——
        #     实测中这类情况在15m颗粒度上占比不低，如果跟异常用同一套
        #     max_retries=5的退避预算，会在明知大概率无解的请求上浪费
        #     大量时间（每只失败的股票要消耗约93秒的累计退避等待）。
        #     这里单独限制空结果最多重试empty_max_retries次就提前放弃。
        empty_max_retries = min(2, self.max_retries)
        empty_retry_count = 0

        for attempt in range(1, self.max_retries + 1):
            try:
                df = yf.download(
                    ticker, start=start, end=end, interval=interval,
                    auto_adjust=True, progress=False, threads=False,
                )
                if df is None or df.empty:
                    empty_retry_count += 1
                    self.logger.debug(
                        f"{ticker}/{granularity}: 返回空数据 "
                        f"attempt={attempt}/{self.max_retries}"
                    )
                    if empty_retry_count >= empty_max_retries:
                        self.logger.info(
                            f"{ticker}/{granularity}: 连续{empty_retry_count}次查询"
                            f"成功但返回空结果，大概率是这只股票在该颗粒度上确实"
                            f"没有数据（yfinance对'请求超出可用窗口'和'纯粹没有"
                            f"数据'这两种完全不同的情况会打印同一句报错文字，没法"
                            f"直接从文字区分），提前放弃，不再消耗剩余重试次数"
                        )
                        return None
                    time.sleep(self._backoff_seconds(attempt))
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df[~df.index.duplicated(keep="first")]
                df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])

                if granularity == "daily":
                    bad = df["Close"].pct_change().abs() > 0.80
                    if bad.sum() > 0:
                        self.logger.warning(f"{ticker}: 剔除{bad.sum()}个疑似异常价格bar")
                        df = df[~bad]
                else:
                    if df.index.tz is None:
                        df.index = df.index.tz_localize("UTC").tz_convert(SYD_TZ).tz_localize(None)
                    else:
                        df.index = df.index.tz_convert(SYD_TZ).tz_localize(None)

                if df.empty:
                    return None
                return df

            except Exception as e:
                self.logger.warning(
                    f"{ticker}/{granularity}: 拉取失败 "
                    f"attempt={attempt}/{self.max_retries} error={e}"
                )
                time.sleep(self._backoff_seconds(attempt))

        self.logger.error(f"{ticker}/{granularity}: {self.max_retries}次重试后仍失败，跳过")
        return None
