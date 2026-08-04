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
    # 批量预热（v1.2新增）—— 用少量大批次yfinance请求替代逐票单独
    # 请求，大幅减少网络请求次数
    #
    # 背景：实测发现"每只股票只补1-2天增量"和"每只股票补完整历史"
    # 耗时几乎一样长——真正决定总耗时的是请求次数（网络往返/连接
    # 开销），不是每次请求带的数据范围大小。逐票请求2000次网络调用，
    # 不管每次要的是1天还是1000天数据，总耗时量级不变。批量请求把
    # 2000次压缩成~40次（每批50只，跟screener.py的download_ohlcv()
    # 用的是同一个批次大小，已经在生产环境验证过稳定可靠），能带来
    # 数量级的提速。
    #
    # 批量请求不影响数据本身的正确性/精度——同一个yfinance/Yahoo接口，
    # 只是把50只股票的请求打包进一次HTTP调用，每只股票拿到的OHLCV
    # 数值跟单独请求完全一致。
    # ────────────────────────────────────────────────────────

    def _coverage_from_manifest(self, granularity: str) -> dict:
        """
        从manifest批量读取"每只股票当前缓存到哪天"，用于warm_batch()
        筛选"本地已经完整覆盖、完全不需要碰网络"的股票。比逐只打开
        Parquet文件读取index快得多——manifest是一张小表，一次SQL查询
        就能拿到全部股票的覆盖范围，不需要对每只股票单独做一次磁盘
        I/O+反序列化。

        如果manifest因为某种原因跟实际Parquet文件内容不完全同步
        （比如某次_update_manifest()写入失败但Parquet本身写成功了），
        最坏结果只是把那只股票误判成"需要重新拉取"，多打一次网络
        请求，不会有数据正确性问题——真正的数据读写永远走Parquet
        文件本身，manifest只是加速用的索引，不是唯一真相来源。
        """
        if not os.path.exists(self.manifest_path):
            return {}
        conn = sqlite3.connect(self.manifest_path)
        try:
            rows = conn.execute(
                "SELECT ticker, earliest_date, latest_date FROM cache_coverage "
                "WHERE granularity = ?",
                (granularity,),
            ).fetchall()
        except Exception:
            rows = []
        finally:
            conn.close()
        result = {}
        for ticker, earliest, latest in rows:
            try:
                result[ticker] = (pd.Timestamp(earliest), pd.Timestamp(latest))
            except Exception:
                continue
        return result

    def warm_batch(self, tickers: list[str], granularity: str, start: str, end: str,
                   batch_size: int = 50, max_minutes: Optional[float] = None,
                   max_consecutive_batch_failures: int = 3,
                   progress_every_n_batches: int = 5) -> dict:
        """
        批量预热指定颗粒度的数据到[start, end]区间。

        流程：
          1. 用manifest快速筛掉"本地已经完整覆盖[start,end]"的股票，
             这些完全不碰网络
          2. 剩下真正需要更新的股票按batch_size分批，每批一次
             yf.download(多只股票列表)请求
          3. 批内每只股票各自merge进它自己的Parquet文件（跟单只
             请求路径共享同一套_merge_and_save逻辑，保证两条路径
             写出来的缓存格式完全一致）

          为了让批量逻辑保持简单，批内每只需要更新的股票统一请求
          [start,end]这整段（不是各自精确计算的缺口），代价是"本地
          已经有的那部分会被重新拿一次、重新写一次"，这个代价很小
          （多余的网络传输量本身不是瓶颈，请求次数才是），换来的是
          代码简单、批内所有股票能共用同一次请求。

        熔断：批次级别（不是单只股票级别）——如果连续
        max_consecutive_batch_failures批全部返回空（每批已经内部
        重试过self.max_retries次），这个信号比"个别股票没数据"强得多
        （50只股票同时颗粒度缺失的概率极低），大概率是网络/yfinance
        整体出问题，提前停止。

        返回:
            {
                "total": 传入的股票总数,
                "already_cached": 本地已完整覆盖、跳过的股票数,
                "fetched_ok": 本次成功拉取到新数据的股票数,
                "fetched_fail": 本次尝试但失败的股票数,
                "processed_batches": 实际处理的批次数,
                "total_batches": 需要处理的批次总数,
                "circuit_broken": 是否因熔断提前停止,
                "circuit_break_message": 熔断提示文字（没触发则为None）,
                "time_budget_stopped": 是否因时间预算提前停止,
            }
        """
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
        coverage_map = self._coverage_from_manifest(granularity)

        need_fetch = []
        already_cached = 0
        for ticker in tickers:
            cov = coverage_map.get(ticker)
            if cov is not None and cov[0] <= start_ts and cov[1] >= end_ts:
                already_cached += 1
                continue
            need_fetch.append(ticker)

        self.logger.info(
            f"[{granularity}] 预热：{len(tickers)}只中{already_cached}只本地已覆盖"
            f"（跳过），{len(need_fetch)}只需要请求（每批{batch_size}只）"
        )

        batches = [need_fetch[i:i + batch_size] for i in range(0, len(need_fetch), batch_size)]
        fetched_ok = fetched_fail = 0
        processed_batches = 0
        consecutive_full_batch_fail = 0
        circuit_broken = False
        circuit_break_message = None
        time_budget_stopped = False
        start_time = time.time()

        for bi, batch in enumerate(batches):
            if max_minutes is not None and (time.time() - start_time) / 60 >= max_minutes:
                self.logger.info(
                    f"[{granularity}] 达到时间预算({max_minutes}分钟)，提前结束，"
                    f"已处理{processed_batches}/{len(batches)}批，下次原样重跑会"
                    f"自动跳过已覆盖的部分，接着补剩下的"
                )
                time_budget_stopped = True
                break

            results = self._fetch_batch_from_yf(batch, granularity, start, end)

            batch_ok = 0
            for ticker in batch:
                fresh = results.get(ticker)
                cached = self._read_cached(ticker, granularity)
                self._merge_and_save(ticker, granularity, cached, fresh)
                if fresh is not None and not fresh.empty:
                    fetched_ok += 1
                    batch_ok += 1
                else:
                    fetched_fail += 1

            processed_batches += 1
            consecutive_full_batch_fail = 0 if batch_ok > 0 else consecutive_full_batch_fail + 1

            if consecutive_full_batch_fail >= max_consecutive_batch_failures:
                circuit_break_message = (
                    f"[{granularity}] 连续{consecutive_full_batch_fail}批"
                    f"（每批{batch_size}只，每批内部已重试{self.max_retries}次）"
                    f"全部拉取失败——单只股票偶尔没数据是正常现象，但连续好几批"
                    f"整批50只全部失败的概率极低，大概率是网络/yfinance整体"
                    f"出问题，已提前停止，不再继续空转浪费时间。已处理"
                    f"{processed_batches}/{len(batches)}批，原样重跑会自动跳过"
                    f"已成功的部分"
                )
                self.logger.critical(circuit_break_message)
                circuit_broken = True
                break

            if processed_batches % progress_every_n_batches == 0:
                elapsed = time.time() - start_time
                self.logger.info(
                    f"[{granularity}] 批次进度 {processed_batches}/{len(batches)}，"
                    f"已用{elapsed/60:.1f}分钟 | 成功{fetched_ok}/失败{fetched_fail}"
                )

            time.sleep(1.0)  # 批次之间轻微限速，比逐票的0.3秒间隔更宽松，
                              # 因为每批已经是50只股票打包成一次请求

        return {
            "total": len(tickers), "already_cached": already_cached,
            "fetched_ok": fetched_ok, "fetched_fail": fetched_fail,
            "processed_batches": processed_batches, "total_batches": len(batches),
            "circuit_broken": circuit_broken, "circuit_break_message": circuit_break_message,
            "time_budget_stopped": time_budget_stopped,
        }

    def _fetch_batch_from_yf(self, tickers: list[str], granularity: str,
                             start: str, end: str) -> dict:
        """
        一次yf.download()请求多只股票，返回 {ticker: DataFrame或None}。

        重试策略跟单只请求的_fetch_from_yf()不同：这里只对"整个批次
        请求本身失败"（HTTP/网络异常，或者yfinance返回完全空的结果）
        重试，不对"批次请求成功、但批内某几只股票没有数据"这种情况
        重试整个批次——后者是正常的个股噪音（对应此前实测的15分钟
        颗粒度背景失败率），重试整批只会让已经成功的那些股票被
        无意义地重新请求一遍，不会让本来没数据的股票变得有数据。
        """
        cfg = GRANULARITY_CONFIG[granularity]
        interval = cfg["interval"]
        max_days = cfg["max_history_days"]

        if max_days is not None:
            earliest_allowed = pd.Timestamp.now().normalize() - pd.Timedelta(days=max_days)
            if pd.Timestamp(start) < earliest_allowed:
                self.logger.warning(
                    f"[{granularity}] 批量请求起点{start}早于该颗粒度约"
                    f"{max_days}天的可用窗口（约从{earliest_allowed.date()}起），"
                    f"更早的部分yfinance不提供——数据源本身的硬限制"
                )

        results: dict = {t: None for t in tickers}

        for attempt in range(1, self.max_retries + 1):
            try:
                raw = yf.download(
                    tickers, start=start, end=end, interval=interval,
                    auto_adjust=True, progress=False, threads=False,
                    group_by="ticker",
                )
            except Exception as e:
                self.logger.warning(
                    f"[{granularity}] 批量拉取异常 attempt={attempt}/{self.max_retries} "
                    f"批次大小={len(tickers)} error={e}"
                )
                time.sleep(self._backoff_seconds(attempt))
                continue

            if raw is None or raw.empty:
                self.logger.debug(
                    f"[{granularity}] 批量拉取整批返回空 "
                    f"attempt={attempt}/{self.max_retries} 批次大小={len(tickers)}"
                )
                time.sleep(self._backoff_seconds(attempt))
                continue

            # 批次整体请求成功（哪怕批内个别股票没有数据）——不再重试，
            # 直接解析当前结果并返回
            if len(tickers) == 1:
                sub_frames = {tickers[0]: raw}
            else:
                sub_frames = {}
                for t in tickers:
                    try:
                        sub_frames[t] = raw[t]
                    except Exception:
                        continue  # 这只股票在这批返回结果里没有对应的列，跳过

            for t, tdf in sub_frames.items():
                try:
                    tdf = tdf.dropna(how="all")
                    if tdf.empty:
                        continue
                    if isinstance(tdf.columns, pd.MultiIndex):
                        tdf.columns = tdf.columns.get_level_values(0)
                    tdf = tdf[~tdf.index.duplicated(keep="first")]
                    tdf = tdf.dropna(subset=["Open", "High", "Low", "Close", "Volume"])

                    if granularity == "daily":
                        bad = tdf["Close"].pct_change().abs() > 0.80
                        if bad.sum() > 0:
                            tdf = tdf[~bad]
                    else:
                        if tdf.index.tz is None:
                            tdf.index = tdf.index.tz_localize("UTC").tz_convert(SYD_TZ).tz_localize(None)
                        else:
                            tdf.index = tdf.index.tz_convert(SYD_TZ).tz_localize(None)

                    if not tdf.empty:
                        results[t] = tdf
                except Exception as e:
                    self.logger.debug(f"[{granularity}] 批内单只解析异常 [{t}]: {e}")
                    continue

            return results

        self.logger.error(
            f"[{granularity}] 批量拉取{self.max_retries}次重试后仍失败，"
            f"这批{len(tickers)}只全部跳过"
        )
        return results

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
