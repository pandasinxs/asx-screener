#!/usr/bin/env python3
"""
backtest_intraday.py
=====================
15分钟颗粒度日内策略回测引擎 —— Stage 1（本轮）：真实还原watchlist的
"谁在哪几天被实际监测"这个状态机 + 逐日跨日健康度（health_status）回测。

背景与目的：
    backtest_engine.py里原有的HourlyIntradayApprox（60分钟线近似策略）
    已废弃（本轮决定）。本模块的目标是彻底不再"近似"——直接调用
    intraday_monitor.py里真实生产代码的检测函数（模式1-4），跑在
    真实历史15分钟数据（market_data_cache.py按周累积的Parquet缓存）
    上，让回测结果能直接回答"intraday_monitor.py现在这套设计好不好"，
    而不是回答一个近似策略的问题。

    本文件是Stage 1：只做"重建谁在哪几天被真实监测、每天的
    health_status是什么"这一层——这是Stage 2（真正跑模式1-4检测、
    模拟WIN/LOSS/TIMEOUT）的地基。Stage 1产出可以独立用SQL核对，
    验证过再往上叠Stage 2，避免一次性写完一个大而复杂的系统、
    出了问题不知道错在哪一层。

============================================================
本轮审查中发现的一个重要生产系统行为（必读，直接决定了下面的实现）
============================================================
    watchlist表的PRIMARY KEY是ticker，也就是说每只股票在这张表里
    终其一生只有一行记录。_upsert_core()的UPDATE分支（重选路径）
    只更新total_days/reselect_count/status/tier_level等字段，
    **从不touch days_elapsed**。days_elapsed只在
    run_end_of_day_maintenance()里对当前status='active'的股票逐日+1，
    status一旦变成'exited'，days_elapsed就冻结在当时的值，不会归零。

    结果：一只股票如果被反复重选（这次T3入选、隔几周又被T2选中、
    再隔几个月又入选……），它的days_elapsed是跨越这些重选事件
    累积计算的"这只股票有生之年被active监测过的总天数"，不是
    "这一轮监测的第几天"。total_days同理累积（每次重选都加，
    但封顶60）。

    隐藏行为：一旦某只股票累积的days_elapsed达到60（total_days的
    硬顶），即使之后再被重选、total_days尝试被延长也已经顶到60
    加不动了，get_active_watchlist()的WHERE子句(days_elapsed <
    total_days)会让它永久无法再真正被监测——status字段可能显示
    'active'，但intraday_monitor.py实际上再也不会处理它。这不是
    bug报告，只是如实记录这个真实存在的行为，backtest必须原样
    复刻才能匹配真实系统。

    另一处实现细节（写代码时对照原文自查发现，第一版草稿漏了）：
    真实run_end_of_day_maintenance()里wdb.increment_day_elapsed()是
    无条件执行的（不管当天健康度检查结果如何），健康度检查失败会
    直接exit_watchlist()并continue跳过天数耗尽判断——也就是说健康度
    不达标的退出原因优先于天数耗尽，但天数计数本身每个active交易日
    照常+1。process_ticker()据此实现：days_elapsed无条件+1，健康度
    检查失败时天数耗尽判断被跳过（不代表没有+1）。

    这个发现同时也值得你在生产系统层面知道（不属于这次backtest
    工作范围，这轮不改production代码）：如果"重选=重新给满60天"
    才是你想要的行为，那是watchlist_db.py的_upsert_core()需要改的
    另一个话题，跟这次回测无关，你决定要不要另开一轮处理。
============================================================

已实现（Stage 1）：
    - 从signals_history_backtest（某个已跑过的EOD param_set）重建
      "这只股票哪天第一次入选、之后哪几天被重选"的原始事件序列
      （market_cap_m IS NOT NULL做过滤，还原真实filtered_pool，
      不只是Top3——真实wdb.upsert_watchlist()对filtered_pool全体
      调用，跟backtest_engine.py同一套代码判定市值门槛）
    - 逐交易日回放真实的watchlist状态机（累积days_elapsed/
      total_days，重选延长、重选可撤销当天的健康度/天数耗尽退出
      判定、60天硬顶导致永久不再监测），产出"实际处于active监测
      状态"的连续区间（intraday_active_intervals_backtest）
    - 对每个active区间内的每个交易日，用backtest_engine.py已经
      验证过的DailyHealthEvaluator重新计算health_status（不含
      60分钟线精确化——同一天多算这个划不来，价值也小，已在此前
      的对话中明确接受这个简化），写入intraday_health_backtest，
      供Stage 2判断当天该跑哪条轨道（突破轨道/回调轨道/跳过）

尚未实现（Stage 2，等这一层验证过再做）：
    - 逐15分钟bar真正调用intraday_monitor.py的模式1-4检测函数
    - 模式1两阶段确认的状态机（回测专用持久化，不写生产watchlist.db）
    - WIN/LOSS/TIMEOUT出场模拟（含模式3专属的"次日开盘了结"规则，
      模式1/2/4止损公式在回测里复刻intraday_monitor.py.
      monitor_one_ticker()内联的公式，需要手动跟那边保持同步）

依赖:
    必须跟backtest_engine.py/market_data_cache.py/intraday_monitor.py/
    watchlist_db.py/screener.py同目录运行。

import intraday_monitor的副作用说明：
    intraday_monitor.py在模块顶层调用了logging.basicConfig(handlers=
    [...FileHandler("intraday_monitor.log")...])，import它会在当前
    工作目录下产生一个intraday_monitor.log文件（相对路径，不会像
    daily_analysis.py那样因为写死绝对路径导致FileNotFoundError崩溃，
    只是个无害的副作用）。本文件只调用intraday_monitor.py里不写库、
    不发Telegram的纯函数/常量（HEALTH_BELOW_MA50_GRACE_DAYS等），
    这些内部都没有调用log.*，不会有实际内容写进那个文件，但文件
    本身会被创建/追加一个空壳。可以忽略，或者在.gitignore里加一条。

用法:
    # 先跑一次Stage1（来源EOD回测的param_set必须已经用backtest_engine.py跑过）
    python3 backtest_intraday.py --eod-param-set baseline_v1_full_market \\
        --param-set-name intraday_v1_health_backtest

    # 查看Stage1统计（覆盖率/health_status分布/永久沉寂比例），
    # 不重跑，供动手写Stage2之前先核对这一层结果是否符合预期
    python3 backtest_intraday.py --stats-only \\
        --param-set-name intraday_v1_health_backtest
"""

import argparse
import logging
import os
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd
import requests

ASX_DIR = os.path.dirname(os.path.abspath(__file__))
if ASX_DIR not in sys.path:
    sys.path.insert(0, ASX_DIR)

try:
    import screener  # noqa: E402  防御性导入，backtest_engine内部也会import它，
                      # 这里提前失败能给出更明确的报错定位
except ImportError as e:
    print(f"无法 import screener: {e}")
    sys.exit(1)

try:
    import backtest_engine as bte  # noqa: E402  复用DataLayer/DailyHealthEvaluator/BacktestConfig
except ImportError as e:
    print(f"无法 import backtest_engine: {e}")
    sys.exit(1)

try:
    from watchlist_db import TIER_MONITOR_DAYS, MAX_MONITOR_DAYS  # noqa: E402
    # 只读常量，watchlist_db.py模块顶层不做任何I/O/logging.basicConfig，
    # import它本身没有副作用（跟intraday_monitor.py不同，见下方import说明）
except ImportError as e:
    print(f"无法 import watchlist_db: {e}")
    sys.exit(1)

try:
    import intraday_monitor as im  # noqa: E402  只用其中的纯常量，
                                     # 见模块docstring里的import副作用说明
except ImportError as e:
    print(f"无法 import intraday_monitor: {e}")
    sys.exit(1)


TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(text: str, logger: logging.Logger) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram未配置，跳过推送")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000],
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Telegram推送失败: {e}")


def setup_logging(log_path: str) -> logging.Logger:
    logger = logging.getLogger("backtest_intraday")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


@dataclass
class IntradayBacktestConfig:
    eod_param_set: str = ""       # 来源EOD回测的param_set（必须已经跑过）
    param_set: str = "intraday_baseline"  # 本次intraday回测自己的标签
    db_path: str = os.path.join(ASX_DIR, "backtest_results.db")
    log_path: str = os.path.join(ASX_DIR, "backtest_intraday.log")
    # 是否复刻intraday_monitor.py的check_health()（连续2日跌破MA50提前
    # 退出监测队列）。默认True——跳过这道门槛会系统性高估某些股票的
    # 监测天数覆盖范围，跟"要最真实有效"的目标相悖。
    implement_ma50_exit_gate: bool = True
    max_consecutive_errors: int = 5
    push_telegram: bool = True


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_watchlist_state_backtest (
    ticker                TEXT NOT NULL,
    param_set             TEXT NOT NULL,
    first_entry_date      TEXT,
    days_elapsed_final    INTEGER,
    total_days_final      INTEGER,
    reselect_count_final  INTEGER,
    permanently_dormant   INTEGER DEFAULT 0,
    run_timestamp         TEXT,
    PRIMARY KEY (ticker, param_set)
)
"""

INTERVALS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_active_intervals_backtest (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker         TEXT NOT NULL,
    interval_start TEXT NOT NULL,
    interval_end   TEXT,
    end_reason     TEXT,
    param_set      TEXT NOT NULL
)
"""

HEALTH_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_health_backtest (
    ticker        TEXT NOT NULL,
    trading_date  TEXT NOT NULL,
    health_status TEXT,
    param_set     TEXT NOT NULL,
    PRIMARY KEY (ticker, trading_date, param_set)
)
"""

PROGRESS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_backtest_progress (
    run_key TEXT NOT NULL,
    ticker  TEXT NOT NULL,
    PRIMARY KEY (run_key, ticker)
)
"""


def check_health_backtest(close_pit: Optional[pd.Series]) -> tuple:
    """
    对齐intraday_monitor.py的check_health()：连续
    HEALTH_BELOW_MA50_GRACE_DAYS(2)日跌破MA50则判定不健康。逻辑
    逐行复刻生产版本，数据源换成回测的point-in-time日线切片
    （close_pit已经截止到"as_of"这一天，不含未来数据）。
    """
    if close_pit is None or len(close_pit) < 50:
        return True, ""
    ma50 = close_pit.rolling(50).mean()
    below_streak = 0
    for i in range(1, im.HEALTH_BELOW_MA50_GRACE_DAYS + 1):
        if float(close_pit.iloc[-i]) < float(ma50.iloc[-i]):
            below_streak += 1
    if below_streak >= im.HEALTH_BELOW_MA50_GRACE_DAYS:
        return False, f"连续{im.HEALTH_BELOW_MA50_GRACE_DAYS}日跌破MA50"
    return True, ""


def load_eod_candidates(cfg: IntradayBacktestConfig, logger: logging.Logger) -> dict:
    """
    从signals_history_backtest读取某个EOD param_set的候选记录，还原
    "哪些股票真正会被wdb.upsert_watchlist()写入监测队列"——用
    market_cap_m IS NOT NULL做过滤：这个字段只在SignalGenerator.
    scan_day()里通过市值门槛的候选才会被设置（backtest_engine.py
    v2.2的filtered_pool逻辑），跟真实screener.py的filtered_pool判定
    条件完全一致（同一份代码，backtest_engine.py本来就是import
    screener复用）。

    返回 {ticker: {date_str: tier_level, ...}, ...}，每只股票内部按
    日期升序（依赖SQL的ORDER BY，dict在Python 3.7+保证插入顺序）。
    """
    if not os.path.exists(cfg.db_path):
        logger.error(f"数据库不存在: {cfg.db_path}")
        return {}
    conn = sqlite3.connect(cfg.db_path)
    try:
        df = pd.read_sql_query("""
            SELECT ticker, signal_date, tier_level
            FROM signals_history_backtest
            WHERE param_set = ? AND market_cap_m IS NOT NULL
            ORDER BY ticker, signal_date
        """, conn, params=[cfg.eod_param_set])
    except Exception as e:
        logger.error(f"读取EOD候选失败: {e}")
        conn.close()
        return {}
    conn.close()

    if df.empty:
        logger.error(
            f"param_set={cfg.eod_param_set} 在signals_history_backtest里"
            f"没有market_cap_m非空的记录——检查param_set名字是否正确，"
            f"或者这个EOD回测是否真的跑过"
        )
        return {}

    result: dict = {}
    for ticker, g in df.groupby("ticker"):
        result[ticker] = dict(zip(g["signal_date"], g["tier_level"]))
    logger.info(f"从EOD候选池({cfg.eod_param_set})还原出 {len(result)} 只股票的入选事件序列")
    return result


def _already_done(conn: sqlite3.Connection, run_key: str, ticker: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM intraday_backtest_progress WHERE run_key = ? AND ticker = ?",
        (run_key, ticker),
    ).fetchone()
    return row is not None


def _mark_done(conn: sqlite3.Connection, run_key: str, ticker: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO intraday_backtest_progress (run_key, ticker) VALUES (?, ?)",
        (run_key, ticker),
    )


def process_ticker(ticker: str, appearances: dict, data_layer: "bte.DataLayer",
                    health_eval: "bte.DailyHealthEvaluator",
                    cfg: IntradayBacktestConfig, logger: logging.Logger,
                    master_trading_days: list) -> Optional[dict]:
    """
    对单只股票，回放真实watchlist单条记录（PRIMARY KEY=ticker）的
    完整生命周期状态机。见模块docstring顶部"重要生产系统行为"一节
    ——days_elapsed/total_days跨重选累积、不重置，重选可撤销当天的
    退出判定，累积到60天硬顶后永久不再被监测；健康度不达标的退出
    优先于天数耗尽，但days_elapsed本身每个active交易日无条件+1。

    返回None表示数据不足（跳过，不算错误，属于数据覆盖边界）。
    """
    appearance_dates = sorted(appearances.keys())
    first_entry = appearance_dates[0]

    download_start = (pd.Timestamp(first_entry) - pd.Timedelta(days=400)).date().isoformat()
    today_str = date.today().isoformat()

    daily_df = data_layer.fetch(ticker, download_start, today_str)
    if daily_df is None:
        logger.debug(f"[{ticker}] 日线数据不足，跳过（数据覆盖边界，非错误）")
        return None
    close_s = daily_df["Close"].squeeze()

    trading_days = [d for d in master_trading_days if str(d.date()) >= first_entry]
    if not trading_days:
        return None

    appearance_set = set(appearance_dates)

    days_elapsed = 0
    total_days = TIER_MONITOR_DAYS.get(appearances[first_entry], 7)
    reselect_count = 0
    status_active = True
    permanently_dormant = False
    interval_start = first_entry

    intervals: list = []
    health_rows: list = []

    for day in trading_days:
        day_str = str(day.date())

        if status_active:
            pit_df = daily_df[daily_df.index <= day]
            try:
                health_result = health_eval.evaluate(pit_df, day, hourly_df=None)
                health_status = health_result.get("health_status")
            except Exception as e:
                logger.warning(f"健康度回测异常 [{ticker}] {day_str}: {e}")
                health_status = None
            health_rows.append((ticker, day_str, health_status, cfg.param_set))

        if day_str == first_entry:
            # entry当天：真实系统里，这只股票是screener.py当天16:30才
            # 被写进watchlist的，晚于intraday_monitor.py同一天15:45的
            # EOD维护，entry当天不会被health/天数耗尽判断，从下一个
            # 交易日起才开始计入
            continue

        tentative_exit_reason = None
        if status_active:
            days_elapsed += 1  # 无条件+1，对齐真实wdb.increment_day_elapsed()
            if cfg.implement_ma50_exit_gate:
                close_pit = close_s[close_s.index <= day]
                healthy, reason = check_health_backtest(close_pit)
                if not healthy:
                    tentative_exit_reason = f"健康度不达标：{reason}"
            # 健康度不达标优先于天数耗尽——跟真实run_end_of_day_maintenance()
            # 里"if not healthy: exit; continue"（跳过天数耗尽判断）一致
            if tentative_exit_reason is None and days_elapsed >= total_days:
                tentative_exit_reason = "监测天数耗尽"

        reselected_today = (day_str in appearance_set) and (day_str != first_entry)
        if reselected_today and not permanently_dormant:
            new_tier = appearances[day_str]
            add_days = TIER_MONITOR_DAYS.get(new_tier, 7)
            total_days = min(total_days + add_days, MAX_MONITOR_DAYS)
            reselect_count += 1
            if days_elapsed < total_days:
                if not status_active:
                    interval_start = day_str  # 从exited状态重新激活，开启新区间
                status_active = True
                tentative_exit_reason = None  # 重选撤销今天产生的退出判定
            else:
                permanently_dormant = True  # 60天硬顶已耗尽，重选也救不回来

        if status_active and tentative_exit_reason is not None:
            intervals.append((ticker, interval_start, day_str, tentative_exit_reason, cfg.param_set))
            status_active = False

        if total_days >= MAX_MONITOR_DAYS and days_elapsed >= MAX_MONITOR_DAYS:
            permanently_dormant = True

    if status_active:
        intervals.append((ticker, interval_start, None,
                          "数据覆盖末尾截断（区间尚未真正结束）", cfg.param_set))

    return {
        "intervals": intervals,
        "health_rows": health_rows,
        "first_entry_date": first_entry,
        "days_elapsed_final": days_elapsed,
        "total_days_final": total_days,
        "reselect_count_final": reselect_count,
        "permanently_dormant": permanently_dormant,
    }


def run(cfg: IntradayBacktestConfig, max_minutes: Optional[float] = None) -> str:
    logger = setup_logging(cfg.log_path)
    logger.info(
        f"=== backtest_intraday.py Stage1 启动 [{cfg.param_set}] "
        f"来源EOD={cfg.eod_param_set} "
        f"MA50早退门槛={'开启' if cfg.implement_ma50_exit_gate else '关闭'} ==="
    )

    candidates = load_eod_candidates(cfg, logger)
    if not candidates:
        msg = f"🔴 backtest_intraday Stage1终止 [{cfg.param_set}]：EOD候选池为空"
        logger.error(msg)
        if cfg.push_telegram:
            send_telegram(msg, logger)
        return msg

    data_layer = bte.DataLayer(logger)
    health_cfg = bte.BacktestConfig(use_hourly_vol_ratio=False)
    health_eval = bte.DailyHealthEvaluator(health_cfg, logger)

    earliest = min(min(d.keys()) for d in candidates.values())
    xjo_start = (pd.Timestamp(earliest) - pd.Timedelta(days=400)).date().isoformat()
    today_str = date.today().isoformat()
    xjo_df = data_layer.fetch("^AXJO", xjo_start, today_str)
    if xjo_df is None:
        msg = "🔴 backtest_intraday Stage1终止：无法获取ASX200基准日线，没有交易日历可用"
        logger.error(msg)
        if cfg.push_telegram:
            send_telegram(msg, logger)
        return msg
    master_trading_days = list(xjo_df.index)
    logger.info(
        f"交易日历：{len(master_trading_days)}个交易日"
        f"（{str(master_trading_days[0].date())} ~ {str(master_trading_days[-1].date())}）"
    )

    conn = sqlite3.connect(cfg.db_path)
    conn.execute(SCHEMA_SQL)
    conn.execute(INTERVALS_SCHEMA_SQL)
    conn.execute(HEALTH_SCHEMA_SQL)
    conn.execute(PROGRESS_SCHEMA_SQL)
    conn.commit()

    run_key = f"{cfg.eod_param_set}|{cfg.param_set}|ma50gate={cfg.implement_ma50_exit_gate}"

    tickers = sorted(candidates.keys())
    remaining = [t for t in tickers if not _already_done(conn, run_key, t)]
    if len(remaining) < len(tickers):
        logger.info(
            f"检测到断点：{len(tickers) - len(remaining)}只已完成，"
            f"本次继续剩余{len(remaining)}只"
        )

    processed = 0
    skipped_no_data = 0
    consecutive_errors = 0
    circuit_broken = False
    start_time = time.time()

    for ticker in remaining:
        if max_minutes is not None and (time.time() - start_time) / 60 >= max_minutes:
            logger.info(
                f"达到时间预算({max_minutes}分钟)，提前结束。"
                f"已处理{processed}/{len(remaining)}只，下次原样重跑会自动续上"
            )
            break

        try:
            result = process_ticker(ticker, candidates[ticker], data_layer,
                                    health_eval, cfg, logger, master_trading_days)
        except Exception as e:
            consecutive_errors += 1
            logger.error(
                f"处理异常 [{ticker}]（连续失败{consecutive_errors}/"
                f"{cfg.max_consecutive_errors}）: {e}\n{traceback.format_exc()}"
            )
            if consecutive_errors >= cfg.max_consecutive_errors:
                alert = (
                    f"🔴 backtest_intraday熔断 [{cfg.param_set}]\n"
                    f"连续{consecutive_errors}只股票处理异常，最新错误: {e}\n"
                    f"已提前停止，未标记完成的股票下次会自动重试"
                )
                logger.critical(alert)
                if cfg.push_telegram:
                    send_telegram(alert, logger)
                circuit_broken = True
                break
            continue

        consecutive_errors = 0

        if result is None:
            skipped_no_data += 1
            _mark_done(conn, run_key, ticker)
            conn.commit()
            processed += 1
            continue

        if result["intervals"]:
            conn.executemany("""
                INSERT INTO intraday_active_intervals_backtest
                    (ticker, interval_start, interval_end, end_reason, param_set)
                VALUES (?, ?, ?, ?, ?)
            """, result["intervals"])
        if result["health_rows"]:
            conn.executemany("""
                INSERT OR REPLACE INTO intraday_health_backtest
                    (ticker, trading_date, health_status, param_set)
                VALUES (?, ?, ?, ?)
            """, result["health_rows"])

        conn.execute("""
            INSERT OR REPLACE INTO intraday_watchlist_state_backtest
                (ticker, param_set, first_entry_date, days_elapsed_final,
                 total_days_final, reselect_count_final, permanently_dormant,
                 run_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, cfg.param_set, result["first_entry_date"],
              result["days_elapsed_final"], result["total_days_final"],
              result["reselect_count_final"],
              1 if result["permanently_dormant"] else 0,
              pd.Timestamp.now().isoformat()))

        _mark_done(conn, run_key, ticker)
        conn.commit()
        processed += 1

        if processed % 50 == 0:
            elapsed = time.time() - start_time
            logger.info(f"进度 {processed}/{len(remaining)}，已用{elapsed/60:.1f}分钟")

    conn.close()

    total_done = len(tickers) - len(remaining) + processed
    status_line = (
        "🛑 因熔断提前停止" if circuit_broken else
        ("✅ 全部完成" if total_done >= len(tickers) else "⏸ 已按时间预算暂停")
    )
    summary = (
        f"{status_line} backtest_intraday Stage1 [{cfg.param_set}]\n"
        f"本次处理: {processed}只（数据不足跳过{skipped_no_data}只）\n"
        f"累计总进度: {total_done}/{len(tickers)}只"
        + (f"，下次重跑相同参数会自动续上" if total_done < len(tickers) else "")
    )
    logger.info("=== " + summary.replace("\n", " | ") + " ===")
    if cfg.push_telegram:
        send_telegram(summary, logger)
    return summary


def show_stats(cfg: IntradayBacktestConfig) -> None:
    """
    --stats-only：不重跑，只汇总已经产出的Stage1数据，供你在开始写
    Stage2之前先核对这一层结果是否符合预期（覆盖率、health_status
    分布、有多少股票撞到60天永久沉寂上限等）。
    """
    if not os.path.exists(cfg.db_path):
        print(f"数据库不存在: {cfg.db_path}")
        return
    conn = sqlite3.connect(cfg.db_path)

    state_df = pd.read_sql_query(
        "SELECT * FROM intraday_watchlist_state_backtest WHERE param_set = ?",
        conn, params=[cfg.param_set],
    )
    if state_df.empty:
        print(f"param_set={cfg.param_set} 暂无Stage1结果，先跑一次不带--stats-only的命令")
        conn.close()
        return

    print("=" * 60)
    print(f"Stage1统计 [param_set={cfg.param_set}]")
    print("=" * 60)
    print(f"股票总数: {len(state_df)}")
    print(
        f"永久沉寂（累积监测天数撞到60天硬顶）: "
        f"{int(state_df['permanently_dormant'].sum())}只 "
        f"({state_df['permanently_dormant'].mean()*100:.1f}%)"
    )
    print(f"平均累积监测天数(days_elapsed_final): {state_df['days_elapsed_final'].mean():.1f}")
    print(f"平均重选次数(reselect_count_final): {state_df['reselect_count_final'].mean():.1f}")
    print(
        f"重选过至少一次的股票: "
        f"{(state_df['reselect_count_final'] > 0).sum()}只 "
        f"({(state_df['reselect_count_final'] > 0).mean()*100:.1f}%)"
    )

    intervals_df = pd.read_sql_query(
        "SELECT * FROM intraday_active_intervals_backtest WHERE param_set = ?",
        conn, params=[cfg.param_set],
    )
    print(f"\nactive监测区间总数: {len(intervals_df)}（含数据末尾截断未结束的区间）")
    if not intervals_df.empty:
        print("退出原因分布：")
        for reason, cnt in intervals_df["end_reason"].value_counts().items():
            print(f"  {reason}: {cnt}")

    health_df = pd.read_sql_query(
        "SELECT health_status, COUNT(*) as n FROM intraday_health_backtest "
        "WHERE param_set = ? GROUP BY health_status",
        conn, params=[cfg.param_set],
    )
    print(f"\nhealth_status分布（跨所有active监测日）：")
    total_health = health_df["n"].sum()
    for _, row in health_df.sort_values("n", ascending=False).iterrows():
        pct = row["n"] / total_health * 100 if total_health else 0
        print(f"  {row['health_status']}: {row['n']}天 ({pct:.1f}%)")

    if total_health:
        ready_pullback = health_df[
            health_df["health_status"].isin(["ready", "pullback_bottoming"])
        ]["n"].sum()
        print(
            f"\n其中ready/pullback_bottoming"
            f"（Stage2会真正跑15分钟检测的天数）: "
            f"{ready_pullback}天 ({ready_pullback/total_health*100:.1f}%)"
        )

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="15分钟颗粒度日内策略回测 Stage1：watchlist状态机+健康度回测"
    )
    parser.add_argument("--eod-param-set", default="",
                        help="来源EOD回测的param_set（必须已经跑过backtest_engine.py）")
    parser.add_argument("--param-set-name", default="intraday_baseline",
                        help="本次intraday回测的标签")
    parser.add_argument("--disable-ma50-gate", action="store_true",
                        help="关闭MA50提前退出门槛的复刻（默认开启，是标准方法论的一部分）")
    parser.add_argument("--max-minutes", type=float, default=None)
    parser.add_argument("--stats-only", action="store_true", help="不重跑，只汇总已有Stage1结果")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    cfg = IntradayBacktestConfig(
        eod_param_set=args.eod_param_set,
        param_set=args.param_set_name,
        implement_ma50_exit_gate=not args.disable_ma50_gate,
        push_telegram=not args.no_telegram,
    )

    if args.stats_only:
        show_stats(cfg)
        return

    if not cfg.eod_param_set:
        print(
            "必须指定 --eod-param-set（来源EOD回测的param_set名字，"
            "用 python3 backtest_engine.py --stats-only --leaderboard 查看有哪些）"
        )
        return

    try:
        run(cfg, max_minutes=args.max_minutes)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"顶层未捕获异常: {e}\n{tb}")
        if cfg.push_telegram:
            send_telegram(
                f"🔴 backtest_intraday.py 进程崩溃\n参数集: {cfg.param_set}\n"
                f"错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                logging.getLogger("backtest_intraday"),
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
