#!/usr/bin/env python3
"""
backtest_intraday.py
=====================
15分钟颗粒度日内策略回测引擎。直接调用intraday_monitor.py里真实生产
代码的检测函数（模式1-4），跑在真实历史15分钟数据（market_data_
cache.py按周累积的Parquet缓存）上——不是近似策略；backtest_engine.py
里原有的HourlyIntradayApprox（60分钟线近似）已废弃（v2.3起默认停用）。

分两个阶段，串联方式见下方"运行方式"：

  Stage 1（watchlist状态机 + 跨日健康度）
      重建"谁在哪几天被真实监测、每天health_status是什么"。产出可以
      独立用SQL核对，是Stage 2的地基。

  Stage 2（真实15分钟信号检测 + 出场模拟）
      对Stage 1产出的ready/pullback_bottoming候选日，逐日逐bar真实
      调用intraday_monitor.py的模式1-4检测函数，触发信号后模拟
      WIN/LOSS/TIMEOUT出场。

============================================================
运行方式
============================================================
    A. 全新的EOD param_set，第一次端到端测试：
        python3 backtest_intraday.py --run-all \\
            --eod-param-set my_experiment_v1 \\
            --param-set-name my_experiment_v1_intraday

       一条命令顺序跑完Stage1和Stage2。Stage1在--max-minutes预算内
       没跑完就不会进入Stage2，原样重跑同一条命令会自动从断点续上，
       Stage1真正"全部完成"的那次才会接着自动跑Stage2。Stage2自己
       的param_set会自动取"<--param-set-name>_stage2"。

    B. 只重跑Stage2（不动Stage1的结果）：
       典型场景是本地15分钟数据覆盖率随data_fetcher.py每周累积后，
       隔一段时间想用更多数据重新评估同一批候选：
        python3 backtest_intraday.py --run-stage2 \\
            --stage1-param-set my_experiment_v1_intraday \\
            --param-set-name my_experiment_v1_intraday_recheck_1

       ⚠️ --param-set-name必须换一个新名字，不能复用上次Stage2用过
       的名字——intraday_stage2_progress按(run_key, ticker)记录断点，
       run_key里包含这个名字，复用旧名字会导致上次因为没有15分钟
       数据被跳过的股票被直接当作"已完成"，不会因为数据变多了而
       重新尝试。换新名字后旧结果不会丢（按param_set区分，新旧两次
       的信号分别保留在intraday_signals_backtest里，可以对比着看）。

    C. 分步跑（等价于A，拆成两条命令，方便Stage1跑完先自己看一眼
       --stats-only再决定要不要继续）：
        python3 backtest_intraday.py --eod-param-set my_experiment_v1 \\
            --param-set-name my_experiment_v1_intraday
        python3 backtest_intraday.py --stats-only \\
            --param-set-name my_experiment_v1_intraday
        python3 backtest_intraday.py --run-stage2 \\
            --stage1-param-set my_experiment_v1_intraday \\
            --param-set-name my_experiment_v1_stage2
        python3 backtest_intraday.py --run-stage2 --stats-only \\
            --param-set-name my_experiment_v1_stage2

============================================================
必须原样复刻的生产系统行为（改代码前务必先读）
============================================================
    1. watchlist表PRIMARY KEY是ticker，每只股票终生只有一行记录。
       重选（_upsert_core()的UPDATE分支）只更新total_days/
       reselect_count/status/tier_level，不touch days_elapsed。
       days_elapsed只在run_end_of_day_maintenance()里对active股票
       逐日+1，exited后冻结，不归零——一只股票反复被重选，
       days_elapsed是跨重选事件累积的"有生之年被监测总天数"，不是
       "这一轮第几天"。累积到60天硬顶后即使再被重选也无法解除，
       status可能显示active，但实际上永远不会再被处理
       （permanently_dormant）。这不是bug，是真实生产行为，
       process_ticker()原样复刻。是否要改成"重选=重新计满60天"是
       production层面watchlist_db.py的话题，跟这次回测无关。

    2. 健康度检查失败的退出优先于天数耗尽，但days_elapsed本身每个
       active交易日无条件+1（对齐wdb.increment_day_elapsed()）——
       process_ticker()据此实现。

    3. DAILY_HEALTH参数自动从EOD实验的experiment_metadata同步
       （load_experiment_overrides()），不需要（也不应该）手动再传
       --params-file——避免同一只股票同一个param_set下，不同日期
       用了两套不同阈值算出health_status这种口径不一致。

    4. Stage1健康度评估用60分钟线精确化"尾盘时段量比"，对齐
       backtest_engine.py的标准方法论（use_hourly_vol_ratio=True，
       baseline_v1_full_market等正式EOD回测都是在这个精度下跑的，
       不是可选项）。process_ticker()为每只股票只拉一次60分钟线
       （跟daily_df一样，不是每天单独请求），逐日point-in-time切片
       喂给DailyHealthEvaluator；覆盖不到的日期（60分钟颗粒度约729天
       硬上限）自动回退到全天成交量比例代理，不报错、不跳过整只股票。
       早期版本这里传的是hourly_df=None，Stage1候选池的量能判断口径
       比正规EOD回测更粗糙，两边不完全一致——已对齐，见文件末尾
       CHANGELOG。

    5. 模式1两阶段确认：疑似突破先持久化到pending_breakout（回测内
       存，不写生产watchlist.db），下一次轮询判断confirmed/failed，
       超过MODE1_CONFIRM_MAX_BARS_WAIT根K线未决出也清除。
       pending_breakout限定同一交易日有效，process_ticker_stage2()
       在进入每个候选交易日前会先检查并清除跨天残留的状态（否则
       疑似突破一旦发生在交易日最后一两次轮询、当天剩余轮询次数不够
       confirm/fail，这只股票的模式1会永久失效——已修复，见文件末尾
       CHANGELOG）。

    6. "今天是否已出过信号"的判断，模式1和模式2/3不是同一套逻辑：
       模式1检查"今天有没有任何模式已发过信号"
       （already_signaled_today），模式2/3检查"上一次记录的信号
       是不是同一个模式"（last_signal_mode）。结果是模式1早上触发
       过，模式3下午仍可能独立触发——生产代码本来的行为，原样复刻，
       不做"一天只能一个信号"的简化。

    7. 所有基准位（prior_high/avg_vol_20d/ATR14/回调参考/区间最大
       回撤）都用"不含当天"的历史数据（daily_df.index < day），跟
       lock_daily_reference()的point-in-time行为逐行对齐，避免
       未来函数。

    8. 出场模拟不预设2倍还是3倍风险止盈更合理，两个都独立追踪、
       都记下来——生产的monitor_one_ticker()本身也是同时展示两条
       参考线，没有替交易者选定"自动止盈在哪"。模式3（尾盘确认买）
       例外：T+1次日开盘价固定了结，不套用止损/止盈框架，对齐生产
       "不追、次日附近了结"的设计意图。

    9. 止损公式（compute_stop_loss_stage2()）是本文件里唯一没有
       直接import intraday_monitor.py代码、而是手动复刻的部分——
       因为原公式写在monitor_one_ticker()函数体内部，不是独立函数，
       没法直接复用。以后改monitor_one_ticker()里这几个止损公式，
       这里必须手动跟着改。

============================================================
已知限制（不是这个阶段要解决的，记录在案）
============================================================
    - 断点续跑粒度是"整只股票"，不做"待重试/等数据够了自动补上"这
      套状态机——已经明确决定用"换新param_set名字重跑"代替，见上方
      "运行方式B"
    - commission_pct/slippage_bps在BacktestConfig里定义了但出场模拟
      没有使用，当前是完美成交假设
    - 没有账户级别资金曲线模拟（仓位重叠、同一时间多信号占用资金
      不建模），每个信号独立算胜率
    - intraday_monitor.py模式1-4的常量（VOL_SPIKE_RATIO_M1/
      MODE2_PULLBACK_MAX_DEPTH_PCT等）不接入params覆盖系统，只能
      回答"现在这套设计好不好"，回答不了"哪组参数更好"
    - Stage1为对齐EOD标准方法论新增了60分钟线拉取（见上方生产行为
      第4点），对~500只股票、每只覆盖其完整监测窗口，比之前多一层
      数据获取成本；本地若已有backtest_engine.py之前跑EOD实验时
      顺手缓存下来的60分钟线Parquet（EOD回测对Top10候选每天都会拉
      60分钟线），Stage1这里很可能命中不少缓存，但没有实测验证过
      命中率，不确定这个假设的准确性

============================================================
依赖 & import副作用
============================================================
    必须跟backtest_engine.py/market_data_cache.py/intraday_monitor.py/
    watchlist_db.py/screener.py同目录运行。import intraday_monitor
    会在当前工作目录下产生一个无害的空intraday_monitor.log文件（该
    模块顶层有logging.basicConfig，本文件只用它的纯函数/常量，不会
    有内容真正写进那个文件）。

============================================================
CHANGELOG
============================================================
    - Stage1/Stage2的run()/run_stage2()新增启动时的Telegram推送
      （🚀 ...启动），此前只有完成/熔断/崩溃才推送，中途不知道
      有没有真的跑起来。对齐backtest_engine.py的BacktestEngine.run()
      已有的做法。中途出错的两层覆盖（单只股票连续失败达到
      max_consecutive_errors触发熔断推送；任何逃过这层的意外崩溃
      冒泡到main()顶层try/except推送）本来就存在，不是这次新增的。
    - Stage1健康度评估接入60分钟线精确化（use_hourly_vol_ratio=True，
      process_ticker()为每只股票拉取60分钟线并逐日point-in-time切片），
      对齐backtest_engine.py的EOD标准方法论。此前Stage1这里固定传
      hourly_df=None，候选池的量能判断口径比正规EOD回测更粗糙。
      ⚠️ 这个改动会让同一只股票同一天的health_status可能跟旧版本算
      出来的不一样（不是bug，是精度提升）——用旧的Stage1 param_set
      名字重新运行不会触发重算：run_key只由eod_param_set/param_set/
      ma50gate三者组成，这三者都没变的话intraday_backtest_progress
      会把所有股票当成"已完成"直接跳过，磁盘上还是旧的粗糙结果。
      必须用一个全新的--param-set-name才能让这次精度提升真正生效。
    - process_ticker_stage2()：pending_breakout状态补上跨天过期
      清零（见上方生产行为第5点）
    - main()新增--run-all：Stage1+Stage2一条命令跑完，Stage1未在
      本次--max-minutes预算内完成不会自动进入Stage2
    - 断点续跑改用统一的_progress_done()/_progress_mark_done()（原来
      Stage1/Stage2各自一份几乎相同的代码，表结构完全一致，去重合并）
"""

import argparse
import json
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
    import backtest_engine as bte  # noqa: E402  复用DataLayer/DailyHealthEvaluator/
                                     # BacktestConfig/apply_param_overrides/
                                     # reset_screener_to_defaults
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
    eod_param_set: str = ""       # Stage1专用：来源EOD回测的param_set（必须已经跑过）
    stage1_param_set: str = ""    # Stage2专用：来源Stage1的--param-set-name
    param_set: str = "intraday_baseline"  # 本次运行（Stage1或Stage2）自己的标签
    db_path: str = os.path.join(ASX_DIR, "backtest_results.db")
    log_path: str = os.path.join(ASX_DIR, "backtest_intraday.log")
    # 是否复刻intraday_monitor.py的check_health()（连续2日跌破MA50提前
    # 退出监测队列）。默认True——跳过这道门槛会系统性高估某些股票的
    # 监测天数覆盖范围，跟"要最真实有效"的目标相悖。仅Stage1使用。
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


def load_experiment_overrides(db_path: str, param_set: str, logger: logging.Logger) -> dict:
    """
    从experiment_metadata表读取eod_param_set当时实际用的完整参数内容
    （backtest_engine.py每次跑新param_set都会自动记录这个），返回可以
    直接传给bte.apply_param_overrides()的overrides字典。

    为什么不让调用方单独传--params-file：signals_history_backtest里
    少数几天（raw_top10命中那几天）的health_status，是用EOD跑那一刻
    实际生效的DAILY_HEALTH参数算出来的；本文件还要给同一只股票补算
    "监测窗口内、但当天不在Top10"的其余交易日的health_status（Plan B
    核心工作），如果这里用的DAILY_HEALTH参数跟EOD跑那次不一致（比如
    手滑传了个不同的params.json，或者忘了传），会导致同一只股票、
    同一个param_set下，不同日期的health_status用了两套不同的阈值
    算出来——从experiment_metadata自动读取，从根源上排除这种人为
    对不齐的可能，你不需要记得"这个param_set当时用的是哪份params.json"。

    返回空字典的两种正常情况（不是错误，调用方会自然回退到默认参数）：
      - 这个param_set是baseline（EOD跑的时候没传--params-file），
        params_json存的是占位字符串"(baseline，未传--params-file)"，
        不是合法JSON，属于预期
      - experiment_metadata表里没有这个param_set的记录（比如这张表
        上线之前就已经跑过的很旧的实验）
    两种情况都应该用screener.py/BacktestConfig的默认值，这本身就是
    正确行为。
    """
    if not os.path.exists(db_path):
        return {}
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT params_json FROM experiment_metadata WHERE param_set = ?",
            (param_set,),
        ).fetchone()
    except Exception as e:
        logger.warning(f"读取experiment_metadata失败（将使用默认DAILY_HEALTH参数）: {e}")
        conn.close()
        return {}
    conn.close()

    if not row or not row[0]:
        logger.info(f"experiment_metadata里没有param_set={param_set}的记录，"
                     f"使用默认DAILY_HEALTH参数")
        return {}
    try:
        overrides = json.loads(row[0])
        if not isinstance(overrides, dict):
            return {}
        return overrides
    except (json.JSONDecodeError, TypeError):
        # baseline的占位字符串解析失败，属于预期情况
        logger.info(f"param_set={param_set}是baseline（未使用过--params-file），"
                     f"使用默认DAILY_HEALTH参数")
        return {}


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


def _progress_done(conn: sqlite3.Connection, table: str, run_key: str, ticker: str) -> bool:
    """Stage1(intraday_backtest_progress)/Stage2(intraday_stage2_progress)共用的
    断点续跑查询——两张表结构完全一致(run_key, ticker)，只是分属不同阶段，
    没必要各写一份重复的查询/写入函数。table只在代码内部以硬编码字面量
    传入，不接受外部输入，没有拼接风险。"""
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE run_key = ? AND ticker = ?",
        (run_key, ticker),
    ).fetchone()
    return row is not None


def _progress_mark_done(conn: sqlite3.Connection, table: str, run_key: str, ticker: str) -> None:
    conn.execute(
        f"INSERT OR IGNORE INTO {table} (run_key, ticker) VALUES (?, ?)",
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

    # 健康度精确化，对齐EOD标准方法论：backtest_engine.py的
    # DailyHealthEvaluator默认用60分钟线精确化"尾盘时段量比"这个近似
    # 字段（use_hourly_vol_ratio=True是"标准测试方法论的一部分"，不是
    # 可选项，baseline_v1_full_market等正式EOD回测都是在这个精度下
    # 跑的）。跟daily_df一样，只在这里拉一次（不是每个交易日单独发
    # 请求），下面逐日point-in-time切片复用。60分钟颗粒度yfinance
    # 约729天硬上限，覆盖不到的日期（比如first_entry很早、超出这个
    # 窗口）由DailyHealthEvaluator.evaluate()内部自动回退到全天成交量
    # 比例代理，不会报错、不会跳过整只股票——市值/数据获取层面的
    # 截断处理已经在market_data_cache内部统一做了，这里不重复判断。
    hourly_df = data_layer.fetch_60m(ticker, download_start, today_str)

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
            hourly_pit = (hourly_df[hourly_df.index.normalize() <= day]
                          if hourly_df is not None else None)
            try:
                health_result = health_eval.evaluate(pit_df, day, hourly_df=hourly_pit)
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


def run(cfg: IntradayBacktestConfig, max_minutes: Optional[float] = None) -> tuple:
    """运行Stage1。返回(summary: str, completed: bool)——completed=True表示
    这批股票在本次调用里已经全部处理完（不含熔断/时间预算用完的情况），
    main()的--run-all逻辑用这个布尔值判断能不能接着自动跑Stage2。"""
    logger = setup_logging(cfg.log_path)
    logger.info(
        f"=== backtest_intraday.py Stage1 启动 [{cfg.param_set}] "
        f"来源EOD={cfg.eod_param_set} "
        f"MA50早退门槛={'开启' if cfg.implement_ma50_exit_gate else '关闭'} "
        f"60分钟线健康度精确化=开启（对齐EOD标准方法论） ==="
    )

    candidates = load_eod_candidates(cfg, logger)
    if not candidates:
        msg = f"🔴 backtest_intraday Stage1终止 [{cfg.param_set}]：EOD候选池为空"
        logger.error(msg)
        if cfg.push_telegram:
            send_telegram(msg, logger)
        return msg, False

    start_msg = (
        f"🚀 backtest_intraday Stage1启动 [{cfg.param_set}]\n"
        f"来源EOD: {cfg.eod_param_set}\n"
        f"候选股票: {len(candidates)}只\n"
        f"MA50早退门槛: {'开启' if cfg.implement_ma50_exit_gate else '关闭'}\n"
        f"60分钟线健康度精确化: 开启\n"
        f"时间预算: {max_minutes if max_minutes else '不限'}分钟"
    )
    logger.info(start_msg.replace("\n", " | "))
    if cfg.push_telegram:
        send_telegram(start_msg, logger)

    data_layer = bte.DataLayer(logger)

    # ── 健康度参数自动跟EOD实验对齐（本轮修复）：不要求手动再传一次
    # --params-file，从experiment_metadata自动读取--eod-param-set
    # 当时实际用的DAILY_HEALTH参数，确保同一只股票在同一个param_set下
    # 不会出现"部分天数用A组阈值、部分天数用B组阈值"这种口径不一致。
    # bte.reset_screener_to_defaults()先重置，避免同进程内如果之前
    # 调用过别的实验残留了screener全局状态（虽然Stage1本身不用
    # screener的这些全局值，但apply_param_overrides()会顺带mutate它们，
    # 保持这个复位习惯跟backtest_engine.py自己的run_queue()一致）──
    bte.reset_screener_to_defaults()
    # use_hourly_vol_ratio=True：对齐EOD标准方法论（backtest_engine.py
    # 里这是"标准测试方法论的一部分，默认开启"，baseline_v1_full_market
    # 等正式EOD回测都在这个精度下跑）。这个开关本身只控制
    # DailyHealthEvaluator.evaluate()"要不要尝试用传入的hourly_df覆盖
    # 全天成交量比例代理"——真正的60分钟线获取+逐日point-in-time切片
    # 在process_ticker()里做（每只股票只拉一次，不是每天单独请求）。
    # 此前这里传的是False，Stage1健康度用的是比正规EOD回测更粗糙的
    # 量能代理，两边候选池的口径不一致；现已对齐，见文件头CHANGELOG。
    health_cfg = bte.BacktestConfig(use_hourly_vol_ratio=True)
    exp_overrides = load_experiment_overrides(cfg.db_path, cfg.eod_param_set, logger)
    if exp_overrides:
        bte.apply_param_overrides(exp_overrides, health_cfg, logger)
        logger.info(
            f"已从EOD实验[{cfg.eod_param_set}]的experiment_metadata自动同步"
            f"DAILY_HEALTH参数，确保健康度计算口径跟那次EOD跑的一致"
        )
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
        return msg, False
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
    remaining = [t for t in tickers
                 if not _progress_done(conn, "intraday_backtest_progress", run_key, t)]
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
            _progress_mark_done(conn, "intraday_backtest_progress", run_key, ticker)
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

        _progress_mark_done(conn, "intraday_backtest_progress", run_key, ticker)
        conn.commit()
        processed += 1

        if processed % 50 == 0:
            elapsed = time.time() - start_time
            logger.info(f"进度 {processed}/{len(remaining)}，已用{elapsed/60:.1f}分钟")

    conn.close()

    total_done = len(tickers) - len(remaining) + processed
    completed = (not circuit_broken) and (total_done >= len(tickers))
    status_line = (
        "🛑 因熔断提前停止" if circuit_broken else
        ("✅ 全部完成" if completed else "⏸ 已按时间预算暂停")
    )
    summary = (
        f"{status_line} backtest_intraday Stage1 [{cfg.param_set}]\n"
        f"本次处理: {processed}只（数据不足跳过{skipped_no_data}只）\n"
        f"累计总进度: {total_done}/{len(tickers)}只"
        + (f"，下次重跑相同参数会自动续上" if not completed else "")
    )
    logger.info("=== " + summary.replace("\n", " | ") + " ===")
    if cfg.push_telegram:
        send_telegram(summary, logger)
    return summary, completed


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


INTRADAY_SIGNALS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_signals_backtest (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                   TEXT NOT NULL,
    mode                     TEXT NOT NULL,
    signal_date              TEXT NOT NULL,
    trigger_time             TEXT NOT NULL,
    entry_price              REAL,
    stop_loss                REAL,
    stop_distance_pct        REAL,
    target_2x                REAL,
    target_3x                REAL,
    outcome_2x               TEXT,
    outcome_2x_date          TEXT,
    outcome_2x_price         REAL,
    outcome_2x_pct           REAL,
    outcome_2x_holding_days  INTEGER,
    outcome_3x               TEXT,
    outcome_3x_date          TEXT,
    outcome_3x_price         REAL,
    outcome_3x_pct           REAL,
    outcome_3x_holding_days  INTEGER,
    health_status_at_signal  TEXT,
    param_set                TEXT NOT NULL,
    run_timestamp            TEXT,
    UNIQUE(ticker, signal_date, trigger_time, mode, param_set)
)
"""
# 模式3（T+1尾盘确认买）没有止损/止盈这套概念（次日开盘固定了结），
# 复用outcome_2x这组列存它唯一的出场结果，stop_loss/target_2x/target_3x/
# outcome_3x*全部为NULL——这是刻意的列复用（不建一套几乎全NULL的平行
# 列），查询时用mode='模式3-尾盘确认买'区分即可。

STAGE2_PROGRESS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_stage2_progress (
    run_key TEXT NOT NULL,
    ticker  TEXT NOT NULL,
    PRIMARY KEY (run_key, ticker)
)
"""


def _generate_poll_times() -> list:
    """
    还原真实cron轮询时刻表：crontab是"*/15 10-15"（只在10点到15点这
    6个整点小时内，每15分钟触发一次），is_trading_day_and_time()又把
    SESSION_START_SAFE(10:15)之前的10:00那次过滤掉了，所以实际生效的
    轮询时刻是10:15到15:45，每15分钟一次，共23次。16:00不是真实轮询点
    （cron小时范围是10-15，不含16点整），不能直接拿MARKET_CLOSE当
    循环上界，会多算出一个不存在的轮询点。
    """
    times = []
    t = pd.Timestamp("2000-01-01 " + im.SESSION_START_SAFE)
    end = pd.Timestamp("2000-01-01 15:45")
    while t <= end:
        times.append(t.strftime("%H:%M"))
        t += pd.Timedelta(minutes=15)
    return times


_POLL_TIMES = _generate_poll_times()


def _in_late_session_window(poll_time: str) -> bool:
    """对齐intraday_monitor.py的is_late_session_window()，但用回测里
    给定的历史poll_time判断，不是像生产那样用now_syd()读实时时钟。"""
    return im.LATE_SESSION_START <= poll_time <= im.LATE_SESSION_END


def load_stage2_candidates(cfg: IntradayBacktestConfig, logger: logging.Logger) -> dict:
    """
    从intraday_health_backtest读取Stage1产出的ready/pullback_bottoming
    候选日序列——这就是Stage2要真正逐日逐bar尝试检测的全部范围，
    不需要再碰signals_history_backtest或watchlist状态机，Stage1已经
    把这层过滤做完了。

    返回 {ticker: [(date_str, health_status), ...], ...}，按日期升序。
    """
    if not os.path.exists(cfg.db_path):
        logger.error(f"数据库不存在: {cfg.db_path}")
        return {}
    conn = sqlite3.connect(cfg.db_path)
    try:
        df = pd.read_sql_query("""
            SELECT ticker, trading_date, health_status
            FROM intraday_health_backtest
            WHERE param_set = ? AND health_status IN ('ready', 'pullback_bottoming')
            ORDER BY ticker, trading_date
        """, conn, params=[cfg.stage1_param_set])
    except Exception as e:
        logger.error(f"读取Stage1候选失败: {e}")
        conn.close()
        return {}
    conn.close()

    if df.empty:
        logger.error(
            f"stage1_param_set={cfg.stage1_param_set} 在intraday_health_backtest里"
            f"没有ready/pullback_bottoming记录——检查名字是否正确（这里传的应该是"
            f"Stage1的--param-set-name，不是--eod-param-set那个名字），"
            f"或者Stage1是否真的跑完了"
        )
        return {}

    result: dict = {}
    for ticker, g in df.groupby("ticker"):
        result[ticker] = list(zip(g["trading_date"], g["health_status"]))
    logger.info(
        f"从Stage1({cfg.stage1_param_set})还原出 {len(result)} 只股票的"
        f"ready/pullback_bottoming候选日序列"
    )
    return result


def compute_stop_loss_stage2(mode: str, prior_high: float, atr14: Optional[float],
                              recent_bars_for_low: list,
                              pullback_recent_low: Optional[float]) -> tuple:
    """
    复刻monitor_one_ticker()里各模式内联的止损公式——这是backtest里
    唯一没有直接调用intraday_monitor.py真实代码、而是手动复刻一份的
    部分（因为那部分逻辑写在monitor_one_ticker()内部，不是独立函数，
    没法直接import复用），必须跟那边手动保持同步：生产代码这几个
    公式改了，这里也要跟着改。

    返回 (stop_loss, used_atr)。模式3没有这类止损，调用方不会为它
    调用这个函数。
    """
    if mode == "模式1-突破瞬间买":
        if atr14:
            return round(prior_high - im.ATR_STOP_MULTIPLIER * atr14, 3), True
        return round(prior_high * 0.995, 3), False
    if mode == "模式2-回踩确认买":
        recent_low = min(b["low"] for b in recent_bars_for_low[-6:])
        if atr14:
            return round(recent_low - im.ATR_STOP_MULTIPLIER * atr14, 3), True
        return round(recent_low * 0.995, 3), False
    if mode == "模式4-回调确认买":
        return round(pullback_recent_low, 3), False
    return None, False


def simulate_dual_target_outcome(entry_price: float, stop_loss: float,
                                  entry_day_remaining_bars: list,
                                  daily_df_after_entry_day: pd.DataFrame,
                                  timeout_days: int) -> dict:
    """
    对同一笔入场（entry_price/stop_loss固定不变），独立模拟"2倍风险
    止盈"和"3倍风险止盈"两种假设性出场纪律各自的结果，两者共用同一段
    真实价格路径（入场当天剩余15分钟bar + 之后的日线bar），只是各自
    的target不同——不预设哪个更合理，两个都记下来，等真实数据出来
    再一起看（详见对话讨论：生产的monitor_one_ticker()本身也是同时
    展示target_1r/target_2r两条参考线，没有替你选定"自动止盈在哪"，
    backtest延续这个"两个都给"的态度，不是回测独有的简化）。

    价格路径分两段扫描：
      Phase 1（入场当天剩余15分钟bar）：跟真实颗粒度一致
      Phase 2（次日起的日线bar，最多timeout_days个交易日）：跟EOD
               回测的OutcomeSimulator同一套方法论，不用为了"更精确"
               去多存几十天的15分钟历史，边际精度提升很小
    stop和两个target各自独立找"第一次触碰"的时间点，全程扫完再统一
    判定（不做提前退出的性能优化——这段路径通常很短，换取逻辑清晰、
    不容易在统计口径上出隐藏bug，比省下的那点计算时间更重要）。
    同一时间点stop和target都触发时，stop赢（跟EOD的OutcomeSimulator
    保守假设一致）。
    """
    stop_distance = abs(entry_price - stop_loss)
    target_2x_price = round(entry_price + stop_distance * 2, 4)
    target_3x_price = round(entry_price + stop_distance * 3, 4)

    stop_event = None      # ("intraday"|"daily", pd.Timestamp, price)
    target_2x_event = None
    target_3x_event = None

    for bar in entry_day_remaining_bars:
        low, high, t = bar["low"], bar["high"], bar["time"]
        if stop_event is None and low <= stop_loss:
            stop_event = ("intraday", t, stop_loss)
        if target_2x_event is None and high >= target_2x_price:
            target_2x_event = ("intraday", t, target_2x_price)
        if target_3x_event is None and high >= target_3x_price:
            target_3x_event = ("intraday", t, target_3x_price)

    future_daily = daily_df_after_entry_day.iloc[:timeout_days]
    for dt, row in future_daily.iterrows():
        low, high = float(row["Low"]), float(row["High"])
        if stop_event is None and low <= stop_loss:
            stop_event = ("daily", dt, stop_loss)
        if target_2x_event is None and high >= target_2x_price:
            target_2x_event = ("daily", dt, target_2x_price)
        if target_3x_event is None and high >= target_3x_price:
            target_3x_event = ("daily", dt, target_3x_price)

    right_side_sufficient = len(future_daily) >= timeout_days
    timeout_event = (
        ("daily", future_daily.index[-1], float(future_daily["Close"].iloc[-1]))
        if len(future_daily) else None
    )

    def resolve(target_event):
        if target_event is not None and (stop_event is None or target_event[1] < stop_event[1]):
            return "WIN", target_event
        if stop_event is not None:
            return "LOSS", stop_event
        if right_side_sufficient and timeout_event is not None:
            return "TIMEOUT", timeout_event
        return None, None  # 右侧数据不足，PENDING

    def to_pct(price) -> Optional[float]:
        return round((price / entry_price - 1) * 100, 2) if price is not None else None

    def holding_days(event) -> Optional[int]:
        if event is None:
            return None
        phase, dt, _ = event
        if phase == "intraday":
            return 0  # 入场当天就出场，T+0
        return int((daily_df_after_entry_day.index <= dt).sum())

    def unpack(event):
        if event is None:
            return None, None, None, None
        _, dt, price = event
        return str(dt.date()), price, to_pct(price), holding_days(event)

    outcome_2x, event_2x = resolve(target_2x_event)
    outcome_3x, event_3x = resolve(target_3x_event)
    d2, p2, pct2, hd2 = unpack(event_2x)
    d3, p3, pct3, hd3 = unpack(event_3x)

    return {
        "target_2x": target_2x_price, "target_3x": target_3x_price,
        "outcome_2x": outcome_2x, "outcome_2x_date": d2,
        "outcome_2x_price": p2, "outcome_2x_pct": pct2, "outcome_2x_holding_days": hd2,
        "outcome_3x": outcome_3x, "outcome_3x_date": d3,
        "outcome_3x_price": p3, "outcome_3x_pct": pct3, "outcome_3x_holding_days": hd3,
    }


def simulate_mode3_outcome(daily_df: pd.DataFrame, signal_day: pd.Timestamp,
                            entry_price: float) -> Optional[dict]:
    """
    模式3-尾盘确认买的专属出场规则：T+1，次日开盘价固定了结，不是
    "等止损或止盈触发"这套框架——生产系统的设计意图就是"不追、次日
    附近了结"，硬套止损/止盈框架会跟这个意图脱节（见此前对话C选项的
    确认）。返回None表示右侧数据不足（signal_day之后没有更多交易日），
    视为PENDING。
    """
    future = daily_df[daily_df.index > signal_day]
    if future.empty:
        return None
    exit_price = float(future["Open"].iloc[0])
    exit_date = future.index[0]
    exit_pct = round((exit_price / entry_price - 1) * 100, 2)
    outcome = "WIN" if exit_pct > 0 else "LOSS"
    return {
        "outcome": outcome, "outcome_date": str(exit_date.date()),
        "outcome_price": exit_price, "outcome_pct": exit_pct, "holding_days": 1,
    }


def process_ticker_stage2(ticker: str, health_days: list, data_layer: "bte.DataLayer",
                           cfg: IntradayBacktestConfig, logger: logging.Logger) -> dict:
    """
    对单只股票，走一遍它在Stage1里被判定为ready/pullback_bottoming的
    全部交易日，逐日、逐15分钟bar，真实调用intraday_monitor.py的检测
    函数（不近似），复刻monitor_one_ticker()的完整分支路由——含模式1
    两阶段确认状态机、模式2跨天回踩记忆。触发信号就调用出场模拟。

    这些状态（pending_breakout/last_breakout/last_signal_mode/
    last_signal_date）只需要在这次函数调用的生命周期内存活——一只
    股票的完整时间线在一次调用里从头走到尾，不会跨--max-minutes
    会话中断（检查点粒度是"整只股票"，不是"某一天"），不需要像
    backtest_breakout_memory那样另建表持久化。

    写代码时对照intraday_monitor.py原文发现的一处细节（本轮实现前已
    经在对话里说明过，这里复刻）：模式2/模式3各自的"是否已经出过
    信号"判断，跟模式1不是同一套逻辑——模式1的确认步骤检查
    "今天有没有任何模式已经发过信号"（already_signaled_today），
    模式2/3检查的是"上一次记录的信号具体是不是同一个模式"
    （last_signal_mode是否等于自己）。结果是模式1今早触发过，模式3
    下午仍可能独立触发——这是生产代码本来就有的行为，backtest原样
    复刻，不做"一天只能一个信号"的简化。

    daily_df/15分钟数据获取失败（无论是因为该股票完全没缓存，还是
    某天局部缺口）都只跳过对应的部分，不影响这只股票其余能测的日期
    ——本地15分钟数据会随data_fetcher.py每周持续累积，这不是决定
    要不要处理这只股票的门槛，只是当下这一天暂时测不到。
    """
    download_start = (pd.Timestamp(health_days[0][0]) - pd.Timedelta(days=400)).date().isoformat()
    today_str = date.today().isoformat()

    daily_df = data_layer.fetch(ticker, download_start, today_str)
    if daily_df is None:
        logger.debug(f"[{ticker}] 日线数据不足，跳过（数据覆盖边界，非错误）")
        return {"signals": [], "skipped": True}

    first_date, last_date = health_days[0][0], health_days[-1][0]
    all_15m = data_layer.fetch_15m(ticker, first_date, last_date)
    if all_15m is None:
        logger.debug(f"[{ticker}] 本地无15分钟数据覆盖，跳过（数据现状，非错误）")
        return {"signals": [], "skipped": True}

    last_signal_mode: Optional[str] = None
    last_signal_date: Optional[str] = None
    pending_breakout: Optional[dict] = None   # {"time","price","vol_ratio","bars_waited","date"}
    last_breakout: Optional[dict] = None       # {"date","price"}

    signals: list = []

    for day_str, health_status in health_days:
        day = pd.Timestamp(day_str)

        if pending_breakout is not None and pending_breakout["date"] != day_str:
            # 跨天过期清零，对齐intraday_monitor.py"疑似突破状态限定
            # 同一交易日有效"的真实行为。见文件头CHANGELOG。
            pending_breakout = None

        # 跟lock_daily_reference()完全一致：不含当天，用"今天之前"的
        # 20个交易日算prior_high/avg_vol_20d/ATR14/回调参考/区间最大
        # 回撤——这些都是"今天开盘前就已经锁定"的基准位，用了今天的
        # 数据就是未来函数。
        hist = daily_df[daily_df.index < day]
        if len(hist) < im.BREAKOUT_LOOKBACK_DAYS:
            continue
        recent20 = hist.iloc[-im.BREAKOUT_LOOKBACK_DAYS:]
        prior_high = float(recent20["High"].max())
        avg_vol_20d = float(recent20["Volume"].mean())
        atr14 = im.calc_atr14(hist["High"], hist["Low"], hist["Close"])

        pullback_ref = None
        if health_status == "pullback_bottoming":
            pullback_ref = im.compute_pullback_reference(hist["High"], hist["Low"], hist["Close"])

        breakout_max_dd_pct = None
        if last_breakout is not None:
            breakout_max_dd_pct = im.compute_breakout_max_drawdown(
                hist["High"], hist["Low"], last_breakout["date"], prior_high
            )

        day_15m = all_15m[all_15m.index.date == day.date()]
        if day_15m.empty:
            continue

        day_bars_full = [im._build_bar_record(ts, row, prior_high, avg_vol_20d)
                         for ts, row in day_15m.iterrows()]
        if not day_bars_full:
            continue

        for poll_time in _POLL_TIMES:
            poll_ts = pd.Timestamp(f"{day_str} {poll_time}:00")
            bars_so_far = [b for b in day_bars_full if b["time"] < poll_ts]
            if not bars_so_far:
                continue

            cur_bar = bars_so_far[-1]
            if cur_bar["close"] * cur_bar["volume"] < im.MIN_DOLLAR_VOLUME_INTRADAY:
                continue  # 流动性门槛没过，这次轮询跳过，不影响后面轮询

            already_signaled_today = (last_signal_date == day_str)
            fired: Optional[dict] = None

            if health_status == "ready":
                if pending_breakout is not None and pending_breakout["date"] == day_str:
                    result = im.detect_breakout_confirmation(bars_so_far, prior_high)
                    bars_waited = pending_breakout["bars_waited"] + 1
                    if result == "confirmed":
                        if not already_signaled_today:
                            cur = bars_so_far[-1]
                            fired = {"mode": "模式1-突破瞬间买", "trigger_time": cur["time"],
                                     "entry_price": cur["close"]}
                        pending_breakout = None
                    elif result == "failed" or bars_waited >= im.MODE1_CONFIRM_MAX_BARS_WAIT:
                        pending_breakout = None
                    else:
                        pending_breakout["bars_waited"] = bars_waited
                elif pending_breakout is None:
                    suspected = im.detect_mode1_breakout(
                        bars_so_far, prior_high, avg_vol_20d, im.MIN_DOLLAR_VOLUME_INTRADAY
                    )
                    if suspected:
                        pending_breakout = {
                            "time": suspected["time"], "price": suspected["price"],
                            "vol_ratio": suspected["vol_ratio"], "bars_waited": 0, "date": day_str,
                        }

                if fired is None and last_signal_mode != "模式2-回踩确认买":
                    m2_item = {
                        "last_breakout_date": last_breakout["date"] if last_breakout else None,
                        "last_breakout_price": last_breakout["price"] if last_breakout else None,
                        "avg_vol_20d": avg_vol_20d,
                        "mode2_breakout_max_dd_pct": breakout_max_dd_pct,
                    }
                    m2 = im.detect_mode2_pullback_crossday(m2_item, bars_so_far, prior_high)
                    if m2:
                        fired = {"mode": "模式2-回踩确认买", "trigger_time": m2["time"],
                                 "entry_price": m2["price"]}

                if fired is None and _in_late_session_window(poll_time):
                    if not (last_signal_mode == "模式3-尾盘确认买" and last_signal_date == day_str):
                        day_high_so_far = max(b["high"] for b in bars_so_far)
                        m3 = im.detect_mode3_late_session(bars_so_far, day_high_so_far, avg_vol_20d)
                        if m3:
                            fired = {"mode": "模式3-尾盘确认买", "trigger_time": m3["time"],
                                     "entry_price": m3["price"]}

            elif health_status == "pullback_bottoming":
                if pullback_ref is not None and not already_signaled_today:
                    m4 = im.detect_mode4_pullback_confirm(bars_so_far, pullback_ref)
                    if m4:
                        fired = {"mode": "模式4-回调确认买", "trigger_time": m4["time"],
                                 "entry_price": m4["price"],
                                 "pullback_recent_low": pullback_ref["recent_low"]}

            if fired is None:
                continue

            last_signal_mode = fired["mode"]
            last_signal_date = day_str
            entry_price = fired["entry_price"]
            trigger_time = fired["trigger_time"]

            if fired["mode"] == "模式1-突破瞬间买":
                last_breakout = {"date": day_str, "price": entry_price}

            if fired["mode"] == "模式3-尾盘确认买":
                outcome = simulate_mode3_outcome(daily_df, day, entry_price)
                signals.append({
                    "ticker": ticker, "mode": fired["mode"], "signal_date": day_str,
                    "trigger_time": str(trigger_time), "entry_price": entry_price,
                    "stop_loss": None, "stop_distance_pct": None,
                    "target_2x": None, "target_3x": None,
                    "outcome_2x": outcome["outcome"] if outcome else "PENDING",
                    "outcome_2x_date": outcome["outcome_date"] if outcome else None,
                    "outcome_2x_price": outcome["outcome_price"] if outcome else None,
                    "outcome_2x_pct": outcome["outcome_pct"] if outcome else None,
                    "outcome_2x_holding_days": outcome["holding_days"] if outcome else None,
                    "outcome_3x": None, "outcome_3x_date": None,
                    "outcome_3x_price": None, "outcome_3x_pct": None,
                    "outcome_3x_holding_days": None,
                    "health_status_at_signal": health_status,
                })
            else:
                stop_loss, _used_atr = compute_stop_loss_stage2(
                    fired["mode"], prior_high, atr14, bars_so_far,
                    fired.get("pullback_recent_low"),
                )
                if stop_loss is None or stop_loss >= entry_price:
                    logger.warning(f"[{ticker}] {day_str} {fired['mode']} 止损价异常"
                                   f"(entry={entry_price}, stop={stop_loss})，跳过这个信号")
                    continue

                remaining_bars = [b for b in day_bars_full if b["time"] > trigger_time]
                future_daily = daily_df[daily_df.index > day]
                dual = simulate_dual_target_outcome(
                    entry_price, stop_loss, remaining_bars, future_daily,
                    screener.BT_TIMEOUT_DAYS,
                )
                stop_distance_pct = round(abs(entry_price - stop_loss) / entry_price * 100, 2)
                signals.append({
                    "ticker": ticker, "mode": fired["mode"], "signal_date": day_str,
                    "trigger_time": str(trigger_time), "entry_price": entry_price,
                    "stop_loss": stop_loss, "stop_distance_pct": stop_distance_pct,
                    "target_2x": dual["target_2x"], "target_3x": dual["target_3x"],
                    "outcome_2x": dual["outcome_2x"] or "PENDING",
                    "outcome_2x_date": dual["outcome_2x_date"],
                    "outcome_2x_price": dual["outcome_2x_price"],
                    "outcome_2x_pct": dual["outcome_2x_pct"],
                    "outcome_2x_holding_days": dual["outcome_2x_holding_days"],
                    "outcome_3x": dual["outcome_3x"] or "PENDING",
                    "outcome_3x_date": dual["outcome_3x_date"],
                    "outcome_3x_price": dual["outcome_3x_price"],
                    "outcome_3x_pct": dual["outcome_3x_pct"],
                    "outcome_3x_holding_days": dual["outcome_3x_holding_days"],
                    "health_status_at_signal": health_status,
                })

    return {"signals": signals, "skipped": False}


def run_stage2(cfg: IntradayBacktestConfig, max_minutes: Optional[float] = None) -> tuple:
    """运行Stage2。返回(summary: str, completed: bool)，跟run()同样的约定。"""
    logger = setup_logging(cfg.log_path)
    logger.info(f"=== backtest_intraday.py Stage2 启动 [{cfg.param_set}] "
               f"来源Stage1={cfg.stage1_param_set} ===")

    candidates = load_stage2_candidates(cfg, logger)
    if not candidates:
        msg = f"🔴 Stage2终止 [{cfg.param_set}]：Stage1候选池为空"
        logger.error(msg)
        if cfg.push_telegram:
            send_telegram(msg, logger)
        return msg, False

    start_msg = (
        f"🚀 backtest_intraday Stage2启动 [{cfg.param_set}]\n"
        f"来源Stage1: {cfg.stage1_param_set}\n"
        f"候选股票: {len(candidates)}只\n"
        f"时间预算: {max_minutes if max_minutes else '不限'}分钟"
    )
    logger.info(start_msg.replace("\n", " | "))
    if cfg.push_telegram:
        send_telegram(start_msg, logger)

    data_layer = bte.DataLayer(logger)

    conn = sqlite3.connect(cfg.db_path)
    conn.execute(INTRADAY_SIGNALS_SCHEMA_SQL)
    conn.execute(STAGE2_PROGRESS_SCHEMA_SQL)
    conn.commit()

    run_key = f"{cfg.stage1_param_set}|{cfg.param_set}"
    tickers = sorted(candidates.keys())
    remaining = [t for t in tickers
                 if not _progress_done(conn, "intraday_stage2_progress", run_key, t)]
    if len(remaining) < len(tickers):
        logger.info(f"检测到断点：{len(tickers) - len(remaining)}只已完成，"
                   f"本次继续剩余{len(remaining)}只")

    processed = 0
    skipped = 0
    total_signals = 0
    consecutive_errors = 0
    circuit_broken = False
    start_time = time.time()
    run_ts = pd.Timestamp.now().isoformat()

    for ticker in remaining:
        if max_minutes is not None and (time.time() - start_time) / 60 >= max_minutes:
            logger.info(f"达到时间预算({max_minutes}分钟)，提前结束。"
                       f"已处理{processed}/{len(remaining)}只，下次原样重跑会自动续上")
            break

        try:
            result = process_ticker_stage2(ticker, candidates[ticker], data_layer, cfg, logger)
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"处理异常 [{ticker}]（连续失败{consecutive_errors}/"
                        f"{cfg.max_consecutive_errors}）: {e}\n{traceback.format_exc()}")
            if consecutive_errors >= cfg.max_consecutive_errors:
                alert = (f"🔴 Stage2熔断 [{cfg.param_set}]\n"
                        f"连续{consecutive_errors}只股票处理异常，最新错误: {e}\n"
                        f"已提前停止，未标记完成的股票下次会自动重试")
                logger.critical(alert)
                if cfg.push_telegram:
                    send_telegram(alert, logger)
                circuit_broken = True
                break
            continue

        consecutive_errors = 0

        if result["skipped"]:
            skipped += 1
        elif result["signals"]:
            rows = [(
                s["ticker"], s["mode"], s["signal_date"], s["trigger_time"],
                s["entry_price"], s["stop_loss"], s["stop_distance_pct"],
                s["target_2x"], s["target_3x"],
                s["outcome_2x"], s["outcome_2x_date"], s["outcome_2x_price"],
                s["outcome_2x_pct"], s["outcome_2x_holding_days"],
                s["outcome_3x"], s["outcome_3x_date"], s["outcome_3x_price"],
                s["outcome_3x_pct"], s["outcome_3x_holding_days"],
                s["health_status_at_signal"], cfg.param_set, run_ts,
            ) for s in result["signals"]]
            conn.executemany(f"""
                INSERT OR IGNORE INTO intraday_signals_backtest (
                    ticker, mode, signal_date, trigger_time, entry_price,
                    stop_loss, stop_distance_pct, target_2x, target_3x,
                    outcome_2x, outcome_2x_date, outcome_2x_price,
                    outcome_2x_pct, outcome_2x_holding_days,
                    outcome_3x, outcome_3x_date, outcome_3x_price,
                    outcome_3x_pct, outcome_3x_holding_days,
                    health_status_at_signal, param_set, run_timestamp
                ) VALUES ({",".join(["?"] * 22)})
            """, rows)
            total_signals += len(rows)

        _progress_mark_done(conn, "intraday_stage2_progress", run_key, ticker)
        conn.commit()
        processed += 1

        if processed % 50 == 0:
            elapsed = time.time() - start_time
            logger.info(f"进度 {processed}/{len(remaining)}，已用{elapsed/60:.1f}分钟，"
                       f"累计{total_signals}笔信号")

    conn.close()

    total_done = len(tickers) - len(remaining) + processed
    completed = (not circuit_broken) and (total_done >= len(tickers))
    status_line = (
        "🛑 因熔断提前停止" if circuit_broken else
        ("✅ 全部完成" if completed else "⏸ 已按时间预算暂停")
    )
    summary = (
        f"{status_line} Stage2 [{cfg.param_set}]\n"
        f"本次处理: {processed}只（数据不足跳过{skipped}只）\n"
        f"本次新增信号: {total_signals}笔\n"
        f"累计总进度: {total_done}/{len(tickers)}只"
        + (f"，下次重跑相同参数会自动续上" if not completed else "")
    )
    logger.info("=== " + summary.replace("\n", " | ") + " ===")
    if cfg.push_telegram:
        send_telegram(summary, logger)
    return summary, completed


def show_stage2_stats(cfg: IntradayBacktestConfig) -> None:
    """
    --stats-only --run-stage2：模式拆解 + 2倍/3倍两套出场纪律各自的
    胜率 + 模式4专属的止损距离提醒（对话里讨论过：模式4止损没有ATR
    缓冲，是四个模式里止损天然最紧的一个，命中目标是不是噪音驱动
    要专门看这个数字）。
    """
    if not os.path.exists(cfg.db_path):
        print(f"数据库不存在: {cfg.db_path}")
        return
    conn = sqlite3.connect(cfg.db_path)
    df = pd.read_sql_query(
        "SELECT * FROM intraday_signals_backtest WHERE param_set = ?",
        conn, params=[cfg.param_set],
    )
    conn.close()

    if df.empty:
        print(f"param_set={cfg.param_set} 暂无Stage2结果，先跑一次"
             f"不带--stats-only的--run-stage2命令")
        return

    print("=" * 70)
    print(f"Stage2统计 [param_set={cfg.param_set}]")
    print("=" * 70)
    print(f"信号总数: {len(df)}")
    print("\n按模式拆解：")
    for mode, g in df.groupby("mode"):
        print(f"  {mode}: {len(g)}笔")

    non_mode3 = df[df["mode"] != "模式3-尾盘确认买"]

    print("\n【假设永远2倍风险止盈】（PENDING不计入胜率）")
    for mode, g in non_mode3.groupby("mode"):
        resolved = g[g["outcome_2x"] != "PENDING"]
        if resolved.empty:
            print(f"  {mode}: 全部PENDING，暂无已结算样本")
            continue
        wins = int((resolved["outcome_2x"] == "WIN").sum())
        wr = wins / len(resolved) * 100
        avg_stop_pct = resolved["stop_distance_pct"].mean()
        print(f"  {mode}: {len(resolved)}笔已结算，胜率{wr:.1f}%，"
             f"平均止损距离{avg_stop_pct:.2f}%")
        if mode == "模式4-回调确认买" and avg_stop_pct < 2.0:
            print(f"    ⚠️ 止损距离偏小（<2%），命中目标可能更多是噪音驱动，"
                 f"不代表真实趋势跟随——建议对照对话里讨论过的这个顾虑")

    print("\n【假设永远3倍风险止盈】")
    for mode, g in non_mode3.groupby("mode"):
        resolved = g[g["outcome_3x"] != "PENDING"]
        if resolved.empty:
            print(f"  {mode}: 全部PENDING")
            continue
        wins = int((resolved["outcome_3x"] == "WIN").sum())
        wr = wins / len(resolved) * 100
        print(f"  {mode}: {len(resolved)}笔已结算，胜率{wr:.1f}%")

    mode3 = df[df["mode"] == "模式3-尾盘确认买"]
    if not mode3.empty:
        resolved3 = mode3[mode3["outcome_2x"] != "PENDING"]
        print(f"\n【模式3-尾盘确认买】（T+1固定次日开盘出场，无止盈止损概念，"
             f"outcome_2x这一列复用来存它唯一的结果）")
        if resolved3.empty:
            print("  全部PENDING")
        else:
            wins3 = int((resolved3["outcome_2x"] == "WIN").sum())
            print(f"  {len(resolved3)}笔已结算，胜率{wins3/len(resolved3)*100:.1f}%")

    print("\n⚠️ 提醒：2倍和3倍是同一批信号、同一个入场价和止损价，"
         "分别假设\"永远2倍出场\"\"永远3倍出场\"两种纪律算出来的，"
         "不是两批不同的信号，不能简单相加。样本量小于30笔的分组，"
         "结论仅供参考，不要下结论。")



def main() -> None:
    parser = argparse.ArgumentParser(
        description="15分钟颗粒度日内策略回测：--run-all（一条命令跑完Stage1+Stage2）"
                     "/ 分步跑Stage1（默认）/ --run-stage2（单独跑Stage2）"
    )
    parser.add_argument("--eod-param-set", default="",
                        help="[Stage1 / --run-all] 来源EOD回测的param_set（必须已经跑过"
                             "backtest_engine.py）。DAILY_HEALTH参数会自动从这个param_set的"
                             "experiment_metadata同步，不需要（也不应该）再单独传--params-file")
    parser.add_argument("--stage1-param-set", default="",
                        help="[--run-stage2专用，--run-all不需要] 来源Stage1的--param-set-name")
    parser.add_argument("--run-stage2", action="store_true",
                        help="只运行Stage2，需要先跑完对应的Stage1（--stage1-param-set指向的"
                             "那次）。本地15分钟数据覆盖率随data_fetcher.py每周累积后，隔一段"
                             "时间想用更多数据重新评估同一批Stage1候选时用这个，不需要重新跑"
                             "Stage1——但--param-set-name必须换成没用过的新名字，见文件头"
                             "docstring'运行方式B'")
    parser.add_argument("--run-all", action="store_true",
                        help="一条命令顺序跑完Stage1+Stage2：用--eod-param-set/--param-set-name"
                             "跑Stage1，全部完成后自动以'<--param-set-name>_stage2'为名接着跑"
                             "Stage2。Stage1没在本次--max-minutes预算内跑完就不会进入Stage2，"
                             "原样重跑同一条命令即可续上")
    parser.add_argument("--param-set-name", default="intraday_baseline",
                        help="Stage1（或--run-all里Stage1那部分）的标签；单独跑--run-stage2时"
                             "是Stage2自己的标签")
    parser.add_argument("--disable-ma50-gate", action="store_true",
                        help="[仅Stage1] 关闭MA50提前退出门槛的复刻（默认开启，"
                             "是标准方法论的一部分）")
    parser.add_argument("--max-minutes", type=float, default=None,
                        help="每个阶段各自的时间预算（--run-all时Stage1和Stage2分别可用"
                             "这么多分钟，不是两段共享一个预算）")
    parser.add_argument("--stats-only", action="store_true",
                        help="不重跑，只汇总已有结果（配合--run-stage2看Stage2的统计，"
                             "不加则看Stage1的统计）")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    cfg = IntradayBacktestConfig(
        eod_param_set=args.eod_param_set,
        stage1_param_set=args.stage1_param_set,
        param_set=args.param_set_name,
        implement_ma50_exit_gate=not args.disable_ma50_gate,
        push_telegram=not args.no_telegram,
    )

    if args.stats_only:
        if args.run_stage2:
            show_stage2_stats(cfg)
        else:
            show_stats(cfg)
        return

    if args.run_all:
        if not cfg.eod_param_set:
            print("必须指定 --eod-param-set（--run-all需要先跑Stage1）")
            return
        try:
            _summary1, completed1 = run(cfg, max_minutes=args.max_minutes)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"顶层未捕获异常(Stage1): {e}\n{tb}")
            if cfg.push_telegram:
                send_telegram(
                    f"🔴 backtest_intraday.py --run-all Stage1阶段崩溃\n"
                    f"参数集: {cfg.param_set}\n错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                    logging.getLogger("backtest_intraday"),
                )
            sys.exit(1)

        if not completed1:
            print(
                "\nStage1本次未全部完成（时间预算用完或熔断），--run-all不会自动进入"
                "Stage2。原样重跑同一条命令即可从断点续上；Stage1真正'全部完成'的那次"
                "会自动接着跑Stage2。"
            )
            return

        cfg2 = IntradayBacktestConfig(
            stage1_param_set=cfg.param_set,
            param_set=f"{cfg.param_set}_stage2",
            push_telegram=cfg.push_telegram,
        )
        try:
            run_stage2(cfg2, max_minutes=args.max_minutes)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"顶层未捕获异常(Stage2): {e}\n{tb}")
            if cfg2.push_telegram:
                send_telegram(
                    f"🔴 backtest_intraday.py --run-all Stage2阶段崩溃\n"
                    f"参数集: {cfg2.param_set}\n错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                    logging.getLogger("backtest_intraday"),
                )
            sys.exit(1)
        return

    if args.run_stage2:
        if not cfg.stage1_param_set:
            print(
                "必须指定 --stage1-param-set（来源Stage1的--param-set-name，"
                "不是--eod-param-set那个名字）"
            )
            return
        try:
            run_stage2(cfg, max_minutes=args.max_minutes)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"顶层未捕获异常: {e}\n{tb}")
            if cfg.push_telegram:
                send_telegram(
                    f"🔴 backtest_intraday.py Stage2 进程崩溃\n参数集: {cfg.param_set}\n"
                    f"错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                    logging.getLogger("backtest_intraday"),
                )
            sys.exit(1)
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
                f"🔴 backtest_intraday.py Stage1 进程崩溃\n参数集: {cfg.param_set}\n"
                f"错误: {e}\n\ntraceback(截断):\n{tb[-1500:]}",
                logging.getLogger("backtest_intraday"),
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
