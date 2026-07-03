# ============================================================
# watchlist_db.py
# 监测队列数据库管理 —— screener.py 和 intraday_monitor.py 共用
#
# 设计要点（自问自答记录见对话）：
# - 监测天数按筛选等级（T1-T4）分配，T1最优给30天，依次递减
# - 同一股票被EOD重复选中 → 监测天数累加（不超过上限），并记录"复选次数"
# - 健康度字段：若盘中监测发现走势恶化，提前清出，不必跑满天数
# ============================================================

import os
import sqlite3
import logging
from datetime import date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

WATCHLIST_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "watchlist.db"
)

# 监测等级 → 初始监测天数映射
# T1最严格筛选出来的信号质量最高，给最长观察期
# 2026-07更新：天数翻倍，配合daily_analysis.py的跨日因子分析需要更长观察窗口
TIER_MONITOR_DAYS = {
    "T1": 60,
    "T2": 40,
    "T3": 24,
    "T4": 14,
}

# 单只股票最长监测天数上限（防止累加无限拉长，资源失控）
# 上限设为60天而非90天（简单翻倍值），理由：
# yfinance的15分钟K线接口最多只能回溯60天历史数据（已在实测中确认，
# 见daily_analysis.py的BACKFILL_PERIOD="60d"）。如果监测周期超过60天，
# 超出部分的股票会进入"有监测天数配额、但daily_analysis.py无法再补充
# 新的跨日历史数据"的空转状态——不是系统bug，是外部数据源的硬限制，
# 但监测周期设计上不应该超过数据源能实际支撑的范围，否则后段监测
# 名不副实。60天上限让T1的60天监测周期恰好与数据覆盖上限对齐。
MAX_MONITOR_DAYS = 60


def init_watchlist_db() -> None:
    """初始化监测队列数据库（首次运行自动建表）"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    ticker            TEXT PRIMARY KEY,
                    company_name      TEXT,
                    tier_level        TEXT,
                    tier_label        TEXT,
                    composite_score   REAL,
                    entry_date        TEXT NOT NULL,
                    last_reselect_date TEXT,
                    total_days        INTEGER NOT NULL,
                    days_elapsed      INTEGER DEFAULT 0,
                    reselect_count    INTEGER DEFAULT 0,
                    status            TEXT DEFAULT 'active',
                    exit_reason       TEXT,
                    exit_date         TEXT,
                    source            TEXT DEFAULT 'eod',  -- 'eod' 自动筛选 / 'manual' 用户手动添加
                    -- 监测期内锁定的关键基准位（每个新交易日重新计算一次）
                    ref_date          TEXT,
                    prior_high_20d    REAL,
                    prior_low_20d     REAL,
                    avg_vol_20d       REAL,
                    -- 用于止损判定的记录
                    last_signal_mode  TEXT,
                    last_signal_date  TEXT,
                    last_signal_price REAL,
                    stop_loss_price   REAL,
                    -- daily_analysis.py盘前分析写入的当日跨日状态，
                    -- 供intraday_monitor.py读取，决定是否对该股票
                    -- 运行三种日内模式判断（只对today_status='ready'的股票判断）
                    today_status       TEXT,
                    today_status_date  TEXT,
                    today_signal_count INTEGER DEFAULT 0,
                    notes             TEXT
                )
            """)
            # 兼容旧数据库：若表已存在但缺少source/today_status列，补充上去
            cols = [r[1] for r in conn.execute("PRAGMA table_info(watchlist)").fetchall()]
            if "source" not in cols:
                conn.execute("ALTER TABLE watchlist ADD COLUMN source TEXT DEFAULT 'eod'")
            if "today_status" not in cols:
                conn.execute("ALTER TABLE watchlist ADD COLUMN today_status TEXT")
            if "today_status_date" not in cols:
                conn.execute("ALTER TABLE watchlist ADD COLUMN today_status_date TEXT")
            if "today_signal_count" not in cols:
                conn.execute("ALTER TABLE watchlist ADD COLUMN today_signal_count INTEGER DEFAULT 0")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS intraday_snapshots (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker          TEXT NOT NULL,
                    snapshot_time   TEXT NOT NULL,   -- ISO格式，含日期时间
                    trading_date    TEXT NOT NULL,   -- 仅日期，便于按日聚合
                    price           REAL,
                    high            REAL,
                    low             REAL,
                    volume          REAL,
                    vwap            REAL,
                    pct_from_prior_high REAL,    -- 距离前高百分比
                    vol_vs_avg_ratio    REAL,    -- 该时段量比（vs过去20日同时段均量）
                    breakout_state      TEXT,    -- 'none'/'breaking'/'confirmed'/'failed'
                    UNIQUE(ticker, snapshot_time)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snap_ticker_date "
                "ON intraday_snapshots(ticker, trading_date)"
            )
            conn.commit()
        log.info(f"监测队列数据库就绪：{WATCHLIST_DB_PATH}")
    except Exception as e:
        log.error(f"监测队列数据库初始化失败: {e}")


def _upsert_core(ticker: str, company_name: str, tier_level: str,
                  tier_label: str, composite_score: Optional[float],
                  add_days: int, source: str) -> dict:
    """
    所有入队/续期操作共享的核心逻辑：
    - 不存在则新建，存在则累加天数（封顶MAX_MONITOR_DAYS）
    - 不管来源是EOD自动选股还是用户手动添加，累加规则完全一致，
      避免两套独立实现以后改一处忘了改另一处
    - source字段记录"最近一次入队/续期的来源"，每次upsert都覆盖更新
    返回dict，包含本次操作结果（供调用方组装Telegram回复文案）
    """
    today = date.today().isoformat()
    if add_days < 1:
        add_days = 1
    if add_days > MAX_MONITOR_DAYS:
        add_days = MAX_MONITOR_DAYS

    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            row = conn.execute(
                "SELECT total_days, days_elapsed, reselect_count FROM watchlist WHERE ticker = ?",
                (ticker,)
            ).fetchone()

            if row is None:
                conn.execute("""
                    INSERT INTO watchlist
                        (ticker, company_name, tier_level, tier_label, composite_score,
                         entry_date, total_days, days_elapsed, reselect_count, status, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 'active', ?)
                """, (ticker, company_name, tier_level, tier_label,
                      composite_score, today, add_days, source))
                log.info(f"监测队列新增 [{ticker}]：{source} → {add_days}天")
                conn.commit()
                return {"action": "created", "ticker": ticker,
                        "added_days": add_days, "new_total": add_days,
                        "days_elapsed": 0, "reselect_count": 0}
            else:
                total_days, days_elapsed, reselect_count = row
                new_total = min(total_days + add_days, MAX_MONITOR_DAYS)
                actual_added = new_total - total_days  # 可能因封顶而小于add_days
                conn.execute("""
                    UPDATE watchlist
                    SET total_days = ?, reselect_count = ?, last_reselect_date = ?,
                        tier_level = ?, tier_label = ?, composite_score = ?,
                        status = 'active', exit_reason = NULL, exit_date = NULL,
                        source = ?, company_name = ?
                    WHERE ticker = ?
                """, (new_total, reselect_count + 1, today,
                      tier_level, tier_label, composite_score, source,
                      company_name, ticker))
                conn.commit()
                log.info(
                    f"监测队列累加 [{ticker}]：{total_days}天 + {add_days}天 → "
                    f"{new_total}天（已用{days_elapsed}天，第{reselect_count + 1}次复选，来源:{source}）"
                )
                return {"action": "extended", "ticker": ticker,
                        "added_days": actual_added, "new_total": new_total,
                        "days_elapsed": days_elapsed, "reselect_count": reselect_count + 1,
                        "capped": actual_added < add_days}
    except Exception as e:
        log.error(f"监测队列写入失败 [{ticker}]: {e}")
        return {"action": "error", "ticker": ticker, "error": str(e)}


def upsert_watchlist(ticker: str, company_name: str, tier_level: str,
                      tier_label: str, composite_score: float) -> None:
    """
    EOD选股后调用：将信号加入监测队列。
    若已在队列中（重复入选），累加监测天数（不超过MAX_MONITOR_DAYS），
    并记录复选次数和复选日期 —— 这代表市场对该股票的关注度在持续，
    值得延长观察。
    """
    add_days = TIER_MONITOR_DAYS.get(tier_level, 7)
    _upsert_core(ticker, company_name, tier_level, tier_label,
                 composite_score, add_days, source="eod")


def upsert_watchlist_manual(ticker: str, company_name: str, days: int) -> dict:
    """
    用户通过Telegram手动添加股票到监测队列时调用。
    与EOD路径的核心区别：
    - 天数由用户直接指定，不查TIER_MONITOR_DAYS表
    - tier_level/tier_label/composite_score填None（这只股票没有经过T1-T4打分），
      intraday_monitor.py推送信号时若读到这些字段为空，会显示"手动添加"而非具体评分
    - 同样遵循"重复添加→累加天数，封顶MAX_MONITOR_DAYS"的统一规则
    返回dict供bot.py组装回复文案（新增/续期/天数被封顶等情况都需要不同提示）
    """
    return _upsert_core(ticker, company_name, tier_level=None,
                        tier_label="手动添加", composite_score=None,
                        add_days=days, source="manual")


def remove_from_watchlist(ticker: str) -> bool:
    """
    用户通过Telegram手动移出监测队列。
    返回True表示成功移出，False表示该股票本来就不在队列中。
    """
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            row = conn.execute(
                "SELECT status FROM watchlist WHERE ticker = ?", (ticker,)
            ).fetchone()
            if row is None:
                return False
            conn.execute("""
                UPDATE watchlist
                SET status = 'exited', exit_reason = '用户手动移出', exit_date = ?
                WHERE ticker = ?
            """, (date.today().isoformat(), ticker))
            conn.commit()
        log.info(f"监测队列手动移出 [{ticker}]")
        return True
    except Exception as e:
        log.error(f"手动移出失败 [{ticker}]: {e}")
        return False


def list_watchlist_for_display(include_exited: bool = False) -> list:
    """
    供 /watchlist Telegram命令使用：返回便于人类阅读的队列列表。
    排序：active优先于exited；同状态内按剩余天数比例升序
    （快到期的排前面，提醒你注意），而非按composite_score
    （手动添加的股票该字段为None，若用那个排序会全部沉底，不直观）。
    """
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT ticker, company_name, tier_label, composite_score,
                       entry_date, total_days, days_elapsed, reselect_count,
                       source, status, exit_reason,
                       last_signal_mode, last_signal_date
                FROM watchlist
                ORDER BY
                    CASE status WHEN 'active' THEN 0 ELSE 1 END,
                    CAST(days_elapsed AS REAL) / total_days DESC
            """).fetchall()
        cols = ["ticker", "company_name", "tier_label", "composite_score",
                "entry_date", "total_days", "days_elapsed", "reselect_count",
                "source", "status", "exit_reason",
                "last_signal_mode", "last_signal_date"]
        result = [dict(zip(cols, r)) for r in rows]
        if not include_exited:
            result = [r for r in result if r["status"] == "active"]
        return result
    except Exception as e:
        log.error(f"读取展示用监测队列失败: {e}")
        return []


def get_watchlist_entry(ticker: str) -> Optional[dict]:
    """查询单只股票当前是否在队列中及其详情，供/watch命令判断重复添加前先提示当前状态"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            row = conn.execute("""
                SELECT ticker, company_name, total_days, days_elapsed, status, source
                FROM watchlist WHERE ticker = ?
            """, (ticker,)).fetchone()
        if row is None:
            return None
        cols = ["ticker", "company_name", "total_days", "days_elapsed", "status", "source"]
        return dict(zip(cols, row))
    except Exception as e:
        log.error(f"查询监测条目失败 [{ticker}]: {e}")
        return None


def get_active_watchlist() -> list:
    """返回所有仍在监测期内的股票（status='active' 且 days_elapsed < total_days）"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT ticker, company_name, tier_level, tier_label, composite_score,
                       entry_date, total_days, days_elapsed, reselect_count, source,
                       ref_date, prior_high_20d, prior_low_20d, avg_vol_20d,
                       last_signal_mode, last_signal_date, last_signal_price, stop_loss_price,
                       today_status, today_status_date, today_signal_count
                FROM watchlist
                WHERE status = 'active' AND days_elapsed < total_days
                ORDER BY composite_score DESC
            """).fetchall()
        cols = ["ticker", "company_name", "tier_level", "tier_label", "composite_score",
                "entry_date", "total_days", "days_elapsed", "reselect_count", "source",
                "ref_date", "prior_high_20d", "prior_low_20d", "avg_vol_20d",
                "last_signal_mode", "last_signal_date", "last_signal_price", "stop_loss_price",
                "today_status", "today_status_date", "today_signal_count"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error(f"读取监测队列失败: {e}")
        return []


def update_daily_reference(ticker: str, ref_date: str, prior_high_20d: float,
                            prior_low_20d: float, avg_vol_20d: float) -> None:
    """每个新交易日开盘后调用一次：锁定当日基准位，全天不变（防未来函数）"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                UPDATE watchlist
                SET ref_date = ?, prior_high_20d = ?, prior_low_20d = ?, avg_vol_20d = ?
                WHERE ticker = ?
            """, (ref_date, prior_high_20d, prior_low_20d, avg_vol_20d, ticker))
            conn.commit()
    except Exception as e:
        log.error(f"更新基准位失败 [{ticker}]: {e}")


def increment_day_elapsed(ticker: str) -> None:
    """每个交易日收盘后调用一次，监测天数+1"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute(
                "UPDATE watchlist SET days_elapsed = days_elapsed + 1 WHERE ticker = ?",
                (ticker,)
            )
            conn.commit()
    except Exception as e:
        log.error(f"更新监测天数失败 [{ticker}]: {e}")


def exit_watchlist(ticker: str, reason: str) -> None:
    """提前清出监测队列（健康度不达标/已触发信号且完成交易/天数耗尽）"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                UPDATE watchlist
                SET status = 'exited', exit_reason = ?, exit_date = ?
                WHERE ticker = ?
            """, (reason, date.today().isoformat(), ticker))
            conn.commit()
        log.info(f"监测队列移出 [{ticker}]：{reason}")
    except Exception as e:
        log.error(f"移出监测队列失败 [{ticker}]: {e}")


def record_signal(ticker: str, mode: str, price: float, stop_loss: float) -> None:
    """记录最近一次触发的信号（用于避免同一信号当天重复推送，以及次日止损监控）"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                UPDATE watchlist
                SET last_signal_mode = ?, last_signal_date = ?,
                    last_signal_price = ?, stop_loss_price = ?
                WHERE ticker = ?
            """, (mode, date.today().isoformat(), price, stop_loss, ticker))
            conn.commit()
    except Exception as e:
        log.error(f"记录信号失败 [{ticker}]: {e}")


def update_today_status(ticker: str, status: str, signal_count: int = 0) -> None:
    """
    daily_analysis.py盘前分析完成后调用，写入该股票今日的跨日状态。

    status取值：ready / watch / caution / accumulating
    （与daily_analysis.py的evaluate_ticker()返回值保持一致，
    这里不做校验转换，交给调用方保证一致性，避免两处各自维护
    一套状态枚举导致后续不同步）

    写入today_status_date是为了让intraday_monitor.py能判断
    "这个状态是不是今天算出来的"——如果daily_analysis.py因为某种
    原因当天没跑（比如VM重启错过了crontab），intraday_monitor.py
    读到的会是昨天的状态，必须能识别出这种过期情况，不能默认信任。
    """
    today = date.today().isoformat()
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                UPDATE watchlist
                SET today_status = ?, today_status_date = ?, today_signal_count = ?
                WHERE ticker = ?
            """, (status, today, signal_count, ticker))
            conn.commit()
        log.info(f"今日状态写入 [{ticker}]: {status}（信号数:{signal_count}）")
    except Exception as e:
        log.error(f"今日状态写入失败 [{ticker}]: {e}")


def get_today_status(ticker: str) -> dict:
    """
    intraday_monitor.py每次15分钟轮询时调用，读取该股票今日跨日状态。

    返回dict包含：
    - status: 状态值，如果today_status_date不是今天，返回'stale'
             （过期状态，不能作为判断依据）
    - is_fresh: 布尔值，True表示状态是今天写入的，可信
    - signal_count: 今日信号数

    stale状态的处理交给调用方决定——保守做法是stale时不运行三个模式
    判断（避免用旧数据做新决策），但仍然继续写快照积累数据。
    """
    today = date.today().isoformat()
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            row = conn.execute("""
                SELECT today_status, today_status_date, today_signal_count
                FROM watchlist WHERE ticker = ?
            """, (ticker,)).fetchone()

        if row is None or row[0] is None:
            return {"status": "unknown", "is_fresh": False, "signal_count": 0}

        status, status_date, signal_count = row
        is_fresh = (status_date == today)

        return {
            "status": status if is_fresh else "stale",
            "is_fresh": is_fresh,
            "signal_count": signal_count or 0,
            "raw_status": status,       # 即使过期也保留原始值，供日志参考
            "status_date": status_date,
        }
    except Exception as e:
        log.error(f"读取今日状态失败 [{ticker}]: {e}")
        return {"status": "unknown", "is_fresh": False, "signal_count": 0}


def save_snapshot(ticker: str, snapshot_time: str, trading_date: str,
                   price: float, high: float, low: float, volume: float,
                   vwap: float, pct_from_prior_high: float,
                   vol_vs_avg_ratio: float, breakout_state: str) -> None:
    """保存每次15分钟轮询的快照，供历史比对"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            conn.execute("""
                INSERT OR IGNORE INTO intraday_snapshots
                    (ticker, snapshot_time, trading_date, price, high, low, volume,
                     vwap, pct_from_prior_high, vol_vs_avg_ratio, breakout_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker, snapshot_time, trading_date, price, high, low, volume,
                  vwap, pct_from_prior_high, vol_vs_avg_ratio, breakout_state))
            conn.commit()
    except Exception as e:
        log.error(f"保存快照失败 [{ticker}]: {e}")


def get_today_snapshots(ticker: str, trading_date: str) -> list:
    """读取某股票今日全部快照（按时间升序），用于判断'第一次突破'/'回踩'等时序逻辑"""
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT snapshot_time, price, high, low, volume, vwap,
                       pct_from_prior_high, vol_vs_avg_ratio, breakout_state
                FROM intraday_snapshots
                WHERE ticker = ? AND trading_date = ?
                ORDER BY snapshot_time ASC
            """, (ticker, trading_date)).fetchall()
        cols = ["snapshot_time", "price", "high", "low", "volume", "vwap",
                "pct_from_prior_high", "vol_vs_avg_ratio", "breakout_state"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error(f"读取今日快照失败 [{ticker}]: {e}")
        return []


def get_recent_same_time_volumes(ticker: str, time_of_day: str,
                                  lookback_days: int = 20) -> list:
    """
    读取过去N个交易日同一时段（如10:15）的成交量，
    用于计算'该时段历史平均量'，比单纯和当天早盘比更公平。
    time_of_day格式：'HH:MM'
    """
    cutoff = (date.today() - timedelta(days=lookback_days * 2)).isoformat()
    try:
        with sqlite3.connect(WATCHLIST_DB_PATH) as conn:
            rows = conn.execute("""
                SELECT volume FROM intraday_snapshots
                WHERE ticker = ? AND trading_date >= ?
                  AND snapshot_time LIKE ?
                ORDER BY trading_date DESC
                LIMIT ?
            """, (ticker, cutoff, f"%T{time_of_day}%", lookback_days)).fetchall()
        return [r[0] for r in rows if r[0] is not None]
    except Exception as e:
        log.error(f"读取历史同时段量能失败 [{ticker}]: {e}")
        return []
