#!/usr/bin/env python3
"""
data_fetcher.py
================
独立的行情数据预热/累积工具，跟backtest_engine.py解耦——这个脚本只管
"把本地缓存养肥"，不跑任何选股/回测逻辑。backtest_engine.py的DataLayer
已经改成优先查market_data_cache.py这份本地缓存，本地已经覆盖的区间
不会再对yfinance重新请求。

两种运行模式：

1. --mode backfill
   预热日线（实际很深，回溯到标准回测窗口+热身缓冲即可）和60分钟线
   （~729天）的标准回测窗口。跑完之后，同一个测试窗口下反复跑多组
   backtest_engine.py参数实验，理论上数据下载阶段一次网络请求都不用
   发（除非你把测试窗口往前/往后挪了，那时只会补那一小段缺口，不会
   全部重下）。幂等：重复运行不会重复下载已经覆盖的部分，可以放心
   在每次开一批新实验前都跑一次，跑得快的原因就是大部分股票直接
   命中本地缓存。

2. --mode weekly15m
   拉取全市场15分钟线最近~59天窗口，合并进逐票累积的Parquet归档。
   yfinance对15分钟颗粒度只给最近~60天，无法一次性回补更早的历史
   ——这是数据源本身的硬限制。这个模式的价值在于"按周期定时跑"：
   只要不断档超过~60天，每周新增的这一段会跟上次缓存的末尾无缝
   衔接，长期下来归档范围就单调往前滚动累积，最终能积累出真正
   多年的15分钟历史，供未来对intraday_monitor.py做更贴近真实颗粒度
   的验证用（现在backtest_engine.py里的HTF小时级变种策略，就是在
   等这份15分钟数据攒够之前的一个过渡性近似方案）。
   建议配置crontab每周固定跑一次（比如周六收盘后），不要断档太久
   ——断档超过~60天，中间缺的那段永久补不回来了。

覆盖范围（本轮确认）：全市场~2000只，两种模式都是。

用法：
    # 第一次跑（或者想确保标准窗口覆盖完整时）：全市场日线+60分钟线预热
    python3 data_fetcher.py --mode backfill --universe full

    # 每周crontab跑：全市场15分钟线增量累积
    python3 data_fetcher.py --mode weekly15m --universe full

    # 查看目前缓存覆盖情况（不发任何网络请求）
    python3 data_fetcher.py --coverage

    # 限制单次运行时间（配合crontab，避免跟下一次任务重叠；未跑完的
    # 部分下次运行会自动接着补，跟backtest_engine.py的--max-minutes
    # 是同一个设计思路）
    python3 data_fetcher.py --mode weekly15m --universe full --max-minutes 120
"""

import argparse
import logging
import os
import sys
import time
import traceback
from typing import Optional

import pandas as pd
import requests

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

try:
    import market_data_cache as mdc
except ImportError as e:
    print(f"无法 import market_data_cache —— 必须和本文件同目录。原始错误: {e}")
    sys.exit(1)

try:
    import screener  # 复用get_asx_universe()，跟backtest_engine.py的universe来源一致
except ImportError as e:
    print(f"无法 import screener —— 本脚本必须和screener.py同目录。原始错误: {e}")
    sys.exit(1)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# 跟backtest_engine.py的标准测试窗口保持一致——预热的目的就是让
# backtest_engine.py按默认参数跑起来时数据基本全部命中本地缓存
STANDARD_WINDOW_DAYS = 700
STANDARD_WARMUP_DAYS = 365
HOURLY_MAX_HISTORY_DAYS = 729

# yfinance 15分钟颗粒度实际上限约60天，留1天安全余量。曾经短暂收窄到
# 55天，是把"部分股票15m颗粒度本身没数据"误判成"窗口边界问题"——后经
# 交叉验证（同一59天窗口下仍有714只成功、失败股票的daily/60m数据都完好）
# 证实与窗口宽度无关，改回59天，跟market_data_cache.py的15m配置保持一致
WEEKLY_15M_WINDOW_DAYS = 59

# v1.2：批量请求的批次大小，跟screener.py的download_ohlcv()用同一个
# 数值（已在生产环境验证过稳定可靠）。熔断逻辑（连续多少批全部失败
# 就提前停止）现在搬进了market_data_cache.py的warm_batch()内部，
# 按"批次"而不是"单只股票"计数——批量模式下，单只股票偶尔没数据是
# 正常噪音，但连续好几批（每批50只）全部失败是极强的系统性问题信号，
# 不再需要像v1.1那样对daily/15m分别设不同的宽松阈值。
BATCH_SIZE = 50


def setup_logging(log_path: str) -> logging.Logger:
    logger = logging.getLogger("data_fetcher")
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


def send_telegram(text: str, logger: logging.Logger) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Telegram推送失败: {e}")


def resolve_universe(universe: str, universe_file: str, logger: logging.Logger) -> list[str]:
    if universe == "full":
        tickers = screener.get_asx_universe()
        logger.info(f"universe=full：{len(tickers)}只")
        return tickers
    if universe == "file":
        if not universe_file or not os.path.exists(universe_file):
            logger.error(f"universe文件不存在: {universe_file}")
            return []
        with open(universe_file, encoding="utf-8") as f:
            tickers = [ln.strip() for ln in f if ln.strip()]
        logger.info(f"universe=file：{len(tickers)}只（来自 {universe_file}）")
        return tickers
    logger.error(f"未知universe: {universe}")
    return []


def run_backfill(cache: "mdc.MarketDataCache", tickers: list[str],
                 max_minutes: Optional[float], logger: logging.Logger,
                 push_telegram: bool = True) -> dict:
    """
    预热日线+60分钟线到标准回测窗口。v1.2改为委托给
    cache.warm_batch()，用批量请求（每批BATCH_SIZE只）替代逐票单独
    请求——决定耗时的是网络请求次数，不是每次请求带的数据范围大小，
    批量能把请求次数从~2000次压到~40次，带来数量级的提速。

    天然幂等、天然支持断点续跑：warm_batch()内部用manifest快速判断
    "本地是否已经完整覆盖所需区间"，已覆盖的股票完全不碰网络；被
    max_minutes打断后原样重跑同一条命令，已经覆盖的批次会立刻跳过，
    只继续处理还没处理到的。

    日线和60分钟线分两次warm_batch()调用，共享同一个max_minutes
    时间预算（日线先跑，用剩的时间再给60分钟线，避免两段加起来
    超出用户设定的总预算）。
    """
    end = pd.Timestamp.now().normalize().date().isoformat()
    start = (pd.Timestamp.now().normalize()
             - pd.Timedelta(days=STANDARD_WINDOW_DAYS + STANDARD_WARMUP_DAYS)).date().isoformat()
    hourly_earliest = (pd.Timestamp.now().normalize()
                       - pd.Timedelta(days=HOURLY_MAX_HISTORY_DAYS))
    hourly_start = max(pd.Timestamp(start), hourly_earliest).date().isoformat()

    logger.info(f"日线预热窗口: {start} ~ {end}（标准{STANDARD_WINDOW_DAYS}天"
                f"+{STANDARD_WARMUP_DAYS}天热身缓冲）")
    logger.info(f"60分钟线预热窗口: {hourly_start} ~ {end}"
                f"（受yfinance约{HOURLY_MAX_HISTORY_DAYS}天上限约束）")

    overall_start = time.time()
    daily_result = cache.warm_batch(tickers, "daily", start, end,
                                    batch_size=BATCH_SIZE, max_minutes=max_minutes)

    if daily_result["circuit_broken"] and push_telegram:
        send_telegram(f"🔴 data_fetcher.py熔断 [backfill/daily]\n"
                      f"{daily_result['circuit_break_message']}", logger)

    remaining_minutes = None
    if max_minutes is not None:
        elapsed_min = (time.time() - overall_start) / 60
        remaining_minutes = max(0.0, max_minutes - elapsed_min)

    if daily_result["circuit_broken"] or (remaining_minutes is not None and remaining_minutes <= 0):
        if not daily_result["circuit_broken"]:
            logger.info("日线阶段已用完全部时间预算，本次跳过60分钟线预热，下次重跑会继续")
        hourly_result = {"fetched_ok": 0, "fetched_fail": 0, "already_cached": 0,
                         "circuit_broken": False, "circuit_break_message": None}
    else:
        hourly_result = cache.warm_batch(tickers, "60m", hourly_start, end,
                                         batch_size=BATCH_SIZE, max_minutes=remaining_minutes)
        if hourly_result["circuit_broken"] and push_telegram:
            send_telegram(f"🔴 data_fetcher.py熔断 [backfill/60m]\n"
                          f"{hourly_result['circuit_break_message']}", logger)

    return {
        "total": len(tickers),
        "daily_ok": daily_result["fetched_ok"] + daily_result["already_cached"],
        "daily_fail": daily_result["fetched_fail"],
        "hourly_ok": hourly_result["fetched_ok"] + hourly_result["already_cached"],
        "hourly_fail": hourly_result["fetched_fail"],
        "circuit_broken": daily_result["circuit_broken"] or hourly_result["circuit_broken"],
    }


def run_weekly_15m(cache: "mdc.MarketDataCache", tickers: list[str],
                   max_minutes: Optional[float], logger: logging.Logger,
                   push_telegram: bool = True) -> dict:
    """
    拉取全市场15分钟线最近~59天窗口，合并进逐票累积的归档。v1.2改为
    委托给cache.warm_batch()，用批量请求替代逐票单独请求，原因同
    run_backfill()——决定耗时的是请求次数，不是每次请求的数据范围。

    15分钟颗粒度本身有一定的正常背景失败率（部分股票在这个颗粒度上
    yfinance数据源确实缺失intraday覆盖，不是bug），warm_batch()内部
    的熔断判断是按"批次"计数（连续几批50只全部失败），这个信号比
    "单只股票没数据"强得多，不会被正常的背景噪音误触发。
    """
    end = pd.Timestamp.now().normalize().date().isoformat()
    start = (pd.Timestamp.now().normalize()
             - pd.Timedelta(days=WEEKLY_15M_WINDOW_DAYS)).date().isoformat()
    logger.info(f"15分钟线增量窗口: {start} ~ {end}（yfinance约60天硬上限内）")

    result = cache.warm_batch(tickers, "15m", start, end,
                              batch_size=BATCH_SIZE, max_minutes=max_minutes)

    if result["circuit_broken"] and push_telegram:
        send_telegram(f"🔴 data_fetcher.py熔断 [weekly15m]\n"
                      f"{result['circuit_break_message']}", logger)

    return {
        "total": len(tickers),
        "ok": result["fetched_ok"] + result["already_cached"],
        "fail": result["fetched_fail"],
        "circuit_broken": result["circuit_broken"],
    }


def print_coverage(cache: "mdc.MarketDataCache") -> None:
    for granularity in ("daily", "60m", "15m"):
        df = cache.coverage_summary(granularity)
        if df.empty:
            print(f"\n[{granularity}] 缓存为空")
            continue
        print(f"\n[{granularity}] 已缓存 {len(df)} 只股票")
        print(f"  最早覆盖: {df['earliest_date'].min()}")
        print(f"  最晚覆盖: {df['latest_date'].max()}")
        print(f"  最近更新: {df['last_updated_at'].max()}")


def main():
    parser = argparse.ArgumentParser(description="ASX行情数据本地缓存预热/累积工具")
    parser.add_argument("--mode", choices=["backfill", "weekly15m"], default=None,
                        help="backfill: 预热日线+60分钟线标准窗口；"
                             "weekly15m: 增量累积15分钟线（建议crontab每周跑）")
    parser.add_argument("--universe", choices=["full", "file"], default="full")
    parser.add_argument("--universe-file", default="")
    parser.add_argument("--max-minutes", type=float, default=None,
                        help="本次运行时间预算（分钟），到点优雅停止，下次原样重跑自动接着补")
    parser.add_argument("--coverage", action="store_true",
                        help="只打印当前缓存覆盖情况，不跑任何抓取")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    log_path = os.path.join(_SCRIPT_DIR, "data_fetcher.log")
    logger = setup_logging(log_path)
    push = not args.no_telegram

    cache = mdc.MarketDataCache(logger=logger)

    if args.coverage:
        print_coverage(cache)
        return

    if not args.mode:
        logger.error("必须指定 --mode backfill 或 --mode weekly15m（或用 --coverage 只看现状）")
        return

    tickers = resolve_universe(args.universe, args.universe_file, logger)
    if not tickers:
        logger.error("universe为空，终止")
        if push:
            send_telegram("🔴 data_fetcher.py：universe为空，终止", logger)
        return

    start_msg = f"🚀 data_fetcher.py 启动 [{args.mode}]，universe={len(tickers)}只"
    logger.info(start_msg)
    if push:
        send_telegram(start_msg, logger)

    try:
        if args.mode == "backfill":
            result = run_backfill(cache, tickers, args.max_minutes, logger, push_telegram=push)
            status = "🛑 因熔断提前停止" if result.get("circuit_broken") else "✅ backfill完成"
            summary = (
                f"{status}：universe共{result['total']}只\n"
                f"日线成功{result['daily_ok']}/失败{result['daily_fail']}\n"
                f"60分钟线成功{result['hourly_ok']}/失败{result['hourly_fail']}"
            )
        else:
            result = run_weekly_15m(cache, tickers, args.max_minutes, logger, push_telegram=push)
            status = "🛑 因熔断提前停止" if result.get("circuit_broken") else "✅ weekly15m完成"
            summary = (
                f"{status}：universe共{result['total']}只\n"
                f"成功{result['ok']}/失败{result['fail']}"
            )
        logger.info(summary.replace("\n", " | "))
        if push:
            send_telegram(summary, logger)
    except Exception as e:
        tb = traceback.format_exc()
        logger.critical(f"data_fetcher.py崩溃: {e}\n{tb}")
        if push:
            send_telegram(f"🔴 data_fetcher.py崩溃: {e}\n{tb[-1000:]}", logger)
        sys.exit(1)


if __name__ == "__main__":
    main()
