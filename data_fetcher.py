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
                 max_minutes: Optional[float], logger: logging.Logger) -> dict:
    """
    预热日线+60分钟线到标准回测窗口。对每只股票分别调用
    cache.get_daily()/get_60m()——已经完整覆盖的股票这两次调用完全
    不碰网络，直接从本地Parquet返回，几乎瞬间跳过；只有真正有缺口
    的股票才会实际发请求。

    天然幂等、天然支持断点续跑：不需要像backtest_engine.py那样
    额外维护一张进度表——"这只股票的本地缓存是否已经覆盖所需区间"
    这件事本身就是进度状态，被max_minutes打断后原样重跑同一条命令
    即可，已经覆盖的股票会立刻跳过，只继续处理还没处理到的。
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

    start_time = time.time()
    daily_ok = daily_fail = hourly_ok = hourly_fail = 0
    processed = 0

    for i, ticker in enumerate(tickers):
        if max_minutes is not None and (time.time() - start_time) / 60 >= max_minutes:
            logger.info(
                f"达到时间预算({max_minutes}分钟)，本次预热提前结束，"
                f"已处理{processed}/{len(tickers)}只，下次原样重跑会自动跳过"
                f"已经覆盖的部分，接着补剩下的"
            )
            break

        if i % 50 == 0 and i > 0:
            elapsed = time.time() - start_time
            logger.info(
                f"进度 {i}/{len(tickers)}，已用{elapsed/60:.1f}分钟 | "
                f"日线成功{daily_ok}/失败{daily_fail} | "
                f"60分钟线成功{hourly_ok}/失败{hourly_fail}"
            )

        try:
            df = cache.get_daily(ticker, start, end)
            if df is not None:
                daily_ok += 1
            else:
                daily_fail += 1
        except Exception as e:
            daily_fail += 1
            logger.warning(f"日线预热异常 [{ticker}]: {e}")

        try:
            df60 = cache.get_60m(ticker, hourly_start, end)
            if df60 is not None:
                hourly_ok += 1
            else:
                hourly_fail += 1
        except Exception as e:
            hourly_fail += 1
            logger.warning(f"60分钟线预热异常 [{ticker}]: {e}")

        processed += 1
        time.sleep(0.3)  # 轻微限速，避免对yfinance过快连续请求触发限流

    return {
        "processed": processed, "total": len(tickers),
        "daily_ok": daily_ok, "daily_fail": daily_fail,
        "hourly_ok": hourly_ok, "hourly_fail": hourly_fail,
    }


def run_weekly_15m(cache: "mdc.MarketDataCache", tickers: list[str],
                   max_minutes: Optional[float], logger: logging.Logger) -> dict:
    """
    拉取全市场15分钟线最近~59天窗口，合并进逐票累积的归档。每只股票
    的15分钟缓存只要曾经覆盖过某段历史，这次只会补"缓存末尾到现在"
    这一小段新增数据（get_15m内部的缺口检测逻辑），不会重新下载整个
    59天窗口——除非这只股票是第一次被拉取。
    """
    end = pd.Timestamp.now().normalize().date().isoformat()
    start = (pd.Timestamp.now().normalize()
             - pd.Timedelta(days=WEEKLY_15M_WINDOW_DAYS)).date().isoformat()
    logger.info(f"15分钟线增量窗口: {start} ~ {end}（yfinance约60天硬上限内）")

    start_time = time.time()
    ok = fail = 0
    processed = 0

    for i, ticker in enumerate(tickers):
        if max_minutes is not None and (time.time() - start_time) / 60 >= max_minutes:
            logger.info(
                f"达到时间预算({max_minutes}分钟)，本次提前结束，"
                f"已处理{processed}/{len(tickers)}只，剩下的下次运行"
                f"（比如下周同一个crontab任务）会自动补上——只要总断档"
                f"不超过~60天，不会有永久缺口"
            )
            break

        if i % 50 == 0 and i > 0:
            elapsed = time.time() - start_time
            fail_pct = fail / (ok + fail) * 100 if (ok + fail) > 0 else 0.0
            logger.info(
                f"进度 {i}/{len(tickers)}，已用{elapsed/60:.1f}分钟 | "
                f"成功{ok}/失败{fail}（失败率{fail_pct:.1f}%）"
            )

        try:
            df = cache.get_15m(ticker, start, end)
            if df is not None:
                ok += 1
            else:
                fail += 1
        except Exception as e:
            fail += 1
            logger.warning(f"15分钟线增量拉取异常 [{ticker}]: {e}")

        processed += 1
        time.sleep(0.3)

    return {"processed": processed, "total": len(tickers), "ok": ok, "fail": fail}


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
            result = run_backfill(cache, tickers, args.max_minutes, logger)
            summary = (
                f"✅ backfill完成：{result['processed']}/{result['total']}只\n"
                f"日线成功{result['daily_ok']}/失败{result['daily_fail']}\n"
                f"60分钟线成功{result['hourly_ok']}/失败{result['hourly_fail']}"
            )
        else:
            result = run_weekly_15m(cache, tickers, args.max_minutes, logger)
            summary = (
                f"✅ weekly15m完成：{result['processed']}/{result['total']}只\n"
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
