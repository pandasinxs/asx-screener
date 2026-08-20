#!/usr/bin/env python3
"""
check_15m_coverage.py
======================
一次性诊断脚本：backtest_intraday.py（Stage1）产出的
intraday_health_backtest里，health_status为ready/pullback_bottoming
的那些(ticker, 日期)——也就是Stage2真正会尝试跑15分钟检测的那批
候选——到底有多少能在本地market_data_cache的15分钟Parquet缓存里
真的找到数据。

这是决定"现在动手写Stage2划不划算"的关键数字。show_stats()本身
不检查这一层（只统计理论上"该被监测"的天数，不检查数据是否真的
到位），这个脚本补上这个盲区。

不是核心pipeline的一部分，只是一次性诊断用，跑完看数字就好，
不需要接入crontab或者其他自动化流程。

用法：
    python3 check_15m_coverage.py --param-set my_v1_intraday
"""
import argparse
import os
import sqlite3

import pandas as pd

ASX_DIR = os.path.dirname(os.path.abspath(__file__))
BACKTEST_DB = os.path.join(ASX_DIR, "backtest_results.db")
CACHE_MANIFEST_DB = os.path.join(ASX_DIR, "market_data_cache", "cache_manifest.db")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage1候选天数 vs 本地15分钟数据实际覆盖情况诊断")
    parser.add_argument("--param-set", required=True, help="Stage1的--param-set-name（不是--eod-param-set）")
    args = parser.parse_args()

    if not os.path.exists(BACKTEST_DB):
        print(f"数据库不存在: {BACKTEST_DB}")
        return
    if not os.path.exists(CACHE_MANIFEST_DB):
        print(f"缓存清单数据库不存在: {CACHE_MANIFEST_DB}"
              f"（还没跑过data_fetcher.py？或者market_data_cache目录不在默认位置？）")
        return

    conn = sqlite3.connect(BACKTEST_DB)
    health_df = pd.read_sql_query("""
        SELECT ticker, trading_date, health_status
        FROM intraday_health_backtest
        WHERE param_set = ? AND health_status IN ('ready', 'pullback_bottoming')
    """, conn, params=[args.param_set])
    conn.close()

    if health_df.empty:
        print(f"param_set={args.param_set} 在intraday_health_backtest里"
              f"没有ready/pullback_bottoming记录，先确认Stage1跑完了、"
              f"--param-set传的是Stage1自己的标签（不是--eod-param-set那个名字）")
        return

    cache_conn = sqlite3.connect(CACHE_MANIFEST_DB)
    coverage_df = pd.read_sql_query("""
        SELECT ticker, earliest_date, latest_date
        FROM cache_coverage WHERE granularity = '15m'
    """, cache_conn)
    cache_conn.close()

    coverage_map = {
        row.ticker: (row.earliest_date, row.latest_date)
        for row in coverage_df.itertuples()
    }

    def has_15m(row) -> bool:
        rng = coverage_map.get(row["ticker"])
        if rng is None:
            return False
        earliest, latest = rng
        return earliest <= row["trading_date"] <= latest

    health_df["has_15m"] = health_df.apply(has_15m, axis=1)

    total = len(health_df)
    covered = int(health_df["has_15m"].sum())

    print("=" * 60)
    print(f"15分钟数据覆盖诊断 [param_set={args.param_set}]")
    print("=" * 60)
    print(f"ready/pullback_bottoming总天数: {total}")
    print(f"本地15分钟缓存已覆盖: {covered} ({covered/total*100:.1f}%)")
    print(f"尚未覆盖（Stage2现在测不到）: {total-covered} ({(total-covered)/total*100:.1f}%)")

    print("\n按health_status拆分：")
    for status, g in health_df.groupby("health_status"):
        cov = int(g["has_15m"].sum())
        print(f"  {status}: {len(g)}天，覆盖{cov}天 ({cov/len(g)*100:.1f}%)")

    per_ticker = health_df.groupby("ticker").agg(
        total_days=("has_15m", "size"), covered_days=("has_15m", "sum")
    )
    per_ticker["coverage_pct"] = per_ticker["covered_days"] / per_ticker["total_days"] * 100
    qualifying = per_ticker[per_ticker["total_days"] >= 3].sort_values("coverage_pct")

    if not qualifying.empty:
        print(f"\n候选天数≥3天、但覆盖率最低的股票（最多显示10只，"
              f"这些是样本量够但数据缺口大的、最值得关注的情况）：")
        print(qualifying.head(10).to_string())
    else:
        print("\n没有候选天数≥3天的股票，样本普遍很薄")


if __name__ == "__main__":
    main()
