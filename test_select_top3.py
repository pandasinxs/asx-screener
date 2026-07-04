import screener as sc

def main():
    print("正在下载市场快照...")
    market_snap = sc.get_market_snapshot()
    
    print("正在获取股票池...")
    universe = sc.get_asx_universe()
    if not universe:
        print("错误：股票池为空")
        return
    
    print(f"正在下载{len(universe)}只股票K线（需要几分钟）...")
    all_data = sc.download_ohlcv(universe, period="1y")
    
    print(f"K线完成：{len(all_data)}只有效")
    print()
    
    signals, raw_signals, tier_label, tier_level = sc.select_top3(
        all_data, market_snap, write_to_db=True
    )
    
    print(f"=== 筛选结果 ===")
    print(f"层级分布: {tier_label}")
    print(f"Top{len(signals)}:")
    for i, s in enumerate(signals, 1):
        print(f"  #{i} {s['ticker']} [{s['tier_level']}] "
              f"评分={s['composite_score']} "
              f"趋势强度={s.get('trend_strength_score', 'N/A')} "
              f"持续性={s.get('persistence_score', 0)}")
    
    print()
    print(f"Top10候选全部({len(raw_signals)}只):")
    for s in raw_signals:
        print(f"  {s['ticker']} [{s['tier_level']}] 评分={s['composite_score']}")

if __name__ == "__main__":
    main()
