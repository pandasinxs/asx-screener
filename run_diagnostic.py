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
    
    print(f"K线下载完成：{len(all_data)}只有效")
    print()
    
    print("=== 条件失败率诊断 ===")
    sc.run_tier_diagnostic(all_data, market_snap, tier_levels=["T1", "T2"])
    
    print()
    print("=== 趋势强度评分阈值扫描 ===")
    sc.run_threshold_scan(all_data, market_snap, tier_levels=["T1", "T2", "T3", "T4"])

if __name__ == "__main__":
    main()
