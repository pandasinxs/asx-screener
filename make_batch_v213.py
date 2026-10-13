#!/usr/bin/env python3
"""
make_batch_v213.py —— 生成"流动性门槛 + T4下限门槛"这一批8个实验的params文件和队列文件。

用法（在VM上）:
    cd ~/asx && python3 make_batch_v213.py

做什么：
  1. 读取 ~/asx/params.json（生产配置），先核对关键基线值没被改过；
  2. 对每个实验，复制一份完整的生产配置，只改一个变量，写到
     ~/asx/backtest-configs/params/<实验名>.json；
  3. 写队列文件 ~/asx/backtest-configs/queue_liq_t4floor_v213.txt（universe=full）。
不修改params.json本身、不修改任何生产文件、不启动回测。
"""
import copy
import json
import os
import sys

BASE_DIR = os.path.expanduser(os.environ.get("ASX_HOME", "~/asx"))
SRC = os.path.join(BASE_DIR, "params.json")
OUT_DIR = os.path.join(BASE_DIR, "backtest-configs")
PARAMS_DIR = os.path.join(OUT_DIR, "params")
QUEUE = os.path.join(OUT_DIR, "queue_liq_t4floor_v213.txt")

# 默认值（screener.py里的常量；生产params.json没写这个键时就是它）
DEFAULT_MIN_LIQ = 300_000.0

# (实验名, 描述, 修改函数)
EXPERIMENTS = [
    ("v213_liq_x0.5", "流动性门槛 30万→15万（放宽）",   lambda p: p.__setitem__("MIN_LIQUIDITY_DOLLAR_VOL", DEFAULT_MIN_LIQ * 0.5)),
    ("v213_liq_x2",   "流动性门槛 30万→60万（收紧）",   lambda p: p.__setitem__("MIN_LIQUIDITY_DOLLAR_VOL", DEFAULT_MIN_LIQ * 2)),
    ("v213_liq_x3",   "流动性门槛 30万→90万（更紧）",   lambda p: p.__setitem__("MIN_LIQUIDITY_DOLLAR_VOL", DEFAULT_MIN_LIQ * 3)),
    ("v213_t4_adx12", "T4 adx_min 15→12（放宽）",       lambda p: p["TIERS"]["T4"].__setitem__("adx_min", 12)),
    ("v213_t4_adx18", "T4 adx_min 15→18（收紧）",       lambda p: p["TIERS"]["T4"].__setitem__("adx_min", 18)),
    ("v213_t4_consol30", "T4 consol 0.25→0.30（放宽）", lambda p: p["TIERS"]["T4"].__setitem__("consol", 0.30)),
    ("v213_t4_consol20", "T4 consol 0.25→0.20（收紧）", lambda p: p["TIERS"]["T4"].__setitem__("consol", 0.20)),
    ("v213_t4_closepos40", "T4 close_pos 0.50→0.40（放宽）", lambda p: p["TIERS"]["T4"].__setitem__("close_pos", 0.40)),
]


def main() -> int:
    if not os.path.exists(SRC):
        print(f"❌ 找不到 {SRC}"); return 1
    with open(SRC, encoding="utf-8") as f:
        base = json.load(f)

    # ── 核对这确实是当前的生产基线（交接文档里记录的、已验证的覆盖值）──
    problems = []
    if base.get("BT_STOP_ATR_MULT") != 2.5: problems.append(f"BT_STOP_ATR_MULT={base.get('BT_STOP_ATR_MULT')}，应为2.5")
    if base.get("TIER_BONUS", {}).get("T1") != 0.075: problems.append(f"TIER_BONUS.T1={base.get('TIER_BONUS', {}).get('T1')}，应为0.075")
    if base.get("TREND_SCORE_THRESHOLD", {}).get("T1") != 0.58: problems.append(f"TREND_SCORE_THRESHOLD.T1={base.get('TREND_SCORE_THRESHOLD', {}).get('T1')}，应为0.58")
    t4 = base.get("TIERS", {}).get("T4", {})
    if (t4.get("adx_min"), t4.get("consol"), t4.get("close_pos")) != (15, 0.25, 0.5):
        problems.append(f"TIERS.T4 的 adx_min/consol/close_pos = {(t4.get('adx_min'), t4.get('consol'), t4.get('close_pos'))}，应为(15, 0.25, 0.5)")
    if "MIN_LIQUIDITY_DOLLAR_VOL" in base and base["MIN_LIQUIDITY_DOLLAR_VOL"] != DEFAULT_MIN_LIQ:
        problems.append(f"MIN_LIQUIDITY_DOLLAR_VOL={base['MIN_LIQUIDITY_DOLLAR_VOL']}，应为{DEFAULT_MIN_LIQ:g}（或不写）")
    if problems:
        print("❌ params.json 与记录的生产基线不一致，已中止（没有写任何文件）：")
        for p in problems: print("   -", p)
        print("   如果这是你有意改过的新基线，把这段输出贴给我，我来调整脚本。")
        return 1

    os.makedirs(PARAMS_DIR, exist_ok=True)
    lines = ["# v2.13批次：流动性门槛 + T4下限门槛，基线=生产params.json，每个实验只改一个变量",
             "# 格式: 实验名,参数文件(相对本文件所在目录),universe",
             "# 运行时必须带 --end 2026-05-31（样本内），不要碰holdout"]
    for name, desc, mutate in EXPERIMENTS:
        p = copy.deepcopy(base)
        mutate(p)
        path = os.path.join(PARAMS_DIR, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(p, f, ensure_ascii=False, indent=2)
        lines.append(f"{name},params/{name}.json,full")
        print(f"  ✓ {name:20s} {desc}")
    with open(QUEUE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n✅ 已生成 {len(EXPERIMENTS)} 个实验，队列文件: {QUEUE}")
    print("下一步（后台运行，不要加--max-minutes）：")
    print(f"  source ~/.asx_env && cd {BASE_DIR}")
    print(f"  nohup python3 backtest_engine.py --run-queue {QUEUE} --end 2026-05-31 > queue_v213.log 2>&1 &")
    return 0


if __name__ == "__main__":
    sys.exit(main())
