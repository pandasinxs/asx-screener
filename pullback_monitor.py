# ============================================================
# FIRST PULLBACK — 盘中回踩监控
# 每30分钟运行：11:00am - 1:30pm AEST (01:00-03:30 UTC)
# 检测候选股是否回踩至VWAP + 缩量 + 不破启动点
# ============================================================

import os, json, time
import yfinance as yf
import requests
from datetime import datetime, date

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "你的TOKEN")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "7553937057")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

# ── 计算VWAP ──────────────────────────────────────────────────
def calc_vwap(df):
    c = df['Close'].squeeze()
    h = df['High'].squeeze()
    l = df['Low'].squeeze()
    v = df['Volume'].squeeze()
    tp = (h + l + c) / 3
    return (tp * v).cumsum() / v.cumsum()

# ── Telegram ─────────────────────────────────────────────────
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg,
                                 "parse_mode": "HTML"}, timeout=10)
    except: pass

# ── 主程序 ───────────────────────────────────────────────────
def run_monitor():
    today    = date.today().strftime('%Y-%m-%d')
    now_aest = datetime.utcnow()
    now_str  = f"{(now_aest.hour + 10) % 24:02d}:{now_aest.minute:02d} AEST"

    print(f"\n[{today} {now_str}] Pullback Monitor 运行...")

    # 读取今日watchlist
    try:
        with open(WATCHLIST_FILE) as f:
            wl = json.load(f)
    except:
        print("watchlist.json 不存在，跳过")
        return

    if wl.get('date') != today:
        print("今日无watchlist（morning scanner尚未运行或日期不符）")
        return

    stocks = wl.get('stocks', [])
    if not stocks:
        print("Watchlist为空")
        return

    # 读取今日已提醒记录（避免重复提醒同一只股票）
    try:
        with open(ALERTED_FILE) as f:
            alerted_data = json.load(f)
        if alerted_data.get('date') != today:
            alerted_data = {'date': today, 'alerted': []}
    except:
        alerted_data = {'date': today, 'alerted': []}

    alerted_set = set(alerted_data.get('alerted', []))
    new_alerts  = []

    print(f"监控 {len(stocks)} 只候选股，已发送过提醒：{len(alerted_set)} 只")

    for stock in stocks:
        ticker = stock['ticker']

        # 跳过已提醒的股票
        if ticker in alerted_set:
            print(f"  ⏭  {ticker}: 今日已发送过提醒")
            continue

        try:
            # 下载今日5分钟数据
            intra = yf.download(ticker, period='1d', interval='5m',
                                progress=False)
            if intra is None or intra.empty or len(intra) < 6:
                print(f"  ⚠️  {ticker}: 盘中数据不足")
                continue

            close  = intra['Close'].squeeze()
            high   = intra['High'].squeeze()
            low    = intra['Low'].squeeze()
            volume = intra['Volume'].squeeze()

            curr_price = float(close.iloc[-1])
            today_high = float(high.max())
            launch_pt  = stock.get('launch_pt', float(low.iloc[0]))
            prev_close = stock.get('prev_close', curr_price)

            # 计算VWAP
            vwap_series = calc_vwap(intra)
            curr_vwap   = float(vwap_series.iloc[-1])

            # ── 四个入场条件 ─────────────────────────────────

            # 条件1：今日高点距VWAP至少5%（确认有过明显拉升）
            spike_above_vwap = (today_high - curr_vwap) / curr_vwap
            if spike_above_vwap < 0.05:
                print(f"  ❌ {ticker}: 拉升幅度不够（高点仅{round(spike_above_vwap*100,1)}% > VWAP）")
                continue

            # 条件2：当前价格回踩至VWAP附近（VWAP上方0-4%之间）
            vwap_gap = (curr_price - curr_vwap) / curr_vwap
            if not (-0.005 <= vwap_gap <= 0.04):
                print(f"  ⏳ {ticker}: 尚未回踩到VWAP（现价{round(vwap_gap*100,1)}% vs VWAP）")
                continue

            # 条件3：回调过程缩量（近3根K线均量 < 前3根K线均量的80%）
            recent_vol = float(volume.iloc[-3:].mean())
            prior_vol  = float(volume.iloc[-6:-3].mean())
            if prior_vol <= 0 or recent_vol >= prior_vol * 0.80:
                print(f"  ❌ {ticker}: 回调未缩量（近:{round(recent_vol)} 前:{round(prior_vol)}）")
                continue

            # 条件4：价格不跌破启动点（早盘第一根K线低点）
            if curr_price <= launch_pt:
                print(f"  ❌ {ticker}: 已跌破启动点${launch_pt}")
                continue

            # 条件5：今日仍然保持涨幅（至少比前收高5%）
            if prev_close > 0 and curr_price < prev_close * 1.05:
                print(f"  ❌ {ticker}: 涨幅已大幅收窄")
                continue

            # ── 所有条件满足，发送入场提示 ───────────────────

            change_pct   = round((curr_price - prev_close) / prev_close * 100, 1)
            vol_decline  = round((1 - recent_vol / prior_vol) * 100) if prior_vol > 0 else 0
            vwap_pct     = round(vwap_gap * 100, 1)
            target1      = round(curr_price * 1.10, 3)
            target2      = round(curr_price * 1.20, 3)
            stop         = round(launch_pt * 0.99, 3)
            ann_info     = stock.get('ann_title') or "请手动核查公告"

            msg = (
                f"🎯 <b>First Pullback 入场提示 — {ticker}</b>\n"
                f"⏰ {now_str}\n\n"
                f"💰 现价：${curr_price}  （今日 +{change_pct}%）\n"
                f"📊 VWAP：${round(curr_vwap, 3)}"
                f"（价格在VWAP上方 {vwap_pct}%）\n"
                f"📉 回调缩量：{vol_decline}%\n"
                f"🚀 启动点（止损基准）：${launch_pt}\n\n"
                f"🎯 目标1 +10%（锁半仓）：${target1}\n"
                f"🎯 目标2 +20%（清仓）：${target2}\n"
                f"🛑 止损：跌破 ${stop} 立刻出\n\n"
                f"📋 催化剂：{ann_info}\n\n"
                f"❓ 入场前确认三个问题：\n"
                f"1. 这个公告是真实催化剂？\n"
                f"2. 主力没有出货迹象？\n"
                f"3. 这是当天第一次回踩VWAP？\n"
                f"→ 三个YES才入场"
            )

            send_telegram(msg)
            new_alerts.append(ticker)
            alerted_set.add(ticker)
            print(f"  🎯 提醒发送：{ticker} @ ${curr_price} "
                  f"(VWAP ${round(curr_vwap,3)}, 缩量{vol_decline}%)")

            time.sleep(1)

        except Exception as e:
            print(f"  ⚠️  {ticker} 出错: {e}")

    # 更新已提醒记录
    alerted_data['alerted'] = list(alerted_set)
    with open(ALERTED_FILE, 'w') as f:
        json.dump(alerted_data, f, indent=2)

    if not new_alerts:
        print(f"  本次运行无新提示")
    else:
        print(f"  ✅ 本次发送 {len(new_alerts)} 个提示：{new_alerts}")

if __name__ == '__main__':
    run_monitor()