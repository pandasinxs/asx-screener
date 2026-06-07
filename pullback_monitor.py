# ============================================================
# FIRST PULLBACK — 盘中回踩监控 v2
# 优化：更严格的入场条件 + 首次信号限制
# 运行：每15分钟，11:00am - 4:00pm AEST
# ============================================================

import os, json, time
import yfinance as yf
import requests
from datetime import datetime, date

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

# ── 计算VWAP ──────────────────────────────────────────────────
def calc_vwap(df):
    c  = df['Close'].squeeze()
    h  = df['High'].squeeze()
    l  = df['Low'].squeeze()
    v  = df['Volume'].squeeze()
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
    today   = date.today().strftime('%Y-%m-%d')
    utc_h   = datetime.utcnow().hour
    utc_m   = datetime.utcnow().minute
    aest_h  = (utc_h + 10) % 24
    now_str = f"{aest_h:02d}:{utc_m:02d} AEST"

    print(f"\n[{today} {now_str}] Pullback Monitor 运行...")

    # 读取今日watchlist
    try:
        with open(WATCHLIST_FILE) as f:
            wl = json.load(f)
    except:
        print("watchlist.json 不存在，跳过")
        return

    if wl.get('date') != today:
        print("今日无watchlist")
        return

    stocks = wl.get('stocks', [])
    if not stocks:
        print("Watchlist为空")
        return

    # 读取已提醒记录（每只股票只发一次）
    try:
        with open(ALERTED_FILE) as f:
            alerted_data = json.load(f)
        if alerted_data.get('date') != today:
            alerted_data = {'date': today, 'alerted': []}
    except:
        alerted_data = {'date': today, 'alerted': []}

    alerted_set = set(alerted_data.get('alerted', []))
    new_alerts  = []

    for stock in stocks:
        ticker = stock['ticker']

        # 每只股票只发一次（首次回踩）
        if ticker in alerted_set:
            continue

        try:
            intra = yf.download(ticker, period='1d', interval='5m',
                                progress=False)
            if intra is None or intra.empty or len(intra) < 6:
                continue

            close  = intra['Close'].squeeze()
            high   = intra['High'].squeeze()
            low    = intra['Low'].squeeze()
            volume = intra['Volume'].squeeze()

            curr_price  = float(close.iloc[-1])
            today_high  = float(high.max())
            today_low   = float(low.min())           # 当日最低点（新版止损基准）
            prev_close  = stock.get('prev_close', curr_price)

            # 计算VWAP
            vwap_series = calc_vwap(intra)
            curr_vwap   = float(vwap_series.iloc[-1])

            # ── 条件1：今日仍保持≥5%涨幅（催化剂有效）
            if prev_close > 0 and curr_price < prev_close * 1.05:
                print(f"  ❌ {ticker}: 涨幅已大幅收窄")
                continue

            # ── 条件2：当前价格在VWAP附近（-0.5% ~ +2%）【收窄】
            vwap_gap = (curr_price - curr_vwap) / curr_vwap
            if not (-0.005 <= vwap_gap <= 0.02):
                status = "未回踩到位" if vwap_gap > 0.02 else "已跌破VWAP"
                print(f"  ⏳ {ticker}: {status}（现价{round(vwap_gap*100,1)}% vs VWAP）")
                continue

            # ── 条件3：回调过程显著缩量（<65%）【收严】
            recent_vol = float(volume.iloc[-3:].mean())
            prior_vol  = float(volume.iloc[-6:-3].mean())
            if prior_vol <= 0 or recent_vol >= prior_vol * 0.65:
                print(f"  ❌ {ticker}: 缩量不明显（{round(recent_vol/prior_vol*100 if prior_vol>0 else 100)}% 前段量）")
                continue

            # ── 条件4：价格不跌破当日最低点【改为当日低点】
            if curr_price <= today_low * 1.002:  # 留0.2%缓冲
                print(f"  ❌ {ticker}: 跌破当日最低点${today_low}")
                continue

            # ── 所有条件满足 → 发出首次回踩提示 ──────────────
            change_pct  = round((curr_price - prev_close) / prev_close * 100, 1)
            vol_decline = round((1 - recent_vol / prior_vol) * 100) if prior_vol > 0 else 0
            vwap_pct    = round(vwap_gap * 100, 1)
            target1     = round(curr_price * 1.10, 3)
            target2     = round(curr_price * 1.20, 3)
            stop        = round(today_low * 0.99, 3)
            ann_info    = stock.get('ann_headline') or "请手动核查公告"

            msg = (
                f"🎯 <b>First Pullback 入场提示 — {ticker}</b>\n"
                f"⏰ {now_str}\n\n"
                f"💰 现价：${curr_price}  今日 +{change_pct}%\n"
                f"📊 VWAP：${round(curr_vwap, 3)}"
                f"（现价在VWAP上方{vwap_pct}%）\n"
                f"📉 回调缩量：{vol_decline}%\n"
                f"🔻 当日最低（止损基准）：${today_low}\n\n"
                f"🎯 目标1 +10%（锁半仓）：${target1}\n"
                f"🎯 目标2 +20%（清仓）：${target2}\n"
                f"🛑 止损：跌破 ${stop}\n\n"
                f"📋 催化剂：{ann_info}\n\n"
                f"❓ 入场前确认：\n"
                f"1. 公告是真实催化剂？\n"
                f"2. 这是今日第一次回踩VWAP？\n"
                f"3. 大盘没有明显走弱？\n"
                f"→ 三个YES才入场"
            )

            send_telegram(msg)
            new_alerts.append(ticker)
            alerted_set.add(ticker)
            print(f"  🎯 提醒：{ticker} @ ${curr_price} "
                  f"(VWAP ${round(curr_vwap,3)}, 缩量{vol_decline}%)")
            time.sleep(1)

        except Exception as e:
            print(f"  ⚠️  {ticker} 出错: {e}")

    # 更新已提醒记录
    alerted_data['alerted'] = list(alerted_set)
    with open(ALERTED_FILE, 'w') as f:
        json.dump(alerted_data, f, indent=2)

    if not new_alerts:
        print(f"  本次无新提示（监控{len(stocks)}只，已提醒{len(alerted_set)}只）")
    else:
        print(f"  ✅ 本次发送{len(new_alerts)}个提示：{new_alerts}")

if __name__ == '__main__':
    run_monitor()