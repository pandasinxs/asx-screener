# ============================================================
# ASX SWING TRADE SCREENER v6
# 分级筛选：T1最严 → T4最宽，确保每天至少有参考信号
# ============================================================

# Cell 1: !pip install yfinance requests -q

import os
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "你的TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "7553937057")

# ── 四个筛选等级参数 ──────────────────────────────────────────
TIERS = [
    {
        "level": "T1", "label": "🔴 最严格",
        "vol_mult": 2.0, "close_pos": 0.90,
        "obv_pct": 0.95, "rsi_lo": 45, "rsi_hi": 65,
        "consol": 0.12, "note": "最高质量信号"
    },
    {
        "level": "T2", "label": "🟡 标准",
        "vol_mult": 1.5, "close_pos": 0.75,
        "obv_pct": 0.85, "rsi_lo": 42, "rsi_hi": 68,
        "consol": 0.15, "note": "标准质量，今日无T1信号"
    },
    {
        "level": "T3", "label": "🟢 放宽",
        "vol_mult": 1.2, "close_pos": 0.60,
        "obv_pct": 0.75, "rsi_lo": 38, "rsi_hi": 72,
        "consol": 0.20, "note": "参考信号，需更谨慎判断"
    },
    {
        "level": "T4", "label": "⚪ 观察",
        "vol_mult": 1.0, "close_pos": 0.50,
        "obv_pct": 0.65, "rsi_lo": 35, "rsi_hi": 75,
        "consol": 0.25, "note": "仅供观察，不建议直接入场"
    },
]

# ── 动态获取ASX股票池 ─────────────────────────────────────────
def get_asx_universe():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        df = pd.read_csv(url, skiprows=1, encoding='latin1')
        code_col = next((c for c in df.columns if 'code' in c.lower()), None)
        if code_col is None:
            return []
        codes = df[code_col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        print(f"ASX上市公司：{len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

# ── 指标计算 ──────────────────────────────────────────────────
def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = -delta.clip(upper=0).rolling(period).mean()
    rs = gain / loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))

def calc_obv(close, volume):
    direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (direction * volume).cumsum()

def calc_mfi(high, low, close, volume, period=14):
    typical = (high + low + close) / 3
    raw_mf = typical * volume
    pos_mf = raw_mf.where(typical > typical.shift(1), 0).rolling(period).sum()
    neg_mf = raw_mf.where(typical < typical.shift(1), 0).rolling(period).sum()
    mfr = pos_mf / neg_mf.replace(0, 1e-10)
    return 100 - (100 / (1 + mfr))

# ── 大盘状态 ──────────────────────────────────────────────────
def check_market_status():
    try:
        xjo = yf.download("^AXJO", period="3mo", interval="1d", progress=False)
        if xjo.empty or len(xjo) < 50:
            return "green"
        close = xjo['Close'].squeeze()
        ma50 = close.rolling(50).mean()
        deviation = (float(close.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1])
        weekly = close.resample('W').last()
        sharp_drop = (weekly.pct_change().iloc[-2:] < -0.05).any()
        if deviation < -0.03 or sharp_drop:
            return "red"
        if deviation < 0:
            return "yellow"
        return "green"
    except:
        return "green"

# ── 单股分析（根据tier参数）──────────────────────────────────
def analyze_stock(ticker, tier):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df is None or len(df) < 60:
            return None

        close  = df['Close'].squeeze()
        high   = df['High'].squeeze()
        low    = df['Low'].squeeze()
        volume = df['Volume'].squeeze()

        lc   = float(close.iloc[-1])
        lh   = float(high.iloc[-1])
        ll   = float(low.iloc[-1])
        lvol = float(volume.iloc[-1])

        # 流动性：$30万澳元（固定，不随tier放宽）
        avg_vol_20 = float(volume.iloc[-20:].mean())
        if avg_vol_20 * lc < 300_000:
            return None

        # 计算指标
        ma50     = close.rolling(50).mean()
        vol_ma20 = volume.rolling(20).mean()
        rsi      = calc_rsi(close)
        obv      = calc_obv(close, volume)
        mfi      = calc_mfi(high, low, close, volume)

        lm50       = float(ma50.iloc[-1])
        lm50_10ago = float(ma50.iloc[-11])
        lvm20      = float(vol_ma20.iloc[-1])
        lrsi       = float(rsi.iloc[-1])
        lobv       = float(obv.iloc[-1])
        lmfi       = float(mfi.iloc[-1])

        # 固定条件（所有tier都需满足）
        if lc < lm50: return None
        if lm50 <= lm50_10ago: return None

        # tier参数控制的条件
        r15 = df.iloc[-15:]
        pr = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > tier["consol"]: return None

        if float(volume.iloc[-10:].mean()) >= float(volume.iloc[-30:-10].mean()): return None

        high_20 = float(high.iloc[-20:].max())
        if lc < high_20 * 0.97: return None

        if lvol < lvm20 * tier["vol_mult"]: return None

        day_range = lh - ll
        if day_range > 0:
            close_position = (lc - ll) / day_range
            if close_position < tier["close_pos"]: return None
        else:
            close_position = 0.5

        if not (tier["rsi_lo"] <= lrsi <= tier["rsi_hi"]): return None

        obv_30d_high = float(obv.iloc[-30:].max())
        if lobv < obv_30d_high * tier["obv_pct"]: return None

        if not (40 <= lmfi <= 70): return None

        # 市值下限（固定）
        info = yf.Ticker(ticker).info
        if info.get('marketCap', 0) < 50_000_000: return None

        return {
            'ticker'      : ticker,
            'price'       : round(lc, 3),
            'entry_limit' : round(lc * 1.02, 3),
            'stop_loss'   : round(lc * 0.90, 3),
            'take_profit' : round(lc * 1.20, 3),
            'rsi'         : round(lrsi, 1),
            'mfi'         : round(lmfi, 1),
            'vol_ratio'   : round(lvol / lvm20, 2),
            'close_pos'   : round(close_position * 100, 1),
            'market_cap_m': round(info.get('marketCap', 0) / 1_000_000),
        }
    except:
        return None

# ── Telegram ─────────────────────────────────────────────────
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram失败: {e}")

# ── 主程序 ───────────────────────────────────────────────────
def run_screener():
    today = datetime.now().strftime('%Y-%m-%d')

    # 大盘检查
    market_status = check_market_status()
    print(f"大盘状态: {market_status.upper()}")

    if market_status == "red":
        send_telegram(
            f"🔴 <b>大盘警告 {today}</b>\n\n"
            "ASX200大幅跌破50日均线或近期急跌。\n"
            "今日<b>不建议开新仓</b>，收紧止损至5%。"
        )
        return

    market_label = "⚠️ " if market_status == "yellow" else ""
    market_note = (
        "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。"
    ) if market_status == "yellow" else ""

    # 获取股票池
    universe = get_asx_universe()
    if not universe:
        return

    total = len(universe)
    print(f"[{today}] 扫描 {total} 只股票...")

    # 分级筛选：T1→T4，找到信号立即停止
    found_tier = None
    signals = []

    for tier in TIERS:
        print(f"\n  正在运行 {tier['level']} ({tier['label']})...")
        tier_signals = []

        for idx, ticker in enumerate(universe, 1):
            if idx % 200 == 0:
                print(f"    进度：{idx}/{total}...")
            result = analyze_stock(ticker, tier)
            if result:
                tier_signals.append(result)
                print(f"    ✅ {ticker}")

        if tier_signals:
            found_tier = tier
            signals = tier_signals
            print(f"  {tier['level']} 找到 {len(signals)} 个信号，停止搜索。")
            break
        else:
            print(f"  {tier['level']} 无信号，进入下一级...")

    # 四级全部无信号
    if not signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{total} 只\n"
            f"四个筛选等级均无信号。\n"
            f"市场整体偏弱，建议观望。{market_note}"
        )
        print("四级均无信号。")
        return

    # 发送汇总
    tier_emoji = found_tier["label"]
    tier_note = found_tier["note"]

    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"扫描：{total} 只\n"
        f"信号等级：{tier_emoji}\n"
        f"触发信号：{len(signals)} 只\n"
        f"说明：{tier_note}\n\n"
        + "\n".join([f"• {s['ticker']}" for s in signals])
        + market_note
    )

    # 逐一发详情
    for s in signals:
        msg = (
            f"{tier_emoji} <b>策略信号 — {s['ticker']}</b>\n"
            f"📅 {today}\n\n"
            f"💰 昨收：${s['price']}\n"
            f"🟢 入场上限：${s['entry_limit']}（超过不追）\n"
            f"🎯 止盈：${s['take_profit']}（+20%）\n"
            f"🛑 止损：${s['stop_loss']}（-10%）\n\n"
            f"📊 指标：\n"
            f"  • RSI：{s['rsi']}\n"
            f"  • MFI：{s['mfi']}\n"
            f"  • 成交量：均量的 {s['vol_ratio']}×\n"
            f"  • 收盘位置：{s['close_pos']}%\n"
            f"  • 市值：${s['market_cap_m']}M AUD\n\n"
            f"⚠️ 核对图表后再决定入场\n"
            f"📌 确认本周无重大公告{market_note}"
        )
        send_telegram(msg)

    print(f"\n完成，{found_tier['level']} 级，共 {len(signals)} 个信号。")

run_screener()