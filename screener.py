import os
# ============================================================
# ASX SWING TRADE SCREENER v5
# 改进：市值仅设下限$5000万 | 成交量2倍确认
#       收盘位置过滤 | 合理入场价上限
# ============================================================

# ── 配置 ─────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# ── 依赖 ─────────────────────────────────────────────────────
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

# ── 动态获取ASX全部上市公司 ──────────────────────────────────
def get_asx_universe():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        df = pd.read_csv(url, skiprows=1, encoding='latin1')
        code_col = next((c for c in df.columns if 'code' in c.lower()), None)
        if code_col is None:
            print(f"CSV列名: {df.columns.tolist()}")
            return []
        codes = df[code_col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        print(f"ASX上市公司总数：{len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"获取ASX列表失败: {e}")
        return []

# ── 指标计算（纯pandas）──────────────────────────────────────
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

# ── 三档大盘过滤 ─────────────────────────────────────────────
def check_market_status():
    try:
        xjo = yf.download("^AXJO", period="3mo", interval="1d", progress=False)
        if xjo.empty or len(xjo) < 50:
            return "green"
        close      = xjo['Close'].squeeze()
        ma50       = close.rolling(50).mean()
        latest     = float(close.iloc[-1])
        latest_ma50= float(ma50.iloc[-1])
        deviation  = (latest - latest_ma50) / latest_ma50
        weekly     = close.resample('W').last()
        sharp_drop = (weekly.pct_change().iloc[-2:] < -0.05).any()
        if deviation < -0.03 or sharp_drop:
            return "red"
        if deviation < 0:
            return "yellow"
        return "green"
    except:
        return "green"

# ── 单股分析 ─────────────────────────────────────────────────
def analyze_stock(ticker, idx, total):
    try:
        df = yf.download(ticker, period="6mo", interval="1d", progress=False)
        if df is None or len(df) < 60:
            return None

        close  = df['Close'].squeeze()
        high   = df['High'].squeeze()
        low    = df['Low'].squeeze()
        volume = df['Volume'].squeeze()

        lc    = float(close.iloc[-1])
        lh    = float(high.iloc[-1])
        ll    = float(low.iloc[-1])
        lvol  = float(volume.iloc[-1])

        # ── 流动性粗筛：日均成交额 > $200万 ──────────────────
        avg_vol_20   = float(volume.iloc[-20:].mean())
        avg_turnover = avg_vol_20 * lc
        if avg_turnover < 2_000_000:
            return None

        # ── 计算指标 ─────────────────────────────────────────
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

        # ── 策略条件 ─────────────────────────────────────────

        # 1. 股价在50日均线以上
        if lc < lm50:
            return None

        # 2. 50日均线向上倾斜
        if lm50 <= lm50_10ago:
            return None

        # 3. 近15日整理幅度 < 12%
        r15 = df.iloc[-15:]
        pr  = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > 0.12:
            return None

        # 4. 整理期间量能萎缩
        vol_recent = float(volume.iloc[-10:].mean())
        vol_prior  = float(volume.iloc[-30:-10].mean())
        if vol_recent >= vol_prior:
            return None

        # 5. 接近20日突破位（收盘在20日最高价97%以上）
        high_20 = float(high.iloc[-20:].max())
        if lc < high_20 * 0.97:
            return None

        # 6. 放量突破：成交量 ≥ 均量2倍（从1.5倍提高到2倍）
        if lvol < lvm20 * 2.0:
            return None

        # 7. 收盘位置过滤：收盘价在当日最高最低范围的90%以上
        #    排除冲高回落的长上影线假突破
        day_range = lh - ll
        if day_range > 0:
            close_position = (lc - ll) / day_range
            if close_position < 0.90:
                return None

        # 8. RSI 45–65
        if not (45 <= lrsi <= 65):
            return None

        # 9. OBV处于近30日高点95%以上
        obv_30d_high = float(obv.iloc[-30:].max())
        if lobv < obv_30d_high * 0.95:
            return None

        # 10. MFI 40–70
        if not (40 <= lmfi <= 70):
            return None

        # ── 通过技术条件才查市值（节省API调用）─────────────
        info       = yf.Ticker(ticker).info
        market_cap = info.get('marketCap', 0)

        # 市值只设下限$5000万，排除壳公司和僵尸股
        # 不设上限：任何市值的股票只要技术形态好都有机会
        if market_cap < 50_000_000:
            return None

        # ── 计算合理入场价上限（突破价+2%） ─────────────────
        # 若次日开盘价超过此价格，建议放弃，不追高
        entry_limit = round(lc * 1.02, 3)

        print(f"  ✅ [{idx}/{total}] 信号：{ticker}")
        return {
            'ticker'      : ticker,
            'price'       : round(lc, 3),
            'entry_limit' : entry_limit,
            'stop_loss'   : round(lc * 0.90, 3),
            'take_profit' : round(lc * 1.20, 3),
            'rsi'         : round(lrsi, 1),
            'mfi'         : round(lmfi, 1),
            'vol_ratio'   : round(lvol / lvm20, 2),
            'market_cap_m': round(market_cap / 1_000_000),
            'close_pos'   : round(close_position * 100, 1),
        }

    except Exception:
        return None

# ── 发送Telegram ─────────────────────────────────────────────
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram失败: {e}")

# ── 主程序 ───────────────────────────────────────────────────
def run_screener():
    today = datetime.now().strftime('%Y-%m-%d')

    # 大盘状态
    market_status = check_market_status()
    print(f"大盘状态：{market_status.upper()}")

    if market_status == "red":
        send_telegram(
            f"🔴 <b>大盘警告 {today}</b>\n\n"
            "ASX200大幅跌破50日均线（>3%）或近期单周跌幅超5%。\n\n"
            "今日<b>不建议开新仓</b>。\n"
            "现有持仓建议收紧止损至5%。"
        )
        print("红灯：已发警告，今日不扫描。")
        return

    # 黄灯标签和提示
    market_label = "⚠️ " if market_status == "yellow" else ""
    market_note  = (
        "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，"
        "建议适当缩减仓位，严格执行止损。"
    ) if market_status == "yellow" else ""

    # 获取股票池
    universe = get_asx_universe()
    if not universe:
        print("获取股票列表失败，退出。")
        return

    total = len(universe)
    print(f"[{today}] 开始扫描 {total} 只股票...\n")

    signals = []
    for idx, ticker in enumerate(universe, 1):
        if idx % 100 == 0:
            print(f"  进度：{idx}/{total}...")
        result = analyze_stock(ticker, idx, total)
        if result:
            signals.append(result)

    # 无信号
    if not signals:
        send_telegram(
            f"📊 <b>{market_label}ASX全市场扫描完成 {today}</b>\n\n"
            f"扫描股票：{total} 只\n"
            f"今日无符合策略的信号。{market_note}"
        )
        print(f"\n扫描完成，今日无信号。")
        return

    # 汇总消息
    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"扫描股票：{total} 只\n"
        f"触发信号：{len(signals)} 只\n\n"
        + "\n".join([f"• {s['ticker']}" for s in signals])
        + market_note
    )

    # 逐一发详情
    for s in signals:
        msg = (
            f"🔔 <b>{market_label}策略信号 — {s['ticker']}</b>\n"
            f"📅 {today}\n\n"
            f"💰 昨收：${s['price']}\n"
            f"🟢 入场上限：${s['entry_limit']}（超过此价不追）\n"
            f"🎯 止盈：${s['take_profit']}（+20%）\n"
            f"🛑 止损：${s['stop_loss']}（-10%）\n\n"
            f"📊 指标：\n"
            f"  • RSI：{s['rsi']}\n"
            f"  • MFI：{s['mfi']}（资金净流入）\n"
            f"  • 成交量：均量的 {s['vol_ratio']}×\n"
            f"  • 收盘位置：当日波幅 {s['close_pos']}% 处\n"
            f"  • 市值：${s['market_cap_m']}M AUD\n\n"
            f"⚠️ 核对图表后再决定入场\n"
            f"📌 确认本周无重大公告{market_note}"
        )
        send_telegram(msg)

    print(f"\n扫描完成，共 {len(signals)} 个信号已发送。")

run_screener()
