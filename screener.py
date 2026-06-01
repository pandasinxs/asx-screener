# ============================================================
# ASX SWING TRADE SCREENER v7
# 批量下载（速度快10倍）+ 8级分级筛选
# ============================================================

# Cell 1 (Colab only): !pip install yfinance requests -q

import os, time
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "你的TOKEN")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "7553937057")

# ── 8个筛选等级 ───────────────────────────────────────────────
# vol_mult      : 成交量须达均量的倍数
# close_pos     : 收盘位置须在当日波幅的比例以上（过滤长上影线）
# obv_pct       : OBV须达近30日高点的比例
# rsi_lo/hi     : RSI范围
# consol        : 近15日整理幅度上限
# vol_decline   : 是否要求整理期间量能萎缩
# near_breakout : 是否要求接近20日最高点
# use_obv       : 是否启用OBV条件
# use_mfi       : 是否启用MFI条件

TIERS = [
    {"level":"T1","label":"🔴 精英",
     "vol_mult":2.0,"close_pos":0.90,"obv_pct":0.95,
     "rsi_lo":45,"rsi_hi":65,"consol":0.12,
     "vol_decline":True,"near_breakout":True,"use_obv":True,"use_mfi":True,
     "note":"最高质量，所有条件严格满足"},
    {"level":"T2","label":"🟠 优质",
     "vol_mult":1.5,"close_pos":0.75,"obv_pct":0.85,
     "rsi_lo":42,"rsi_hi":68,"consol":0.15,
     "vol_decline":True,"near_breakout":True,"use_obv":True,"use_mfi":True,
     "note":"高质量信号"},
    {"level":"T3","label":"🟡 标准",
     "vol_mult":1.2,"close_pos":0.60,"obv_pct":0.75,
     "rsi_lo":38,"rsi_hi":72,"consol":0.20,
     "vol_decline":True,"near_breakout":True,"use_obv":True,"use_mfi":False,
     "note":"标准质量"},
    {"level":"T4","label":"🟢 放宽",
     "vol_mult":1.0,"close_pos":0.50,"obv_pct":0.65,
     "rsi_lo":35,"rsi_hi":75,"consol":0.25,
     "vol_decline":True,"near_breakout":True,"use_obv":False,"use_mfi":False,
     "note":"参考信号，需谨慎判断"},
    {"level":"T5","label":"🔵 宽松",
     "vol_mult":0.8,"close_pos":0.40,"obv_pct":0.55,
     "rsi_lo":30,"rsi_hi":78,"consol":0.30,
     "vol_decline":False,"near_breakout":True,"use_obv":False,"use_mfi":False,
     "note":"宽松条件，仅供参考"},
    {"level":"T6","label":"🟣 很宽松",
     "vol_mult":0.5,"close_pos":0.30,"obv_pct":0.50,
     "rsi_lo":25,"rsi_hi":80,"consol":0.40,
     "vol_decline":False,"near_breakout":False,"use_obv":False,"use_mfi":False,
     "note":"很宽松，需自行做额外研究"},
    {"level":"T7","label":"⚫ 极宽松",
     "vol_mult":0.3,"close_pos":0.20,"obv_pct":0.40,
     "rsi_lo":20,"rsi_hi":82,"consol":0.50,
     "vol_decline":False,"near_breakout":False,"use_obv":False,"use_mfi":False,
     "note":"极宽松，仅趋势参考"},
    {"level":"T8","label":"⚪ 最低门槛",
     "vol_mult":0.1,"close_pos":0.10,"obv_pct":0.30,
     "rsi_lo":15,"rsi_hi":85,"consol":0.60,
     "vol_decline":False,"near_breakout":False,"use_obv":False,"use_mfi":False,
     "note":"最低门槛：仅需在均线上方且有基本成交量"},
]

# ── 获取ASX全部股票代码 ───────────────────────────────────────
def get_asx_universe():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        df = pd.read_csv(url, skiprows=1, encoding='latin1')
        code_col = next((c for c in df.columns if 'code' in c.lower()), None)
        if not code_col:
            return []
        codes  = df[code_col].dropna().astype(str).str.strip()
        valid  = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        print(f"ASX股票池：{len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

# ── 批量下载（核心加速）────────────────────────────────────────
def batch_download_all(tickers, batch_size=50):
    """一次下载50只，比逐只快10倍，并规避限速"""
    all_data    = {}
    n_batches   = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch     = tickers[i : i + batch_size]
        batch_num = i // batch_size + 1

        if batch_num % 5 == 0 or batch_num == 1:
            print(f"  下载 {batch_num}/{n_batches} 批...")

        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="6mo",
                                 interval="1d", progress=False)
                if not df.empty and len(df) >= 60:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(batch, period="6mo", interval="1d",
                                  progress=False, group_by='ticker')
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how='all')
                        if not tdf.empty and len(tdf) >= 60:
                            all_data[t] = tdf
                    except:
                        pass
        except:
            # 批量失败时逐只重试
            for t in batch:
                try:
                    df = yf.download(t, period="6mo", interval="1d",
                                     progress=False)
                    if not df.empty and len(df) >= 60:
                        all_data[t] = df
                except:
                    pass

        time.sleep(0.5)   # 每批间隔0.5秒，避免触发限速

    print(f"  下载完成：{len(all_data)}/{len(tickers)} 只有效数据")
    return all_data

# ── 指标计算 ──────────────────────────────────────────────────
def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = -delta.clip(upper=0).rolling(period).mean()
    rs    = gain / loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))

def calc_obv(close, volume):
    d = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (d * volume).cumsum()

def calc_mfi(high, low, close, volume, period=14):
    tp     = (high + low + close) / 3
    raw_mf = tp * volume
    pos    = raw_mf.where(tp > tp.shift(1), 0).rolling(period).sum()
    neg    = raw_mf.where(tp < tp.shift(1), 0).rolling(period).sum()
    return 100 - (100 / (1 + pos / neg.replace(0, 1e-10)))

# ── 大盘状态 ──────────────────────────────────────────────────
def check_market_status():
    try:
        xjo   = yf.download("^AXJO", period="3mo", interval="1d",
                            progress=False)
        if xjo.empty or len(xjo) < 50:
            return "green"
        close = xjo['Close'].squeeze()
        ma50  = close.rolling(50).mean()
        dev   = (float(close.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1])
        wkly  = close.resample('W').last()
        drop  = (wkly.pct_change().iloc[-2:] < -0.05).any()
        if dev < -0.03 or drop:  return "red"
        if dev < 0:               return "yellow"
        return "green"
    except:
        return "green"

# ── 单股分析（使用预下载数据）────────────────────────────────
def analyze_stock(ticker, df, tier):
    try:
        close  = df['Close'].squeeze()
        high   = df['High'].squeeze()
        low    = df['Low'].squeeze()
        volume = df['Volume'].squeeze()

        if len(close) < 60:
            return None

        lc   = float(close.iloc[-1])
        lh   = float(high.iloc[-1])
        ll   = float(low.iloc[-1])
        lvol = float(volume.iloc[-1])

        # 流动性（固定门槛，不随tier放宽）
        avg_vol_20 = float(volume.iloc[-20:].mean())
        if avg_vol_20 * lc < 300_000:
            return None

        # 技术指标
        ma50     = close.rolling(50).mean()
        vol_ma20 = volume.rolling(20).mean()
        rsi      = calc_rsi(close)
        obv      = calc_obv(close, volume)
        mfi      = calc_mfi(high, low, close, volume)

        lm50       = float(ma50.iloc[-1])
        lm50_prev  = float(ma50.iloc[-11])
        lvm20      = float(vol_ma20.iloc[-1])
        lrsi       = float(rsi.iloc[-1])
        lobv       = float(obv.iloc[-1])
        lmfi       = float(mfi.iloc[-1])

        # ── 固定条件（所有tier均需满足）──────────────────────
        if lc < lm50:          return None   # 在50日均线以上
        if lm50 <= lm50_prev:  return None   # 均线向上

        # ── tier控制的条件 ────────────────────────────────────

        # 整理幅度
        r15  = df.iloc[-15:]
        pr   = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > tier["consol"]:  return None

        # 量能萎缩（可选）
        if tier["vol_decline"]:
            if float(volume.iloc[-10:].mean()) >= float(volume.iloc[-30:-10].mean()):
                return None

        # 接近20日高点（可选）
        if tier["near_breakout"]:
            high_20 = float(high.iloc[-20:].max())
            if lc < high_20 * 0.97:  return None

        # 成交量倍数
        if lvol < lvm20 * tier["vol_mult"]:  return None

        # 收盘位置
        day_range = lh - ll
        if day_range > 0:
            close_pos = (lc - ll) / day_range
            if close_pos < tier["close_pos"]:  return None
        else:
            close_pos = 0.5

        # RSI
        if not (tier["rsi_lo"] <= lrsi <= tier["rsi_hi"]):  return None

        # OBV（可选）
        if tier["use_obv"]:
            obv_high = float(obv.iloc[-30:].max())
            if lobv < obv_high * tier["obv_pct"]:  return None

        # MFI（可选）
        if tier["use_mfi"]:
            if not (40 <= lmfi <= 70):  return None

        # 市值下限（通过技术条件后才查，节省API）
        try:
            info = yf.Ticker(ticker).info
            if info.get('marketCap', 0) < 50_000_000:
                return None
            market_cap_m = round(info.get('marketCap', 0) / 1_000_000)
        except:
            market_cap_m = 0

        return {
            'ticker'      : ticker,
            'price'       : round(lc, 3),
            'entry_limit' : round(lc * 1.02, 3),
            'stop_loss'   : round(lc * 0.90, 3),
            'take_profit' : round(lc * 1.20, 3),
            'rsi'         : round(lrsi, 1),
            'mfi'         : round(lmfi, 1),
            'vol_ratio'   : round(lvol / lvm20, 2),
            'close_pos'   : round(close_pos * 100, 1),
            'market_cap_m': market_cap_m,
        }
    except:
        return None

# ── Telegram ─────────────────────────────────────────────────
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg,
                                 "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram失败: {e}")

# ── 主程序 ───────────────────────────────────────────────────
def run_screener():
    today = datetime.now().strftime('%Y-%m-%d')
    start = time.time()

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
    market_note  = (
        "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。"
    ) if market_status == "yellow" else ""

    # 获取股票池
    universe = get_asx_universe()
    if not universe:
        return

    # 批量下载所有数据（核心加速）
    print(f"\n[{today}] 开始批量下载 {len(universe)} 只股票数据...")
    all_data = batch_download_all(universe)
    print(f"下载耗时：{round(time.time()-start)}秒")

    # 分级筛选
    print("\n开始分级筛选...")
    found_tier = None
    signals    = []

    for tier in TIERS:
        print(f"  {tier['level']} ({tier['label']})...", end=" ")
        tier_signals = []

        for ticker, df in all_data.items():
            result = analyze_stock(ticker, df, tier)
            if result:
                tier_signals.append(result)

        print(f"{len(tier_signals)} 个信号")

        if tier_signals:
            found_tier = tier
            signals    = tier_signals
            break

    # 结果处理
    elapsed = round((time.time() - start) / 60, 1)

    if not signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只\n"
            f"T1–T8 全部无信号\n"
            f"市场整体处于下行趋势，建议观望。\n"
            f"耗时：{elapsed}分钟{market_note}"
        )
        print(f"\n全部无信号。耗时{elapsed}分钟")
        return

    # 按成交量比例排序，最多发20个信号
    signals.sort(key=lambda x: x['vol_ratio'], reverse=True)
    signals = signals[:20]

    tier_label = found_tier["label"]
    tier_level = found_tier["level"]
    tier_note  = found_tier["note"]

    # 汇总消息
    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"扫描：{len(all_data)} 只｜耗时：{elapsed}分钟\n"
        f"信号等级：{tier_label}\n"
        f"触发信号：{len(signals)} 只\n"
        f"说明：{tier_note}\n\n"
        + "\n".join([f"• {s['ticker']}" for s in signals])
        + market_note
    )

    # 逐一发详情（T6-T8只发汇总，不发详情，避免刷屏）
    if tier_level not in ("T7", "T8"):
        for s in signals:
            msg = (
                f"{tier_label} <b>策略信号 — {s['ticker']}</b>\n"
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
                f"⚠️ 核对图表再决定入场\n"
                f"📌 确认本周无重大公告{market_note}"
            )
            send_telegram(msg)
            time.sleep(0.3)
    else:
        send_telegram(
            f"{tier_label} <b>{tier_level} 等级说明</b>\n\n"
            f"当前为最宽松筛选，以上股票仅满足基本趋势条件。\n"
            f"建议：\n"
            f"• 自行查看图表确认形态\n"
            f"• 等待更明确的入场信号\n"
            f"• 降低仓位，严格止损"
        )

    print(f"\n完成：{tier_level} 级，{len(signals)} 个信号，耗时{elapsed}分钟")

run_screener()