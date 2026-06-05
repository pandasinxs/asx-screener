# ============================================================
# ASX SWING TRADE SCREENER v8
# 新增：EOD信号带公告检查 + Gemini简要分析
# 批量下载 + 8级分级筛选
# ============================================================

import os, time
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, date
from google import genai

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "你的TOKEN")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "7553937057")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

gemini_client  = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

# ── 8个筛选等级 ───────────────────────────────────────────────
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

# ── 工具函数 ──────────────────────────────────────────────────
def ask_gemini(prompt: str) -> str:
    if not gemini_client:
        return ""
    try:
        r = gemini_client.models.generate_content(
            model='gemini-2.0-flash-lite',
            contents=prompt
        )
        return r.text.strip()
    except:
        return ""

def get_today_announcement(code: str):
    """检查是否有今日ASX公告，返回(title, is_sensitive)"""
    today = date.today().strftime('%Y-%m-%d')
    url = (f"https://www.asx.com.au/asx/1/company/{code}"
           f"/announcements?count=5&market_sensitive=false")
    try:
        r = requests.get(url, timeout=6,
                         headers={'User-Agent': 'Mozilla/5.0'})
        for ann in r.json().get('data', []):
            if str(ann.get('document_release_date', ''))[:10] == today:
                return ann.get('header', '')[:70], ann.get('market_sensitive', False)
        return None, False
    except:
        return None, False

def analyze_eod_signal(s: dict, ann_title: str) -> str:
    """为EOD信号生成Gemini简要分析"""
    prompt = f"""你是一位严谨的ASX股票分析师。

EOD波段信号：{s['ticker']}
昨收：${s['price']}  RSI：{s['rsi']}  MFI：{s['mfi']}
成交量：{s['vol_ratio']}x均量  收盘位置：{s['close_pos']}%
今日公告：{ann_title or '无'}

请用1-2句中文分析这个技术形态的有效性和主要风险。
要求：只陈述可判断的内容，不确定的直接说"需进一步核查"。"""
    return ask_gemini(prompt)

def get_asx_universe():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        df = pd.read_csv(url, skiprows=1, encoding='latin1')
        col = next((c for c in df.columns if 'code' in c.lower()), None)
        if not col: return []
        codes = df[col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        print(f"ASX股票池：{len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

def batch_download_all(tickers, batch_size=50):
    all_data  = {}
    n_batches = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch     = tickers[i:i+batch_size]
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
                    except: pass
        except:
            for t in batch:
                try:
                    df = yf.download(t, period="6mo", interval="1d",
                                     progress=False)
                    if not df.empty and len(df) >= 60:
                        all_data[t] = df
                except: pass
        time.sleep(0.5)
    print(f"  下载完成：{len(all_data)}/{len(tickers)} 只有效数据")
    return all_data

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

def analyze_stock(ticker, df, tier):
    try:
        close  = df['Close'].squeeze()
        high   = df['High'].squeeze()
        low    = df['Low'].squeeze()
        volume = df['Volume'].squeeze()
        if len(close) < 60: return None
        lc   = float(close.iloc[-1])
        lh   = float(high.iloc[-1])
        ll   = float(low.iloc[-1])
        lvol = float(volume.iloc[-1])
        avg_vol_20 = float(volume.iloc[-20:].mean())
        if avg_vol_20 * lc < 300_000: return None
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
        if lc < lm50:         return None
        if lm50 <= lm50_prev: return None
        r15 = df.iloc[-15:]
        pr  = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > tier["consol"]: return None
        if tier["vol_decline"]:
            if float(volume.iloc[-10:].mean()) >= float(volume.iloc[-30:-10].mean()):
                return None
        if tier["near_breakout"]:
            high_20 = float(high.iloc[-20:].max())
            if lc < high_20 * 0.97: return None
        if lvol < lvm20 * tier["vol_mult"]: return None
        day_range = lh - ll
        if day_range > 0:
            close_pos = (lc - ll) / day_range
            if close_pos < tier["close_pos"]: return None
        else:
            close_pos = 0.5
        if not (tier["rsi_lo"] <= lrsi <= tier["rsi_hi"]): return None
        if tier["use_obv"]:
            obv_high = float(obv.iloc[-30:].max())
            if lobv < obv_high * tier["obv_pct"]: return None
        if tier["use_mfi"]:
            if not (40 <= lmfi <= 70): return None
        try:
            info = yf.Ticker(ticker).info
            if info.get('marketCap', 0) < 50_000_000: return None
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

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg,
                                 "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram失败: {e}")

def run_screener():
    today   = datetime.now().strftime('%Y-%m-%d')
    start   = time.time()
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
    universe = get_asx_universe()
    if not universe: return
    print(f"\n[{today}] 开始批量下载 {len(universe)} 只股票数据...")
    all_data = batch_download_all(universe)
    print(f"下载耗时：{round(time.time()-start)}秒")
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
    elapsed = round((time.time() - start) / 60, 1)
    if not signals:
        send_telegram(
            f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
            f"扫描：{len(all_data)} 只\nT1–T8 全部无信号\n"
            f"市场整体处于下行趋势，建议观望。\n"
            f"耗时：{elapsed}分钟{market_note}"
        )
        return
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

    # T1-T6 发详情（T7/T8只发汇总）
    if tier_level not in ("T7", "T8"):
        for s in signals:
            code = s['ticker'].replace('.AX', '')

            # 检查今日公告
            ann_title, is_sensitive = get_today_announcement(code)
            ann_line = ""
            if ann_title:
                flag = "⭐ 市场敏感公告" if is_sensitive else "📋 今日公告"
                ann_line = f"\n{flag}：{ann_title}"

            # Gemini分析（T1-T4才调用，避免T5/T6过多调用）
            ai_line = ""
            if tier_level in ("T1","T2","T3","T4") and gemini_client:
                analysis = analyze_eod_signal(s, ann_title)
                if analysis:
                    ai_line = f"\n🤖 {analysis}"

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
                f"  • 市值：${s['market_cap_m']}M AUD"
                f"{ann_line}{ai_line}\n\n"
                f"⚠️ 核对图表再决定入场\n"
                f"📌 确认本周无重大公告{market_note}"
            )
            send_telegram(msg)
            time.sleep(0.5)
    else:
        send_telegram(
            f"{tier_label} <b>{tier_level} 等级说明</b>\n\n"
            "当前为最宽松筛选，以上股票仅满足基本趋势条件。\n"
            "建议：自行查看图表，等待更明确信号，严格止损。"
        )

    print(f"\n完成：{tier_level} 级，{len(signals)} 个信号，耗时{elapsed}分钟")

run_screener()