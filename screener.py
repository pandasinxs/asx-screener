# ============================================================
# ASX SWING TRADE SCREENER v9
# 新API：asx.api.markitdigital.com
# 修复：yfinance新闻字段 content.title
# ============================================================

import os, time
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, date
from google import genai

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")

gemini_client  = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

ASX_ANN_URL = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/json',
    'Referer': 'https://www.asx.com.au'
}

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

# ── 今日公告（批量）──────────────────────────────────────────
def get_today_announcements() -> dict:
    today  = date.today().isoformat()
    result = {}
    page   = 0
    while True:
        try:
            r = requests.get(ASX_ANN_URL,
                             params={'itemsPerPage': 100, 'page': page},
                             headers=ASX_HEADERS, timeout=10)
            items = r.json().get('data', {}).get('items', [])
            if not items: break
            got_old = False
            for item in items:
                if item.get('date', '')[:10] < today:
                    got_old = True
                    break
                sym = item.get('symbol', '')
                if sym and sym not in result:
                    result[sym] = {
                        'headline' : item.get('headline', '')[:70],
                        'sensitive': item.get('isPriceSensitive', False)
                    }
            if got_old or len(items) < 100: break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"公告API错误: {e}")
            break
    print(f"今日公告：{len(result)} 只股票有公告")
    return result

# ── yfinance新闻（修复content.title）────────────────────────
def get_yf_news(code: str) -> list:
    try:
        stock  = yf.Ticker(f"{code}.AX")
        today  = date.today().isoformat()
        result = []
        for n in (stock.news or [])[:8]:
            content = n.get('content', {})
            title   = content.get('title', '')
            pub     = content.get('pubDate', '')[:10]
            if title:
                result.append({
                    'title' : title,
                    'date'  : pub,
                    'today' : pub == today,
                    'source': content.get('provider', {}).get('displayName', '')
                })
        result.sort(key=lambda x: x['today'], reverse=True)
        return result
    except:
        return []

# ── Gemini工具 ────────────────────────────────────────────────
def ask_gemini(prompt: str) -> str:
    if not gemini_client: return ""
    try:
        r = gemini_client.models.generate_content(
            model='gemini-2.0-flash-lite', contents=prompt)
        return r.text.strip()
    except:
        return ""

def analyze_eod_signal(s: dict, ann_info: dict, news: list) -> str:
    ann_text  = ann_info['headline'] if ann_info else "无今日公告"
    news_text = news[0]['title'] if news else "无近期新闻"
    prompt = f"""你是一位严谨的ASX股票分析师。

EOD波段信号：{s['ticker']}
昨收：${s['price']}  RSI：{s['rsi']}  MFI：{s['mfi']}
成交量：{s['vol_ratio']}x均量  收盘位置：{s['close_pos']}%
今日公告：{ann_text}
近期新闻：{news_text}

请用1-2句中文分析技术形态的有效性和主要风险。
只陈述可判断的内容，不确定的说"需进一步核查"。"""
    return ask_gemini(prompt)

# ── ASX股票池 ─────────────────────────────────────────────────
def get_asx_universe():
    try:
        df = pd.read_csv(
            "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
            skiprows=1, encoding='latin1')
        col = next((c for c in df.columns if 'code' in c.lower()), None)
        codes  = df[col].dropna().astype(str).str.strip()
        valid  = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        tickers = [f"{c}.AX" for c in valid]
        print(f"ASX股票池：{len(tickers)} 只")
        return tickers
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

# ── 批量下载 ──────────────────────────────────────────────────
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
    print(f"  下载完成：{len(all_data)}/{len(tickers)} 只有效")
    return all_data

# ── 技术指标 ──────────────────────────────────────────────────
def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = -delta.clip(upper=0).rolling(period).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

def calc_obv(close, volume):
    d = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (d * volume).cumsum()

def calc_mfi(high, low, close, volume, period=14):
    tp  = (high + low + close) / 3
    rmf = tp * volume
    pos = rmf.where(tp > tp.shift(1), 0).rolling(period).sum()
    neg = rmf.where(tp < tp.shift(1), 0).rolling(period).sum()
    return 100 - (100 / (1 + pos / neg.replace(0, 1e-10)))

def check_market_status():
    try:
        xjo   = yf.download("^AXJO", period="3mo", interval="1d", progress=False)
        if xjo.empty or len(xjo) < 50: return "green"
        close = xjo['Close'].squeeze()
        ma50  = close.rolling(50).mean()
        dev   = (float(close.iloc[-1]) - float(ma50.iloc[-1])) / float(ma50.iloc[-1])
        drop  = (close.resample('W').last().pct_change().iloc[-2:] < -0.05).any()
        if dev < -0.03 or drop: return "red"
        if dev < 0:              return "yellow"
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
        lc, lh, ll = float(close.iloc[-1]), float(high.iloc[-1]), float(low.iloc[-1])
        lvol       = float(volume.iloc[-1])
        if float(volume.iloc[-20:].mean()) * lc < 300_000: return None
        ma50      = close.rolling(50).mean()
        vol_ma20  = volume.rolling(20).mean()
        rsi       = calc_rsi(close)
        obv       = calc_obv(close, volume)
        mfi       = calc_mfi(high, low, close, volume)
        lm50      = float(ma50.iloc[-1])
        lm50_prev = float(ma50.iloc[-11])
        lvm20     = float(vol_ma20.iloc[-1])
        lrsi      = float(rsi.iloc[-1])
        lobv      = float(obv.iloc[-1])
        lmfi      = float(mfi.iloc[-1])
        if lc < lm50 or lm50 <= lm50_prev: return None
        r15 = df.iloc[-15:]
        pr  = (float(r15['High'].max()) - float(r15['Low'].min())) / lc
        if pr > tier["consol"]: return None
        if tier["vol_decline"]:
            if float(volume.iloc[-10:].mean()) >= float(volume.iloc[-30:-10].mean()):
                return None
        if tier["near_breakout"]:
            if lc < float(high.iloc[-20:].max()) * 0.97: return None
        if lvol < lvm20 * tier["vol_mult"]: return None
        day_range = lh - ll
        close_pos = (lc - ll) / day_range if day_range > 0 else 0.5
        if close_pos < tier["close_pos"]: return None
        if not (tier["rsi_lo"] <= lrsi <= tier["rsi_hi"]): return None
        if tier["use_obv"]:
            if lobv < float(obv.iloc[-30:].max()) * tier["obv_pct"]: return None
        if tier["use_mfi"]:
            if not (40 <= lmfi <= 70): return None
        try:
            info = yf.Ticker(ticker).info
            if info.get('marketCap', 0) < 50_000_000: return None
            mktcap = round(info.get('marketCap', 0) / 1_000_000)
        except:
            mktcap = 0
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
            'market_cap_m': mktcap,
        }
    except:
        return None

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg,
                                 "parse_mode": "HTML"}, timeout=10)
    except: pass

# ── 主程序 ───────────────────────────────────────────────────
def run_screener():
    today = datetime.now().strftime('%Y-%m-%d')
    start = time.time()

    market_status = check_market_status()
    print(f"大盘状态: {market_status.upper()}")

    if market_status == "red":
        send_telegram(
            f"🔴 <b>大盘警告 {today}</b>\n\n"
            "ASX200大幅跌破50日均线或近期急跌。\n"
            "今日<b>不建议开新仓</b>，收紧止损至5%。"
        )
        return

    market_note = (
        "\n\n⚠️ <b>大盘提示</b>：ASX200轻微跌破50日均线，建议适当缩减仓位。"
    ) if market_status == "yellow" else ""
    market_label = "⚠️ " if market_status == "yellow" else ""

    universe = get_asx_universe()
    if not universe: return

    print(f"\n[{today}] 批量下载 {len(universe)} 只...")
    all_data = batch_download_all(universe)
    print(f"下载耗时：{round(time.time()-start)}秒")

    print("\n分级筛选...")
    found_tier, signals = None, []
    for tier in TIERS:
        print(f"  {tier['level']} ({tier['label']})...", end=" ")
        tier_signals = [r for t, df in all_data.items()
                        if (r := analyze_stock(t, df, tier))]
        print(f"{len(tier_signals)} 个")
        if tier_signals:
            found_tier, signals = tier, tier_signals
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

    send_telegram(
        f"📊 <b>{market_label}ASX扫描完成 {today}</b>\n\n"
        f"扫描：{len(all_data)} 只｜耗时：{elapsed}分钟\n"
        f"信号等级：{tier_label}\n"
        f"触发信号：{len(signals)} 只\n"
        f"说明：{found_tier['note']}\n\n"
        + "\n".join([f"• {s['ticker']}" for s in signals])
        + market_note
    )

    if tier_level in ("T7", "T8"):
        send_telegram(
            f"{tier_label} 当前为最宽松筛选，以上股票仅满足基本趋势条件。\n"
            "建议：自行查看图表，等待更明确信号，严格止损。"
        )
        return

    # T1-T6：发详情，加公告和AI分析
    # 拉取今日公告（T1-T4才调用Gemini，T5-T6只显示公告）
    print("拉取今日公告...")
    ann_map = get_today_announcements()

    for s in signals:
        code     = s['ticker'].replace('.AX', '')
        ann_info = ann_map.get(code)

        # 公告行
        if ann_info:
            sen_flag = "⭐ " if ann_info['sensitive'] else ""
            ann_line = f"\n📋 今日公告：{sen_flag}{ann_info['headline']}"
        else:
            ann_line = ""

        # AI分析（仅T1-T4）
        ai_line = ""
        if tier_level in ("T1","T2","T3","T4") and gemini_client:
            news     = get_yf_news(code)
            analysis = analyze_eod_signal(s, ann_info, news)
            if analysis:
                ai_line = f"\n🤖 {analysis}"

        msg = (
            f"{tier_label} <b>策略信号 — {s['ticker']}</b>\n"
            f"📅 {today}\n\n"
            f"💰 昨收：${s['price']}\n"
            f"🟢 入场上限：${s['entry_limit']}（超过不追）\n"
            f"🎯 止盈：${s['take_profit']}（+20%）\n"
            f"🛑 止损：${s['stop_loss']}（-10%）\n\n"
            f"📊 RSI：{s['rsi']}  MFI：{s['mfi']}\n"
            f"   成交量：{s['vol_ratio']}x  收盘位置：{s['close_pos']}%\n"
            f"   市值：${s['market_cap_m']}M AUD"
            f"{ann_line}{ai_line}\n\n"
            f"⚠️ 核对图表再决定入场{market_note}"
        )
        send_telegram(msg)
        time.sleep(0.5)

    print(f"\n完成：{tier_level}，{len(signals)} 个，耗时{elapsed}分钟")

run_screener()