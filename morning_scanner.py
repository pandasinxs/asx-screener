# ============================================================
# FIRST PULLBACK — MORNING SCANNER v3
# 新API：asx.api.markitdigital.com（一次拉取今日全部公告）
# 修复：yfinance新闻字段 content.title
# ============================================================

import os, json, time
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, date
from google import genai

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

gemini_client  = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

ASX_ANN_URL = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements"
ASX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/json',
    'Referer': 'https://www.asx.com.au'
}

# ── 今日公告（一次性批量拉取）────────────────────────────────
def get_recent_announcements(hours_back=72) -> dict:
    from datetime import timezone, timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)
              ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
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
                if item.get('date', '') < cutoff:
                    got_old = True
                    break
                sym = item.get('symbol', '')
                if sym and sym not in result:
                    result[sym] = {
                        'headline' : item.get('headline', '')[:70],
                        'sensitive': item.get('isPriceSensitive', False),
                        'date'     : item.get('date', '')[:10]
                    }
            if got_old or len(items) < 100: break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"公告API错误: {e}")
            break
    print(f"最近{hours_back}小时公告：{len(result)} 只股票")
    return result

# ── yfinance新闻（修复content.title）────────────────────────
def get_yf_news(code: str) -> list:
    try:
        stock = yf.Ticker(f"{code}.AX")
        today = date.today().isoformat()
        results = []
        for n in (stock.news or [])[:8]:
            content = n.get('content', {})
            title   = content.get('title', '')
            pub     = content.get('pubDate', '')[:10]
            if title:
                results.append({
                    'title'  : title,
                    'date'   : pub,
                    'today'  : pub == today,
                    'source' : content.get('provider', {}).get('displayName', '')
                })
        results.sort(key=lambda x: x['today'], reverse=True)
        return results
    except:
        return []

# ── Gemini分析 ────────────────────────────────────────────────
def ask_gemini(prompt: str) -> str:
    if not gemini_client:
        return ""
    try:
        r = gemini_client.models.generate_content(
            model='gemini-2.0-flash-lite', contents=prompt)
        return r.text.strip()
    except:
        return ""

def analyze_candidate(c: dict, ann_info: dict) -> str:
    headline = ann_info.get('headline', '未确认') if ann_info else '未确认'
    prompt = f"""你是一位严谨的ASX股票分析师。

候选股：{c['ticker']}
今日涨幅：+{c['change_pct']}%
成交量：{c['vol_ratio']}x日均量
公告标题：{headline}
现价：${c['price']}  今日最高：${c['today_high']}  VWAP：${c['vwap']}

请用1-2句中文分析：
1. 公告催化逻辑是否成立
2. 主要风险点
只陈述可判断的内容，不确定的直接说"需进一步核查"。"""
    return ask_gemini(prompt)

# ── ASX股票池 ─────────────────────────────────────────────────
def get_asx_universe():
    try:
        df = pd.read_csv(
            "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
            skiprows=1, encoding='latin1')
        col = next((c for c in df.columns if 'code' in c.lower()), None)
        codes = df[col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        return [f"{c}.AX" for c in valid]
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

# ── 批量下载 ──────────────────────────────────────────────────
def batch_daily(tickers, batch_size=100):
    all_data = {}
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="60d", interval="1d", progress=False)
                if not df.empty and len(df) >= 20:
                    all_data[batch[0]] = df
            else:
                raw = yf.download(batch, period="60d", interval="1d",
                                  progress=False, group_by='ticker')
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how='all')
                        if not tdf.empty and len(tdf) >= 20:
                            all_data[t] = tdf
                    except: pass
        except: pass
        time.sleep(0.5)
    return all_data

def batch_intraday(tickers, batch_size=50):
    all_data = {}
    n = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        bn = i // batch_size + 1
        if bn % 5 == 1:
            print(f"  盘中数据 {bn}/{n}批...")
        try:
            if len(batch) == 1:
                df = yf.download(batch[0], period="1d", interval="5m", progress=False)
                if not df.empty: all_data[batch[0]] = df
            else:
                raw = yf.download(batch, period="1d", interval="5m",
                                  progress=False, group_by='ticker')
                for t in batch:
                    try:
                        tdf = raw[t].dropna(how='all')
                        if not tdf.empty: all_data[t] = tdf
                    except: pass
        except: pass
        time.sleep(0.5)
    return all_data

def calc_vwap(df):
    c = df['Close'].squeeze()
    h = df['High'].squeeze()
    l = df['Low'].squeeze()
    v = df['Volume'].squeeze()
    tp = (h + l + c) / 3
    return (tp * v).cumsum() / v.cumsum()

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg,
                                 "parse_mode": "HTML"}, timeout=10)
    except: pass

# ── 主程序 ───────────────────────────────────────────────────
def run_morning_scan():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\n[{today}] First Pullback 早盘扫描开始...")

    # 一次性拉取今日全部公告
    print("拉取今日ASX公告...")
    ann_map = get_recent_announcements()

    universe = get_asx_universe()
    if not universe: return
    print(f"股票池：{len(universe)} 只")

    print("下载日线数据...")
    daily_data = batch_daily(universe, batch_size=100)

    liquid = []
    for t, df in daily_data.items():
        try:
            if float(df['Volume'].iloc[-20:].mean()) * float(df['Close'].iloc[-1]) >= 300_000:
                liquid.append(t)
        except: pass
    print(f"流动性过滤后：{len(liquid)} 只")

    print("下载盘中数据...")
    intra_data = batch_intraday(liquid, batch_size=50)

    # 过滤候选股
    pre_candidates = []
    for t in liquid:
        try:
            daily = daily_data.get(t)
            intra = intra_data.get(t)
            if daily is None or intra is None or intra.empty: continue

            prev_close = float(daily['Close'].squeeze().iloc[-2])
            curr_price = float(intra['Close'].squeeze().iloc[-1])
            change     = (curr_price - prev_close) / prev_close
            if change < 0.10: continue

            today_vol   = float(intra['Volume'].squeeze().sum())
            avg_day_vol = float(daily['Volume'].squeeze().iloc[-20:].mean())
            vol_ratio   = today_vol / avg_day_vol if avg_day_vol > 0 else 0
            if vol_ratio < 0.30: continue

            closes = daily['Close'].squeeze()
            if len(closes) >= 4:
                d1, d2, d3 = (float(closes.iloc[-2]),
                              float(closes.iloc[-3]),
                              float(closes.iloc[-4]))
                if d1 > d2 * 1.05 and d2 > d3 * 1.02:
                    continue

            vwap       = float(calc_vwap(intra).iloc[-1])
            today_high = float(intra['High'].squeeze().max())
            today_low  = float(intra['Low'].squeeze().min())
            launch_pt  = float(intra['Low'].squeeze().iloc[0])
            is_straight = (today_high - curr_price) / today_high < 0.02 if today_high > 0 else True

            pre_candidates.append({
                'ticker'     : t,
                'price'      : round(curr_price, 3),
                'prev_close' : round(prev_close, 3),
                'change_pct' : round(change * 100, 1),
                'vol_ratio'  : round(vol_ratio, 2),
                'vwap'       : round(vwap, 3),
                'today_high' : round(today_high, 3),
                'today_low'  : round(today_low, 3),
                'launch_pt'  : round(launch_pt, 3),
                'is_straight': is_straight,
            })
        except: pass

    print(f"价格/量条件通过：{len(pre_candidates)} 只，检查公告...")

    # 用ann_map做本地查找（无需逐只调API）
    final = []
    for c in pre_candidates:
        code     = c['ticker'].replace('.AX', '')
        ann_info = ann_map.get(code)

        if ann_info is None:
            # 没有今日公告：用yfinance今日新闻作为备选
            news = get_yf_news(code)
            today_news = [n for n in news if n['today']]
            if not today_news:
                print(f"  ❌ {c['ticker']}: 无今日公告/新闻")
                continue
            ann_info = {'headline': today_news[0]['title'], 'sensitive': False}
            c['ann_source'] = 'yfinance'
        else:
            c['ann_source'] = 'asx'

        c['ann_headline']  = ann_info['headline']
        c['ann_sensitive'] = ann_info['sensitive']

        # Gemini分析
        if gemini_client:
            print(f"  🤖 {c['ticker']}: 生成AI分析...")
            c['ai_analysis'] = analyze_candidate(c, ann_info)
        else:
            c['ai_analysis'] = ""

        flag = "✅" if c['ann_source'] == 'asx' else "⚠️"
        print(f"  {flag} {c['ticker']}: +{c['change_pct']}% vol:{c['vol_ratio']}x")
        final.append(c)

    # 保存watchlist
    with open(WATCHLIST_FILE, 'w') as f:
        json.dump({'date': today, 'stocks': final}, f, indent=2, default=str)
    with open(ALERTED_FILE, 'w') as f:
        json.dump({'date': today, 'alerted': []}, f)

    if not final:
        send_telegram(
            f"📋 <b>First Pullback 早盘扫描 {today}</b>\n\n"
            "今日无候选股票。\n"
            "（未发现：有公告 + 涨幅≥10% + 放量 的组合）"
        )
        return

    final.sort(key=lambda x: x['change_pct'], reverse=True)

    lines = [f"⚡ <b>First Pullback 候选 {today}</b>\n"]
    for c in final:
        src_flag = "📋 ASX" if c['ann_source'] == 'asx' else "📰 新闻"
        sen_flag = "⭐ " if c.get('ann_sensitive') else ""
        sl_flag  = "⚠️ 一字拉升" if c['is_straight'] else "✅ 已出现回调空间"
        lines.append(
            f"<b>{c['ticker']}</b>  +{c['change_pct']}%  量:{c['vol_ratio']}x\n"
            f"   现价:{c['price']}  VWAP:{c['vwap']}  高:{c['today_high']}\n"
            f"   {sl_flag}\n"
            f"   {src_flag} {sen_flag}{c['ann_headline']}\n"
        )
        if c.get('ai_analysis'):
            lines.append(f"   🤖 {c['ai_analysis']}\n")

    lines.append(
        "\n等待回踩VWAP后再入场\n"
        "止盈：+10%锁半仓，+20%清仓\n"
        "止损：跌破启动低点或 -8%"
    )
    send_telegram("\n".join(lines))
    print(f"✅ 完成，{len(final)} 个候选")

if __name__ == '__main__':
    run_morning_scan()