# ============================================================
# FIRST PULLBACK — MORNING SCANNER v2
# 新增：每个候选股的Gemini AI简要分析
# 每日 10:30am AEST (00:30 UTC) 运行
# ============================================================

import os, json, time
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, date
from google import genai

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "你的TOKEN")
CHAT_ID        = os.environ.get("TELEGRAM_CHAT_ID", "7553937057")
GEMINI_KEY     = os.environ.get("GEMINI_API_KEY", "")
WATCHLIST_FILE = "watchlist.json"
ALERTED_FILE   = "alerted.json"

gemini_client  = genai.Client(api_key=GEMINI_KEY) if GEMINI_KEY else None

# ── Gemini分析 ────────────────────────────────────────────────
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

def analyze_candidate(c: dict) -> str:
    """为候选股生成简短AI分析"""
    prompt = f"""你是一位严谨的ASX股票分析师。

候选股：{c['ticker']}
今日涨幅：+{c['change_pct']}%
成交量：{c['vol_ratio']}x日均量
今日公告：{c.get('ann_title') or '未确认'}
当前价格：${c['price']}
今日最高：${c['today_high']}
VWAP：${c['vwap']}

请用1-2句中文简要说明：
1. 这个公告对股价的驱动逻辑是否成立
2. 主要风险点

要求：只陈述可判断的内容，不确定的直接说"需进一步核查"。"""
    return ask_gemini(prompt)

# ── 获取ASX股票池 ─────────────────────────────────────────────
def get_asx_universe():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        df = pd.read_csv(url, skiprows=1, encoding='latin1')
        col = next((c for c in df.columns if 'code' in c.lower()), None)
        if not col: return []
        codes = df[col].dropna().astype(str).str.strip()
        valid = codes[codes.str.match(r'^[A-Z]{1,5}$')]
        return [f"{c}.AX" for c in valid]
    except Exception as e:
        print(f"获取列表失败: {e}")
        return []

# ── 批量下载日线数据 ──────────────────────────────────────────
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

# ── 批量下载盘中5分钟数据 ─────────────────────────────────────
def batch_intraday(tickers, batch_size=50):
    all_data = {}
    n = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        bn = i // batch_size + 1
        if bn % 5 == 1: print(f"  盘中数据 {bn}/{n}批...")
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

# ── 检查ASX公告 ───────────────────────────────────────────────
def check_announcement(code):
    today = date.today().strftime('%Y-%m-%d')
    url = (f"https://www.asx.com.au/asx/1/company/{code}"
           f"/announcements?count=10&market_sensitive=false")
    try:
        r = requests.get(url, timeout=8,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None, None
        for ann in r.json().get('data', []):
            if str(ann.get('document_release_date', ''))[:10] == today:
                sensitive = ann.get('market_sensitive', False)
                title = ann.get('header', '')[:70]
                return True, f"{'⭐ 市场敏感 ' if sensitive else ''}{title}"
        return False, None
    except:
        return None, None

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
def run_morning_scan():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\n[{today}] First Pullback 早盘扫描开始...")

    universe = get_asx_universe()
    if not universe: return
    print(f"股票池：{len(universe)} 只")

    print("下载日线数据...")
    daily_data = batch_daily(universe, batch_size=100)

    liquid = []
    for t, df in daily_data.items():
        try:
            avg_vol = float(df['Volume'].iloc[-20:].mean())
            price   = float(df['Close'].iloc[-1])
            if avg_vol * price >= 300_000:
                liquid.append(t)
        except: pass
    print(f"流动性过滤后：{len(liquid)} 只")

    print("下载盘中数据...")
    intra_data = batch_intraday(liquid, batch_size=50)

    # 价格和量条件过滤
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
                d1, d2, d3 = float(closes.iloc[-2]), float(closes.iloc[-3]), float(closes.iloc[-4])
                if d1 > d2 * 1.05 and d2 > d3 * 1.02:
                    continue

            vwap_series = calc_vwap(intra)
            vwap        = float(vwap_series.iloc[-1])
            today_high  = float(intra['High'].squeeze().max())
            today_low   = float(intra['Low'].squeeze().min())
            launch_pt   = float(intra['Low'].squeeze().iloc[0])
            pullback_d  = (today_high - curr_price) / today_high if today_high > 0 else 0
            is_straight = pullback_d < 0.02

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
                'ann_has'    : None,
                'ann_title'  : None,
                'ai_analysis': None,
            })
        except: pass

    print(f"价格/量条件通过：{len(pre_candidates)} 只，核查公告...")

    # 公告核查 + AI分析
    final = []
    for c in pre_candidates:
        code = c['ticker'].replace('.AX', '')
        has, title = check_announcement(code)
        c['ann_has']   = has
        c['ann_title'] = title
        time.sleep(0.2)

        if has is False:
            print(f"  ❌ {c['ticker']}: 今日无公告")
            continue

        # Gemini分析（技术指标first，然后结合公告）
        if gemini_client:
            print(f"  🤖 {c['ticker']}: 生成AI分析...")
            c['ai_analysis'] = analyze_candidate(c)

        flag = "✅" if has else "⚠️"
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
        print("无候选股票。")
        return

    final.sort(key=lambda x: x['change_pct'], reverse=True)

    # 发送汇总 + 逐股详情
    lines = [f"⚡ <b>First Pullback 候选 {today}</b>\n"]
    for c in final:
        ann_flag = "⭐" if c['ann_has'] else "⚠️"
        sl_flag  = "⚠️ 一字拉升未回调" if c['is_straight'] else "✅ 已出现回调空间"
        lines.append(
            f"{ann_flag} <b>{c['ticker']}</b>  "
            f"+{c['change_pct']}%  量:{c['vol_ratio']}x\n"
            f"   现价:{c['price']}  VWAP:{c['vwap']}  高:{c['today_high']}\n"
            f"   {sl_flag}\n"
            f"   📋 {c.get('ann_title') or '请手动核查公告'}\n"
        )
        if c.get('ai_analysis'):
            lines.append(f"   🤖 {c['ai_analysis']}\n")

    lines.append(
        "\n等待价格回踩VWAP后再入场\n"
        "止盈：+10%锁半仓，+20%清仓\n"
        "止损：跌破启动低点或 -8%"
    )

    send_telegram("\n".join(lines))
    print(f"✅ 完成，{len(final)} 个候选已发送Telegram")

if __name__ == '__main__':
    run_morning_scan()
