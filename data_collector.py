import os
from datetime import datetime

def get_stock_comprehensive_data(ticker):
    """
    根据传入的股票代码（如 MAD.AX），去抓取并清洗以下数据：
    1. 今日量价异动 2. 过去20天技术背景 3. 过去半年的公告时间轴
    """
    # 🌟 这里是我们的时空数据组装逻辑。
    # 我们以 Mader Group (MAD.AX) 触发 First Pullback 并伴随重大利好公告为例：
    
    stock_profile = {
        "ticker": ticker,
        "company_name": "Mader Group Limited",
        "industry": "Mining Services / Industrials",
        
        # 1. 空间维度：今日异动数据
        "today_metrics": {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "close_price": 6.20,
            "change_percent": "+8.5%",
            "volume": "1.2M",
            "volume_ratio": "3.2x", # 达到20日平均成交量的3.2倍
            "trigger_strategy": "First Pullback (首次回踩EMA10强企稳) & 阶段箱体突破"
        },
        
        # 2. 技术时间轴背景（过去20个交易日）
        "technical_background_20d": (
            "5日、20日、60日均线呈现标准多头排列，属于强劲上升趋势。"
            "过去两周在 $5.50 - $5.80 区间进行缩量横盘震荡洗盘。"
            "今日属于一举放量突破该平台，创下阶段性新高，下方 $5.80 转化为强支撑位。"
        ),
        
        # 3. 基本面时间轴背景（过去6个月的核心公告线索，给AI建立历史时间感）
        "announcement_timeline_6m": [
            {"date": "2026-01-15", "title": "Quarterly Activities Report", "impact": "中性：业绩符合预期"},
            {"date": "2026-02-20", "title": "Half Year Accounts & Record Revenue", "impact": "强利好：净利润大增25%"},
            {"date": "2026-04-10", "title": "Expansion into Canadian Mining Market", "impact": "利好：开辟北美新增长极"},
            {"date": "2026-06-01", "title": "Initial Director's Interest Notice (董事入股)", "impact": "强利好：管理层自掏腰包买入"},
            {"date": "2026-06-10", "title": "Today's Core: Record Maintenance Contracts Signed", "impact": "今日催化剂"}
        ],
        
        # 4. 今日催化剂文本摘要
        "today_catalyst_text": (
            "Mader Group 今日发布公告宣布，已与西澳两大铁矿石巨头续签并扩大了核心设备维护合同。"
            "新合同将积压订单额提升了 40%，预计将在 2026 财年下半年直接贡献约 1500 万澳元的营收。"
            "利润率预计将维持在 12.5% 的高位。"
        )
    }
    return stock_profile

def serialize_to_prompt(data):
    """
    将上面清洗好的时空数据，完美组装成符合 Gemini 2.5 深度推理的黄金 Prompt
    """
    timeline_str = ""
    for ann in data["announcement_timeline_6m"]:
        timeline_str += f"  • [{ann['date']}] {ann['title']} ({ann['impact']})\n"
        
    prompt = f"""
你是一位精通澳洲股市（ASX）的资深量化交易员和基本面分析师。
请结合该股的【20日技术背景】和【6个月历史公告事件轴】，对【今日异动】进行深度的交叉交叉推理分析。

=== 【基本信息】 ===
股票代码: {data['ticker']} | 公司名称: {data['company_name']} | 所属行业: {data['industry']}

=== 【1. 今日量价异动数据】 ===
* 当日涨跌幅: {data['today_metrics']['change_percent']} (收盘价: ${data['today_metrics']['close_price']})
* 当日成交量: {data['today_metrics']['volume']} (量比达到 20日均值的 {data['today_metrics']['volume_ratio']})
* 触发量化策略: {data['today_metrics']['trigger_strategy']}

=== 【2. 过去 20 天技术面背景空间】 ===
{data['technical_background_20d']}

=== 【3. 过去 6 个月基本面事件时间轴】 ===
{timeline_str}
=== 【4. 今日核心催化剂公告文本】 ===
{data['today_catalyst_text']}

================ 输出规则 ================
请严格按照以下格式输出一份高价值的“交易员内参”。
1. 语言语态：禁止使用“保证、绝对预测、目标价为”等词汇，保持客观、中性的概率语态，使用专业交易术语。
2. 排版要求：使用适当的 Emoji 增加可读性，适合自媒体和私密社群阅读。
3. 结构强制要求：
   🎯 资金意图研判：结合半年公告背景和今日放量，推演这是机构主力建仓、洗盘结束、还是游资短线利好兑现出货？
   ⚡ 催化剂数字提炼：用大白话剥离公告废话，指出今日公告最硬核的利好数字和潜在业绩影响。
   📉 明日交易对策：给出合情合理的支撑位、阻力位，以及明天开盘的实战下注建议（如回调买入、高抛低吸或分批离场）。
   ⚠️ 风险提示：在末尾强制附带一行该行业或大盘的特有风险提示。
"""
    return prompt
