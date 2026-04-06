#!/usr/bin/env python3
"""
qa_engine_v3.py - 单月 TPO 分析引擎（v3.2，完整版）

核心变更：
1. 使用原始文件读取，跳过格式错误行但保留所有有效数据
2. 默认分析最新月（按文件最后一行）
3. 单月数据展示：完单量、总 TPO、年度 YoY、相邻月趋势、十大场景 TPO
4. 趋势对比：仅相邻月 vs 年度 YoY，不统计多月趋势
5. 场景数据：有啥说啥，不编造缺失场景
6. 按列索引解析，处理"售后服务1.0"、"资产权益0.96"等混合格式

用法：
    python qa_engine_v3.py --file <path> [--month <YYYY/M>]
"""

import argparse
import csv
import re
from typing import Any, Dict, List, Optional

# 业务语义常量
CHANNEL_MAP = {
    "费用疑义tpo": "费用疑义",
    "营销活动tpo": "营销活动",
    "售后服务": "售后服务",  # 2025/11 起
    "资产权益": "资产权益",  # 2025/11 起
}

def load_raw_csv(file_path: str) -> Dict:
    """读取原始 CSV 文件，处理格式不一致问题"""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return {"headers": [], "data": []}

    headers = rows[0]
    data = []

    for row in rows[1:]:
        if len(row) < 3:  # 跳过行数据不足3列的行
            continue
        # 清理每列数据
        cleaned_row = []
        for col in row:
            col = col.strip()
            cleaned_row.append(col)
        data.append(cleaned_row)

    return {"headers": headers, "data": data}

def parse_latest_month(file_path: str) -> str:
    """获取文件最新月份（最后一行）"""
    csv_data = load_raw_csv(file_path)
    if not csv_data["data"]:
        return None

    last_row = csv_data["data"][-1]
    month = last_row[0].strip()  # 第一列是月份
    return month

def get_month_data(file_path: str, month: str) -> Dict:
    """获取指定月份数据（返回行数据和列名索引）"""
    csv_data = load_raw_csv(file_path)
    headers = csv_data["headers"]
    data = csv_data["data"]

    # 找到目标月份数据
    for row in data:
        if month in row[0]:
            # 构建列名索引
            col_index = {headers[i]: i for i in range(len(headers))}
            return {"row": row, "col_index": col_index, "headers": headers}

    return None

def parse_yoy(val: Any) -> Optional[float]:
    """解析 YoY 值（支持百分比字符串和数值）"""
    if val is None or val == '':
        return None
    try:
        if isinstance(val, str):
            val_str = val.strip()
            if '%' in val_str:
                # 提取数字部分（可能包含正负号）
                num_part = val_str.replace('%', '').strip()
                return float(num_part) / 100
            else:
                return float(val_str)
        else:
            return float(val)
    except:
        return None

def analyze_month(file_path: str, month: str = None) -> Dict:
    """
    单月 TPO 分析

    返回：
    {
        "month": "2026/3",
        "order_volume": 28616498,
        "total_tpo": 13.31,
        "yoy": -0.18,  # -18%
        "mom_change": -0.049,  # vs 上月 -4.9%
        "scenarios": [
            {"name": "费用疑义", "tpo": 1.74, "yoy": -0.304},
            {"name": "营销活动", "tpo": 0.85, "yoy": -0.375},
            {"name": "售后服务", "tpo": 1.0, "yoy": None},
            {"name": "资产权益", "tpo": 0.96, "yoy": None},
        ]
    }
    """
    # 如果未指定月份，使用最新月
    if not month:
        month = parse_latest_month(file_path)

    # 获取月份数据
    month_data = get_month_data(file_path, month)
    if not month_data:
        return {"error": f"未找到 {month} 的数据"}

    row = month_data["row"]
    col_index = month_data["col_index"]
    headers = month_data["headers"]

    # 基础数据（按列索引读取）
    def get_col_val_by_idx(idx, default=None):
        if idx is None or idx >= len(row):
            return default
        val = row[idx]
        if val == '' or val is None:
            return default
        return val

    # 列索引映射（固定位置）
    idx_month = 0
    idx_order_volume = 1
    idx_total_tpo = 2
    idx_total_yoy = 3
    idx_fee_tpo = 4
    idx_fee_yoy = 5
    idx_promo_tpo = 7
    idx_promo_yoy = 8
    # 列10-14 可能包含"售后服务"和"资产权益"

    order_volume = get_col_val_by_idx(idx_order_volume)
    total_tpo = get_col_val_by_idx(idx_total_tpo)
    yoy_raw = get_col_val_by_idx(idx_total_yoy)
    yoy = parse_yoy(yoy_raw)

    # 转换数值
    try:
        order_volume = int(order_volume) if order_volume else None
    except:
        order_volume = None

    try:
        total_tpo = float(total_tpo) if total_tpo else None
    except:
        total_tpo = None

    # 场景 TPO 数据
    scenarios = []

    # 费用疑义
    fee_tpo = get_col_val_by_idx(idx_fee_tpo)
    fee_yoy_raw = get_col_val_by_idx(idx_fee_yoy)
    if fee_tpo:
        try:
            scenarios.append({
                "name": "费用疑义",
                "tpo": float(fee_tpo),
                "yoy": parse_yoy(fee_yoy_raw)
            })
        except:
            pass

    # 营销活动
    promo_tpo = get_col_val_by_idx(idx_promo_tpo)
    promo_yoy_raw = get_col_val_by_idx(idx_promo_yoy)
    if promo_tpo:
        try:
            scenarios.append({
                "name": "营销活动",
                "tpo": float(promo_tpo),
                "yoy": parse_yoy(promo_yoy_raw)
            })
        except:
            pass

    # 检测"售后服务"和"资产权益"（可能在列10-14）
    for idx in range(10, 15):
        val = get_col_val_by_idx(idx)
        if val and ("售后服务" in val or "资产权益" in val):
            # 从 "售后服务1.0" 或 "资产权益0.96" 提取场景名和 TPO 值
            match = re.match(r'^(.*?)(\d+\.?\d*)$', str(val))
            if match:
                name = match.group(1).strip()
                tpo_val = match.group(2)
                try:
                    scenarios.append({
                        "name": name,
                        "tpo": float(tpo_val),
                        "yoy": None  # 这些场景没有 YOY 数据
                    })
                except:
                    pass

    # 计算相邻月趋势（MoM）
    mom_change = None
    csv_data = load_raw_csv(file_path)
    data = csv_data["data"]
    months = []
    for r in data:
        months.append(r[0])

    if month in months:
        idx = months.index(month)
        if idx > 0:
            prev_month = months[idx - 1]
            prev_data = get_month_data(file_path, prev_month)
            if prev_data:
                prev_row = prev_data["row"]
                prev_total_tpo_raw = prev_row[idx_total_tpo] if len(prev_row) > idx_total_tpo else None
                try:
                    prev_total_tpo = float(prev_total_tpo_raw) if prev_total_tpo_raw else None
                    if prev_total_tpo and total_tpo and prev_total_tpo != 0:
                        mom_change = (total_tpo - prev_total_tpo) / prev_total_tpo
                except:
                    pass

    return {
        "month": month,
        "order_volume": order_volume,
        "total_tpo": total_tpo,
        "yoy": yoy,
        "mom_change": mom_change,
        "scenarios": scenarios
    }

def format_month_report(analysis: Dict) -> str:
    """格式化单月分析报告"""
    lines = []

    # 标题
    month = analysis.get("month", "N/A")
    lines.append(f"## {month} 月度 TPO 分析")
    lines.append("")

    # 基础数据
    lines.append("### 基础数据")
    lines.append(f"- **完单量**: {analysis.get('order_volume', '-'):,.0f}")
    lines.append(f"- **总 TPO**: {analysis.get('total_tpo', '-'):.2f}")

    # 年度 YoY
    yoy = analysis.get("yoy")
    if yoy is not None:
        yoy_pct = yoy * 100
        yoy_emoji = "[UP]" if yoy >= 0 else "[DOWN]"
        lines.append(f"- **年度 YoY**: {yoy_emoji} {yoy_pct:.1f}%（vs 上年同月）")
    else:
        lines.append(f"- **年度 YoY**: 无数据")

    # 相邻月趋势
    mom_change = analysis.get("mom_change")
    if mom_change is not None:
        mom_pct = mom_change * 100
        mom_emoji = "[UP]" if mom_change >= 0 else "[DOWN]"
        lines.append(f"- **相邻月趋势**: {mom_emoji} {mom_pct:.1f}%（vs 上月）")
    else:
        lines.append(f"- **相邻月趋势**: 无上月数据")

    lines.append("")

    # 场景 TPO
    scenarios = analysis.get("scenarios", [])
    lines.append(f"### 场景 TPO（共 {len(scenarios)} 个场景）")

    for scenario in scenarios:
        name = scenario["name"]
        tpo = scenario["tpo"]
        yoy = scenario.get("yoy")

        if yoy is not None:
            yoy_pct = yoy * 100
            yoy_emoji = "[UP]" if yoy >= 0 else "[DOWN]"
            lines.append(f"- **{name}**: {tpo:.2f} ({yoy_emoji} YoY: {yoy_pct:.1f}%)")
        else:
            lines.append(f"- **{name}**: {tpo:.2f}")

    lines.append("")

    # 汇总
    lines.append("### 数据说明")
    lines.append(f"- 数据来源：test.csv")
    lines.append(f"- 分析月份：{month}")
    lines.append(f"- 场景数量：{len(scenarios)} 个（有数据则展示，无数据则不列）")

    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="单月 TPO 分析引擎 v3.2")
    parser.add_argument("--file", required=True, help="csv 文件路径")
    parser.add_argument("--month", default=None, help="指定月份（默认最新月）")
    args = parser.parse_args()

    analysis = analyze_month(args.file, args.month)

    if "error" in analysis:
        print(f"[ERROR] {analysis['error']}")
    else:
        report = format_month_report(analysis)
        print(report)

if __name__ == "__main__":
    main()
