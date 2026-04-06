#!/usr/bin/env python3
"""
eval/latest_month.py - 最新月数据查询（符合严格输出规范）

功能：
- 查询指定文件的最新月份数据（TPO + YOY）
- 输出格式遵循：
  1. 不累计（仅展示当期数值）
  2. 精确描述（禁用模糊词）
  3. 默认最新月
  4. 仅相邻对比（可选 MoM/YOY）

用法：
    python eval/latest_month.py --file <path> [--mom] [--yoy]
"""

import argparse
import json
import os
import sys
import io
import re
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 导入 data_parser（复用解析逻辑）
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from data_parser import parse_file

CSV_PATH = 'D:/OneProject/OpenAgent/test.csv'  # 默认路径


def parse_tpo_csv(path):
    """解析 test.csv 的特殊格式（列数不一致、渠道名内嵌）"""
    def parse_mixed_value(v):
        v = str(v).strip()
        m = re.search(r'([\-\d.]+)\s*$', v)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass
        try:
            return float(v)
        except Exception:
            return None

    def parse_pct(v):
        v = str(v).strip().replace('%', '')
        try:
            return float(v) / 100
        except Exception:
            return None

    with open(path, encoding='utf-8') as f:
        raw_lines = f.readlines()

    rows = []
    for line in raw_lines[1:]:  # 跳过header
        fields = line.strip().split(',')
        n = len(fields)
        row = {}
        row['月'] = fields[0]
        row['完单量'] = int(fields[1]) if fields[1].strip() else None
        row['平台场景总tpo'] = float(fields[2]) if fields[2].strip() else None
        row['平台场景总yoy'] = parse_pct(fields[3]) if fields[3].strip() else None
        row['费用疑义tpo'] = float(fields[4]) if fields[4].strip() else None
        row['费疑yoy'] = parse_pct(fields[5]) if fields[5].strip() else None
        row['费疑贡献占比'] = parse_pct(fields[6]) if fields[6].strip() else None
        row['营销活动tpo'] = float(fields[7]) if fields[7].strip() else None
        row['营销活动yoy'] = parse_pct(fields[8]) if fields[8].strip() else None
        row['营销贡献占比'] = parse_pct(fields[9]) if fields[9].strip() else None

        if n >= 16:
            row['售后服务tpo'] = parse_mixed_value(fields[10])
            row['售后服务yoy'] = None
            row['售后贡献占比'] = parse_pct(fields[13]) if fields[13].strip() else None
            row['资产权益tpo'] = parse_mixed_value(fields[14])
            row['资产权益yoy'] = None
        else:
            row['售后服务tpo'] = float(fields[10]) if fields[10].strip() else None
            row['售后服务yoy'] = parse_pct(fields[11]) if fields[11].strip() else None
            row['售后贡献占比'] = parse_pct(fields[12]) if fields[12].strip() else None
            row['资产权益tpo'] = float(fields[13]) if fields[13].strip() else None
            row['资产权益yoy'] = parse_pct(fields[14]) if fields[14].strip() else None

        rows.append(row)

    return pd.DataFrame(rows)


def get_latest_month(df):
    """获取最新月份"""
    months = df['月'].dropna().unique().tolist()
    months.sort()
    return months[-1] if months else None


def calc_mom(df, current_month, tpo_col):
    """计算环比（vs上月）"""
    months = sorted(df['月'].dropna().unique().tolist())
    idx = months.index(current_month) if current_month in months else -1
    if idx <= 0:
        return None, None
    prev_month = months[idx - 1]
    curr_val = df[df['月'] == current_month][tpo_col].values[0] if len(df[df['月'] == current_month]) > 0 else None
    prev_val = df[df['月'] == prev_month][tpo_col].values[0] if len(df[df['月'] == prev_month]) > 0 else None
    if curr_val is not None and prev_val is not None and prev_val != 0:
        mom = (curr_val - prev_val) / prev_val * 100
        return prev_month, mom
    return None, None


def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '-'
    return f'{v*100:.1f}%' if abs(v) < 1 else f'{v:.1f}%'


def fmt_float(v, decimals=2):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return '-'
    return f'{v:.{decimals}f}'


def main():
    parser = argparse.ArgumentParser(description="最新月数据查询")
    parser.add_argument("--file", default=CSV_PATH, help="xlsx 或 csv 文件路径")
    parser.add_argument("--mom", action="store_true", help="显示环比（vs上月）")
    parser.add_argument("--yoy", action="store_true", help="显示同比（vs上年同月）")
    args = parser.parse_args()

    df = parse_tpo_csv(args.file)
    latest_month = get_latest_month(df)

    if not latest_month:
        print("未找到有效月份")
        return

    df_latest = df[df['月'] == latest_month]

    print('=' * 70)
    print(f'>>> 最新月数据查询结果：{latest_month}')
    print('=' * 70)
    print()

    # TPO 数据
    tpo_cols = ['平台场景总tpo', '费用疑义tpo', '营销活动tpo', '售后服务tpo', '资产权益tpo']
    tpo_data = []
    for col in tpo_cols:
        val = df_latest[col].values[0] if len(df_latest) > 0 else None
        tpo_data.append({"场景": col, "TPO": fmt_float(val)})

    df_tpo = pd.DataFrame(tpo_data)

    print('[TPO 数据]')
    print('-' * 40)
    print(df_tpo.to_string(index=False))
    print()

    # YOY 数据
    yoy_cols = ['平台场景总yoy', '费疑yoy', '营销活动yoy', '售后服务yoy', '资产权益yoy']
    yoy_data = []
    for col in yoy_cols:
        val = df_latest[col].values[0] if len(df_latest) > 0 else None
        yoy_data.append({"场景": col, "YOY": fmt_pct(val)})

    df_yoy = pd.DataFrame(yoy_data)

    print('[YOY 数据]')
    print('-' * 40)
    print(df_yoy.to_string(index=False))
    print()

    # 环比（可选）
    if args.mom:
        print('[环比（vs上月）]')
        print('-' * 40)
        mom_data = []
        for col in tpo_cols:
            prev_month, mom = calc_mom(df, latest_month, col)
            curr_val = df_latest[col].values[0] if len(df_latest) > 0 else None
            row = {"场景": col, f"{latest_month} TPO": fmt_float(curr_val)}
            if prev_month:
                row["上月 TPO"] = fmt_float(df[df['月'] == prev_month][col].values[0] if len(df[df['月'] == prev_month]) > 0 else None)
                row["环比(%)"] = fmt_float(mom / 100)
            else:
                row["上月 TPO"] = "-"
                row["环比(%)"] = "-"
            mom_data.append(row)

        df_mom = pd.DataFrame(mom_data)
        print(df_mom.to_string(index=False))
        print()

    print('=' * 70)
    print('查询完毕')


if __name__ == '__main__':
    main()
