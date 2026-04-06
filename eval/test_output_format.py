#!/usr/bin/env python3
"""快速测试：验证新 qa_engine 输出是否符合规范"""

import sys
import io
import re
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

CSV_PATH = 'D:/OneProject/OpenAgent/test.csv'


def parse_tpo_csv(path):
    """解析 test.csv"""
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
    for line in raw_lines[1:]:
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


def format_conclusion(month, df_month):
    """生成结论（严格规范：不累计、精确描述、默认最新月）"""
    tpo_total = df_month['平台场景总tpo'].values[0] if len(df_month) > 0 else None
    yoy_total = df_month['平台场景总yoy'].values[0] if len(df_month) > 0 else None

    # 计算环比
    months = sorted(parse_tpo_csv(CSV_PATH)['月'].unique().tolist())
    idx = months.index(month) if month in months else -1
    mom_pct = None
    if idx > 0:
        prev_month = months[idx - 1]
        df_prev = parse_tpo_csv(CSV_PATH)[parse_tpo_csv(CSV_PATH)['月'] == prev_month]
        prev_tpo = df_prev['平台场景总tpo'].values[0] if len(df_prev) > 0 else None
        if tpo_total and prev_tpo and prev_tpo != 0:
            mom_pct = (tpo_total - prev_tpo) / prev_tpo * 100

    # 构建结论
    parts = []
    if tpo_total is not None:
        parts.append(f"{month} 平台总 TPO 为 {tpo_total:.2f}")
    if yoy_total is not None:
        parts.append(f"同比下降 {abs(yoy_total*100):.1f}%" if yoy_total < 0 else f"同比上升 {yoy_total*100:.1f}%")
    if mom_pct is not None:
        parts.append(f"环比下降 {abs(mom_pct):.1f}%" if mom_pct < 0 else f"环比上升 {mom_pct:.1f}%")

    conclusion = "；".join(parts) + "。"
    return conclusion


df = parse_tpo_csv(CSV_PATH)
latest_month = sorted(df['月'].unique().tolist())[-1]
df_latest = df[df['月'] == latest_month]

print('=' * 70)
print('>>> 严格输出格式测试')
print('=' * 70)
print()

print(f'**问题**：最新月各场景的 TPO 和 YOY 是多少？')
print()
print(f'**月份**：{latest_month}')
print()
print(f'**结论**：{format_conclusion(latest_month, df_latest)}')
print()

print('**详细数据**：')
print()
print('### TPO 数据')
print('-' * 40)
tpo_cols = ['平台场景总tpo', '费用疑义tpo', '营销活动tpo', '售后服务tpo', '资产权益tpo']
for col in tpo_cols:
    val = df_latest[col].values[0] if len(df_latest) > 0 else None
    if val is not None:
        print(f'{col:<15} {val:.2f}')

print()
print('### YOY 数据')
print('-' * 40)
yoy_cols = ['平台场景总yoy', '费疑yoy', '营销活动yoy', '售后服务yoy', '资产权益yoy']
for col in yoy_cols:
    val = df_latest[col].values[0] if len(df_latest) > 0 else None
    if val is not None:
        pct = val * 100
        print(f'{col:<15} {pct:.1f}%')
    else:
        print(f'{col:<15} -')

print()
print('**趋势对比**：')
print(f'- 环比（vs上月）：{format_conclusion(latest_month, df_latest).split("；")[0]}')
print(f'- 同比（vs上年同月）：{format_conclusion(latest_month, df_latest).split("；")[1] if "同比" in format_conclusion(latest_month, df_latest) else "-"}')

print()
print('=' * 70)
print('输出验证完毕')
