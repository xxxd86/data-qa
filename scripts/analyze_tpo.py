import sys
import io
import re
import pandas as pd
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

CSV_PATH = 'D:/OneProject/OpenAgent/test.csv'


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


def load_data(path):
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
            # 2025/11 之后格式：字段10=售后服务X.XX，字段13=售后贡献占比，字段14=资产权益X.XX
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


def fmt_pct(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return '-'
    return f'{v*100:.1f}%'


def fmt_float(v, decimals=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return '-'
    return f'{v:.{decimals}f}'


def fmt_int(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return '-'
    return f'{int(v):,}'


df = load_data(CSV_PATH)

print('=' * 70)
print('>>> TPO 分场景分析报告 (2025/1 - 2026/3)')
print('=' * 70)

print()
print('[1] 数据概览：各月完单量 + 平台场景总 TPO')
print('-' * 50)
for _, r in df.iterrows():
    yoy = fmt_pct(r['平台场景总yoy'])
    print(f"  {r['月']:<8}  完单量={fmt_int(r['完单量'])}  总TPO={fmt_float(r['平台场景总tpo'])}  YOY={yoy}")

print()
print('[2] 分场景 TPO 汇总（均值）')
print('-' * 50)
scenes = {
    '费用疑义': ('费用疑义tpo', '费疑yoy'),
    '营销活动': ('营销活动tpo', '营销活动yoy'),
    '售后服务': ('售后服务tpo', '售后服务yoy'),
    '资产权益': ('资产权益tpo', '资产权益yoy'),
}
for name, (tcol, ycol) in scenes.items():
    vals = df[tcol].dropna()
    yoy_vals = df[ycol].dropna()
    avg_tpo = vals.mean()
    latest = df.iloc[-1][tcol]
    earliest = df.iloc[0][tcol]
    change = (latest - earliest) / earliest if earliest else None
    avg_yoy = yoy_vals.mean() if len(yoy_vals) else None
    print(f"  {name:<6}  均值TPO={fmt_float(avg_tpo)}  最新={fmt_float(latest)}  "
          f"首->末变化={fmt_pct(change)}  平均YOY={fmt_pct(avg_yoy)}")

print()
print('[3] 各场景 TPO 历史明细')
print('-' * 70)
header = f"{'月':<8}  {'费用疑义':>7}  {'营销活动':>7}  {'售后服务':>7}  {'资产权益':>7}  {'平台总':>7}"
print(header)
print('-' * 70)
for _, r in df.iterrows():
    row_str = (f"{r['月']:<8}  "
               f"{fmt_float(r['费用疑义tpo']):>7}  "
               f"{fmt_float(r['营销活动tpo']):>7}  "
               f"{fmt_float(r['售后服务tpo']):>7}  "
               f"{fmt_float(r['资产权益tpo']):>7}  "
               f"{fmt_float(r['平台场景总tpo']):>7}")
    print(row_str)

print()
print('[4] 各场景 YOY 历史明细（2025/11 及之后无 YOY 为正常）')
print('-' * 70)
header2 = f"{'月':<8}  {'费疑YOY':>9}  {'营销YOY':>9}  {'售后YOY':>9}  {'资产YOY':>9}  {'总YOY':>9}"
print(header2)
print('-' * 70)
for _, r in df.iterrows():
    row_str = (f"{r['月']:<8}  "
               f"{fmt_pct(r['费疑yoy']):>9}  "
               f"{fmt_pct(r['营销活动yoy']):>9}  "
               f"{fmt_pct(r['售后服务yoy']):>9}  "
               f"{fmt_pct(r['资产权益yoy']):>9}  "
               f"{fmt_pct(r['平台场景总yoy']):>9}")
    print(row_str)

print()
print('[5] TPO 趋势分析：各场景最高/最低月份')
print('-' * 50)
scene_cols = ['费用疑义tpo', '营销活动tpo', '售后服务tpo', '资产权益tpo', '平台场景总tpo']
scene_names = ['费用疑义', '营销活动', '售后服务', '资产权益', '平台总']
for col, name in zip(scene_cols, scene_names):
    valid = df[[' 月' if '月' not in df.columns else '月', col]].dropna()
    valid = df[['月', col]].dropna()
    if len(valid) == 0:
        continue
    max_idx = valid[col].idxmax()
    min_idx = valid[col].idxmin()
    print(f"  {name:<6}  最高: {valid.loc[max_idx,'月']}={fmt_float(valid.loc[max_idx,col])}  "
          f"最低: {valid.loc[min_idx,'月']}={fmt_float(valid.loc[min_idx,col])}")

print()
print('[6] 贡献占比分析（最新月 2026/3）')
print('-' * 50)
latest = df.iloc[-1]
contribs = {
    '费用疑义': latest['费疑贡献占比'],
    '营销活动': latest['营销贡献占比'],
    '售后服务': latest['售后贡献占比'],
}
for name, v in contribs.items():
    print(f"  {name}: {fmt_pct(v)}")

print()
print('[7] 重点异常：TPO 下降超 30% 的场景 × 月份')
print('-' * 50)
for name, (tcol, ycol) in scenes.items():
    bad = df[df[ycol].notna() & (df[ycol] < -0.30)][['月', tcol, ycol]]
    if len(bad):
        for _, r in bad.iterrows():
            print(f"  {name} | {r['月']} | TPO={fmt_float(r[tcol])} | YOY={fmt_pct(r[ycol])}")

print()
print('=' * 70)
print('分析完毕')
