#!/usr/bin/env python3
"""
data_parser.py - 数据文件解析器与结构发现模块

功能：
- 支持 xlsx / csv 文件读取
- 自动发现字段结构（类型、统计信息、示例值）
- 基于文件哈希检查记忆缓存，避免重复解析
- 输出结构摘要 JSON，供 qa_engine.py 使用

用法：
    python data_parser.py --file <path> [--memory-dir <dir>] [--output-json] [--force-reparse]
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime

import pandas as pd


# ── 业务语义映射（与 SKILL.md 保持同步）──────────────────────────────────────
CHANNEL_NAME_MAP = {
    "微信渠道客诉": "资产权益",
    "二次号": "售后服务",
}

# YOY 空值豁免月份
YOY_NULL_EXEMPT_MONTHS = {"2025/11", "2025/12", "2025-11", "2025-12"}


def compute_file_hash(file_path: str) -> str:
    """计算文件 MD5 哈希，用于记忆缓存键"""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_file(file_path: str) -> pd.DataFrame:
    """加载 xlsx 或 csv 文件，返回 DataFrame"""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        df = pd.read_excel(file_path, engine="openpyxl")
    elif ext == ".csv":
        # 尝试 UTF-8，失败则 GBK
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="gbk")
    else:
        raise ValueError(f"不支持的文件格式: {ext}，仅支持 .xlsx / .csv")
    return df


def detect_column_type(series: pd.Series) -> str:
    """检测列的语义类型"""
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    # 尝试解析为日期
    try:
        parsed = pd.to_datetime(series.dropna().astype(str).head(5), errors="coerce")
        if parsed.notna().sum() >= 3:
            return "datetime_string"
    except Exception:
        pass
    # 判断是否为分类（唯一值少）
    unique_ratio = series.nunique() / max(len(series.dropna()), 1)
    if unique_ratio < 0.3 and series.nunique() < 50:
        return "categorical"
    return "text"


def analyze_column(series: pd.Series) -> dict:
    """分析单列，返回统计摘要"""
    col_type = detect_column_type(series)
    info = {
        "name": series.name,
        "type": col_type,
        "null_count": int(series.isna().sum()),
        "null_pct": round(series.isna().mean() * 100, 1),
        "unique_count": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().head(3).tolist()],
    }
    if col_type == "numeric":
        non_null = series.dropna()
        if len(non_null) > 0:
            info.update({
                "min": round(float(non_null.min()), 4),
                "max": round(float(non_null.max()), 4),
                "mean": round(float(non_null.mean()), 4),
                "sum": round(float(non_null.sum()), 4),
            })
    if col_type == "categorical":
        top = series.value_counts().head(5)
        info["top_values"] = {str(k): int(v) for k, v in top.items()}
    return info


def apply_business_mapping(columns: list) -> dict:
    """将字段名映射到业务名称，返回 {原始列名: 业务名称} 字典"""
    mapping = {}
    for col in columns:
        col_str = str(col)
        for old_name, new_name in CHANNEL_NAME_MAP.items():
            if old_name in col_str:
                mapping[col_str] = col_str.replace(old_name, new_name)
                break
    return mapping


def parse_file(file_path: str, memory_dir: str = None, force_reparse: bool = False) -> dict:
    """
    主解析函数。
    返回结构摘要字典，包含：
    - file_info: 文件基本信息
    - columns: 各列分析结果
    - business_mapping: 渠道名称映射
    - from_cache: 是否从记忆加载
    - shape: (行数, 列数)
    """
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")

    file_hash = compute_file_hash(file_path)
    memory_dir = memory_dir or os.path.join(os.path.dirname(__file__), "..", "memory")
    os.makedirs(memory_dir, exist_ok=True)
    datasets_path = os.path.join(memory_dir, "datasets.json")

    # ── 检查记忆缓存 ──────────────────────────────────────
    if not force_reparse and os.path.exists(datasets_path):
        with open(datasets_path, "r", encoding="utf-8") as f:
            datasets = json.load(f)
        if file_hash in datasets:
            cached = datasets[file_hash]
            cached["from_cache"] = True
            cached["file_hash"] = file_hash
            print(f"[data_parser] 从记忆加载数据结构: {cached['file_name']}", file=sys.stderr)
            return cached

    # ── 执行完整解析 ──────────────────────────────────────
    print(f"[data_parser] 解析文件: {file_path}", file=sys.stderr)
    df = load_file(file_path)

    columns_info = []
    for col in df.columns:
        col_info = analyze_column(df[col])
        columns_info.append(col_info)

    business_mapping = apply_business_mapping(list(df.columns))

    # 自动检测时间列
    time_columns = [c["name"] for c in columns_info if c["type"] in ("datetime", "datetime_string")]
    numeric_columns = [c["name"] for c in columns_info if c["type"] == "numeric"]
    categorical_columns = [c["name"] for c in columns_info if c["type"] == "categorical"]

    summary = {
        "file_name": os.path.basename(file_path),
        "file_path": file_path,
        "file_hash": file_hash,
        "created_at": datetime.now().isoformat(),
        "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        "columns": columns_info,
        "time_columns": time_columns,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "business_mapping": business_mapping,
        "from_cache": False,
    }

    # ── 存储数据记忆 ──────────────────────────────────────
    if os.path.exists(datasets_path):
        with open(datasets_path, "r", encoding="utf-8") as f:
            datasets = json.load(f)
    else:
        datasets = {}

    datasets[file_hash] = summary
    with open(datasets_path, "w", encoding="utf-8") as f:
        json.dump(datasets, f, ensure_ascii=False, indent=2)

    print(f"[data_parser] 解析完成，已存储数据记忆: {file_hash[:8]}...", file=sys.stderr)
    return summary


def print_summary(summary: dict):
    """打印人类可读的结构摘要"""
    print(f"\n{'='*60}")
    print(f"文件: {summary['file_name']}")
    print(f"数据规模: {summary['shape']['rows']} 行 × {summary['shape']['cols']} 列")
    if summary.get("from_cache"):
        print("[CACHE] 已从记忆缓存加载（文件未变更）")
    print(f"\n字段列表:")
    for col in summary["columns"]:
        null_info = f"(空值: {col['null_pct']}%)" if col["null_count"] > 0 else ""
        stats = ""
        if col["type"] == "numeric" and "min" in col:
            stats = f"  范围: [{col['min']}, {col['max']}]  均值: {col['mean']}"
        print(f"  - {col['name']} [{col['type']}] {null_info} 示例: {col['sample_values'][:2]}{stats}")

    if summary.get("business_mapping"):
        print(f"\n业务渠道映射:")
        for old, new in summary["business_mapping"].items():
            print(f"  {old} → {new}")


def main():
    parser = argparse.ArgumentParser(description="解析 xlsx/csv 文件并生成结构摘要")
    parser.add_argument("--file", required=True, help="xlsx 或 csv 文件路径")
    parser.add_argument("--memory-dir", default=None, help="记忆存储目录（默认: ../memory/）")
    parser.add_argument("--output-json", action="store_true", help="输出 JSON 格式摘要（供程序调用）")
    parser.add_argument("--force-reparse", action="store_true", help="强制重新解析，忽略记忆缓存")
    args = parser.parse_args()

    summary = parse_file(args.file, args.memory_dir, args.force_reparse)

    if args.output_json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print_summary(summary)


if __name__ == "__main__":
    main()
