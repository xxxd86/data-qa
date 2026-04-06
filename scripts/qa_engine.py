#!/usr/bin/env python3
"""
qa_engine.py - 自然语言数据问答引擎（v2.0，迭代版）

核心变更：
1. 禁止累计计算（sum/avg/trend/aggregate）
2. 禁止模糊描述词（剧烈、明显、持续等）
3. 默认返回最新月数据
4. 仅支持相邻月对比（MoM）和年度同比（YOY）

支持的意图：
- single_month: 单月查询（默认最新月）
- compare: 两场景对比
- mom: 环比（vs上月）
- yoy: 同比（vs上年同月）
- max/min: 排名查询
- filter: 异常检测

用法：
    python qa_engine.py --file <path> --question "<问题>" [--memory-dir <dir>]
"""

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入本地模块
sys.path.insert(0, os.path.dirname(__file__))
from data_parser import parse_file, load_file
from memory_manager import search_qa, save_qa_pair, init_db

# ── 业务语义常量 ──────────────────────────────────────────────────────────────
CHANNEL_NAME_MAP = {
    "微信渠道客诉": "资产权益",
    "二次号": "售后服务",
}
CHANNEL_NAME_REVERSE = {v: k for k, v in CHANNEL_NAME_MAP.items()}

# YOY 空值豁免月份（不报异常）
YOY_NULL_EXEMPT = {"2025/11", "2025/12", "2025-11", "2025-12", "202511", "202512"}

# 意图关键词（仅支持精确查询）
TPO_KEYWORDS = ["tpo", "人工效率", "进线完单比"]
YOY_KEYWORDS = ["yoy", "同比", "年度环比", "年同比", "同比（vs上年同月）"]
MOM_KEYWORDS = ["环比", "vs上月", "vs上个月", "环比（vs上月）", "mom"]
COMPARE_KEYWORDS = ["对比", "比较", "vs", "versus", "相比", "差异"]
MAX_KEYWORDS = ["最高", "最多", "最大", "top", "排名"]
MIN_KEYWORDS = ["最低", "最少", "最小"]
FILTER_KEYWORDS = ["异常", "零值", "空值", "tpo=0", "tpon=0", "yoy<"]
MONTH_FILTERS = ["最新月", "最新", "最近", "本月", "当月", "最新月份"]


def normalize_channel_name(text: str) -> str:
    """将旧渠道名映射为当前业务名称"""
    for old, new in CHANNEL_NAME_MAP.items():
        text = text.replace(old, new)
    return text


def detect_intent(question: str) -> dict:
    """
    解析问题意图，返回意图字典：
    {
        "intent": "single_month" | "compare" | "mom" | "yoy" | "max" | "min" | "filter",
        "keywords": [...],
        "month_filter": "2025/01" | "latest" | None,
        "channel_filter": "资产权益" | None,
        "n": 3  (top N)
    }
    """
    q = question.lower()
    q_norm = normalize_channel_name(q)

    intent = {
        "intent": "single_month",  # 默认：单月查询
        "keywords": [],
        "month_filter": None,
        "channel_filter": None,
        "n": None,
        "raw_question": question,
    }

    # ── 意图检测（优先级从高到低）────────────────────────────
    if any(kw in q for kw in MOM_KEYWORDS):
        intent["intent"] = "mom"
    elif any(kw in q for kw in YOY_KEYWORDS):
        intent["intent"] = "yoy"
    elif any(kw in q for kw in COMPARE_KEYWORDS):
        intent["intent"] = "compare"
    elif any(kw in q for kw in MAX_KEYWORDS):
        intent["intent"] = "max"
        m = re.search(r"(?:最高|最多|最大|top)(\d+)", q)
        if m:
            intent["n"] = int(m.group(1))
        else:
            intent["n"] = 1
    elif any(kw in q for kw in MIN_KEYWORDS):
        intent["intent"] = "min"
        intent["n"] = 1
    elif any(kw in q for kw in FILTER_KEYWORDS):
        intent["intent"] = "filter"
    else:
        intent["intent"] = "single_month"

    # ── 时间过滤检测 ──────────────────────────────────────────
    # 匹配 "最新月"、"2025/01" 等
    for kw in MONTH_FILTERS:
        if kw in q:
            intent["month_filter"] = "latest"
            break

    if not intent.get("month_filter"):
        time_patterns = [
            r"20\d{2}[/-]\d{1,2}",    # 2025/01, 2025-01
            r"20\d{2}\d{2}",           # 202501
            r"\d{4}年\d{1,2}月",       # 2025年1月
        ]
        for pattern in time_patterns:
            m = re.search(pattern, question)
            if m:
                intent["month_filter"] = m.group(0)
                break

    # ── 渠道过滤检测 ──────────────────────────────────────────
    all_channels = list(CHANNEL_NAME_MAP.values()) + list(CHANNEL_NAME_MAP.keys())
    for ch in all_channels:
        if ch in question or ch in q_norm:
            intent["channel_filter"] = CHANNEL_NAME_MAP.get(ch, ch)
            break

    return intent


def find_relevant_columns(df: pd.DataFrame, intent: dict, summary: dict) -> dict:
    """
    根据意图和数据结构，找到最相关的列：
    返回 {"time_col": ..., "tpo_cols": {...}, "yoy_cols": {...}, "channel_cols": [...]}
    """
    cols = {
        "time_col": None,
        "tpo_cols": {},  # {"费用疑义tpo": ..., "营销活动tpo": ...}
        "yoy_cols": {},
        "channel_cols": [],
    }

    # 时间列
    for col in df.columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in ["月", "日期", "时间", "date", "month"]):
            cols["time_col"] = col
            break

    # TPO 列（test.csv 格式：费用疑义tpo、营销活动tpo、售后服务tpo、资产权益tpo、平台场景总tpo）
    for col in df.columns:
        col_lower = str(col).lower()
        if "tpo" in col_lower and "yoy" not in col_lower:
            cols["tpo_cols"][col] = col

    # YOY 列
    for col in df.columns:
        col_lower = str(col).lower()
        if "yoy" in col_lower or "同比" in col_lower:
            cols["yoy_cols"][col] = col

    # 场景列（test.csv 无单独渠道列，TPO/YOY 列名包含场景名）
    for col in df.columns:
        col_lower = str(col).lower()
        if any(ch in col_lower for ch in ["费用疑义", "营销活动", "售后服务", "资产权益", "平台场景"]):
            cols["channel_cols"].append(col)

    return cols


def get_latest_month(df: pd.DataFrame, time_col: str) -> str:
    """获取最新月份"""
    if time_col not in df.columns:
        return None
    # 假设时间列是字符串格式，按字典序排序即可（test.csv 为 2025/1, 2025/2...）
    months = df[time_col].dropna().unique().tolist()
    months.sort()
    return months[-1] if months else None


def execute_intent(df: pd.DataFrame, intent: dict, col_info: dict, question: str) -> dict:
    """
    根据意图执行数据操作，返回结果字典：
    {"conclusion": str, "data": DataFrame or scalar, "type": "table"|"scalar"|"list", "month": str}
    """
    q = intent["intent"]
    time_col = col_info.get("time_col")
    tpo_cols = col_info.get("tpo_cols", {})
    yoy_cols = col_info.get("yoy_cols", {})

    # ── 确定目标月份 ──────────────────────────────────────────
    target_month = None
    if intent.get("month_filter") == "latest":
        target_month = get_latest_month(df, time_col)
    elif intent.get("month_filter"):
        target_month = intent["month_filter"]
    else:
        # 默认最新月
        target_month = get_latest_month(df, time_col)

    # ── 筛选目标月数据 ────────────────────────────────────────
    df_month = df.copy()
    if time_col and target_month:
        mask = df_month[time_col].astype(str).str.contains(re.escape(target_month), na=False)
        df_month = df_month[mask]

    # ── 根据意图执行操作 ──────────────────────────────────────
    if q == "single_month":
        return _execute_single_month(df, df_month, intent, col_info, target_month)

    elif q == "compare":
        return _execute_compare(df, df_month, intent, col_info, target_month)

    elif q == "mom":
        return _execute_mom(df, intent, col_info, target_month)

    elif q == "yoy":
        return _execute_yoy(df_month, intent, col_info, target_month)

    elif q in ["max", "min"]:
        return _execute_rank(df_month, intent, col_info, target_month)

    elif q == "filter":
        return _execute_filter(df, df_month, intent, col_info, target_month)

    else:
        return {"conclusion": "未知意图", "data": None, "type": "error", "month": target_month}


def _execute_single_month(df: pd.DataFrame, df_month: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """返回指定月份的所有场景 TPO 和 YOY"""
    tpo_cols = col_info.get("tpo_cols", {})
    yoy_cols = col_info.get("yoy_cols", {})

    # 构建 TPO 结果
    tpo_data = []
    yoy_data = []

    for col in df_month.columns:
        # TPO 列
        if "tpo" in str(col).lower() and "yoy" not in str(col).lower():
            val = df_month[col].values[0] if len(df_month) > 0 else None
            if pd.notna(val):
                tpo_data.append({"场景": col, "TPO": float(val)})

        # YOY 列
        elif "yoy" in str(col).lower() or "同比" in str(col).lower():
            val = df_month[col].values[0] if len(df_month) > 0 else None
            if pd.notna(val):
                # 如果是百分比格式字符串，转换为小数
                if isinstance(val, str):
                    try:
                        val = float(val.strip("%")) / 100
                    except:
                        val = None
                if val is not None:
                    yoy_data.append({"场景": col, "YOY": float(val)})

    df_tpo = pd.DataFrame(tpo_data)
    df_yoy = pd.DataFrame(yoy_data)

    conclusion = f"{month} 各场景 TPO 及 YOY 数据"
    return {
        "conclusion": conclusion,
        "data": {"TPO": df_tpo, "YOY": df_yoy},
        "type": "multi_table",
        "month": month,
    }


def _execute_compare(df: pd.DataFrame, df_month: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """对比两个场景"""
    ch = intent.get("channel_filter")
    tpo_cols = col_info.get("tpo_cols", {})

    if not ch:
        return {"conclusion": "请指定要对比的场景", "data": None, "type": "error", "month": month}

    # 找到包含该场景名的 TPO 列
    tpo_col = None
    for col in tpo_cols:
        if ch in str(col):
            tpo_col = col
            break

    if not tpo_col:
        return {"conclusion": f"未找到场景 {ch} 的 TPO 列", "data": None, "type": "error", "month": month}

    val = df_month[tpo_col].values[0] if len(df_month) > 0 else None

    return {
        "conclusion": f"{month} 场景 {ch} 的 TPO 为 {val if pd.notna(val) else '-'}",
        "data": pd.DataFrame({"场景": [ch], "TPO": [val]}) if pd.notna(val) else None,
        "type": "scalar",
        "month": month,
    }


def _execute_mom(df: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """环比（vs上月）"""
    tpo_cols = col_info.get("tpo_cols", {})
    time_col = col_info.get("time_col")

    if not time_col:
        return {"conclusion": "未找到时间列", "data": None, "type": "error", "month": month}

    # 获取月份列表
    months = sorted(df[time_col].dropna().unique().tolist())
    idx = months.index(month) if month in months else -1

    if idx <= 0:
        return {"conclusion": f"{month} 无上月数据", "data": None, "type": "error", "month": month}

    prev_month = months[idx - 1]

    # 筛选当月和上月
    df_curr = df[df[time_col] == month]
    df_prev = df[df[time_col] == prev_month]

    # 计算各场景的 MoM
    results = []
    for col in tpo_cols:
        curr_val = df_curr[col].values[0] if len(df_curr) > 0 else None
        prev_val = df_prev[col].values[0] if len(df_prev) > 0 else None

        if pd.notna(curr_val) and pd.notna(prev_val) and prev_val != 0:
            mom = (curr_val - prev_val) / prev_val * 100
            results.append({"场景": col, f"{month} TPO": curr_val, f"{prev_month} TPO": prev_val, "环比(%)": mom})

    df_result = pd.DataFrame(results)

    return {
        "conclusion": f"{month} vs {prev_month} 环比计算",
        "data": df_result,
        "type": "table",
        "month": month,
    }


def _execute_yoy(df_month: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """同比（vs上年同月）"""
    yoy_cols = col_info.get("yoy_cols", {})

    if not yoy_cols:
        return {"conclusion": "未找到 YOY 列", "data": None, "type": "error", "month": month}

    results = []
    for col in yoy_cols:
        val = df_month[col].values[0] if len(df_month) > 0 else None
        if pd.notna(val):
            if isinstance(val, str):
                try:
                    val = float(val.strip("%")) / 100
                except:
                    val = None
            if val is not None:
                results.append({"场景": col, "YOY": val, "YOY(%)": val * 100})

    df_result = pd.DataFrame(results)

    return {
        "conclusion": f"{month} 年度同比（vs上年同月）",
        "data": df_result,
        "type": "table",
        "month": month,
    }


def _execute_rank(df_month: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """排名查询（TPO 最高/最低的场景）"""
    tpo_cols = col_info.get("tpo_cols", {})
    n = intent.get("n", 1)

    if intent["intent"] == "max":
        df_sorted = pd.DataFrame(
            sorted([{"场景": col, "TPO": df_month[col].values[0] if len(df_month) > 0 else None} for col in tpo_cols],
                   key=lambda x: x.get("TPO") or -float("inf"),
                   reverse=True)
        )
    else:  # min
        df_sorted = pd.DataFrame(
            sorted([{"场景": col, "TPO": df_month[col].values[0] if len(df_month) > 0 else None} for col in tpo_cols],
                   key=lambda x: x.get("TPO") or float("inf"))
        )

    df_result = df_sorted.head(n)

    rank_type = "最高" if intent["intent"] == "max" else "最低"
    conclusion = f"{month} TPO {rank_type}的前{n}个场景"

    return {
        "conclusion": conclusion,
        "data": df_result,
        "type": "table",
        "month": month,
    }


def _execute_filter(df: pd.DataFrame, df_month: pd.DataFrame, intent: dict, col_info: dict, month: str) -> dict:
    """异常检测（TPO=0 或 YOY<阈值）"""
    tpo_cols = col_info.get("tpo_cols", {})
    yoy_cols = col_info.get("yoy_cols", {})

    results = []
    # 检测 TPO=0
    for col in tpo_cols:
        val = df_month[col].values[0] if len(df_month) > 0 else None
        if pd.notna(val) and val == 0:
            results.append({"场景": col, "异常类型": "TPO=0", "值": val})

    # 检测 YOY<阈值（如 -30%）
    threshold = -0.3
    for col in yoy_cols:
        val = df_month[col].values[0] if len(df_month) > 0 else None
        if pd.notna(val):
            if isinstance(val, str):
                try:
                    val = float(val.strip("%")) / 100
                except:
                    val = None
            if val is not None and val < threshold:
                results.append({"场景": col, "异常类型": "YOY<-30%", "值": val})

    df_result = pd.DataFrame(results)

    return {
        "conclusion": f"{month} 异常数据（TPO=0 或 YOY<-30%）",
        "data": df_result,
        "type": "table",
        "month": month,
    }


def format_result(result: dict, question: str) -> str:
    """将查询结果格式化为 Markdown 输出（遵循严格规范）"""
    lines = []
    lines.append(f"\n## 📊 查询结果\n")
    lines.append(f"**问题**：{question}\n")
    lines.append(f"**月份**：{result.get('month', 'N/A')}\n")
    lines.append(f"**结论**：{result['conclusion']}\n")

    data = result.get("data")
    if data is None:
        lines.append("⚠️ 未找到相关数据。")
    elif isinstance(data, dict) and "TPO" in data:
        # multi_table 类型
        lines.append("**详细数据**：\n")
        lines.append("### TPO 数据")
        lines.append(data["TPO"].to_markdown(index=False, floatfmt=".2f"))
        lines.append("\n### YOY 数据")
        lines.append(data["YOY"].to_markdown(index=False, floatfmt=".2f"))
    elif isinstance(data, pd.DataFrame) and not data.empty:
        lines.append("**详细数据**：\n")
        lines.append(data.to_markdown(index=False, floatfmt=".2f"))
    elif isinstance(data, pd.DataFrame) and data.empty:
        lines.append("未找到匹配数据。")
    elif isinstance(data, str):
        lines.append(f"**结果**：{data}")
    else:
        lines.append(f"**结果**：{data}")

    # 趋势对比（仅当有 MoM 数据时）
    if result.get("type") == "table" and "环比" in result.get("conclusion", ""):
        lines.append("\n**趋势对比**：")
        lines.append("- 环比（vs上月）：见上方表格")
        lines.append("- 同比（vs上年同月）：请使用「同比」查询")

    return "\n".join(lines)


def answer_question(file_path: str, question: str, memory_dir: str = None) -> dict:
    """
    主问答函数：
    1. 解析文件（带缓存）
    2. 检索历史记忆
    3. 执行查询
    4. 存储新问答
    返回 {"answer": str, "from_memory": bool, "intent": dict}
    """
    memory_dir = memory_dir or os.path.join(os.path.dirname(__file__), "..", "memory")

    # ── Step 1: 解析文件 ──────────────────────────────────
    summary = parse_file(file_path, memory_dir)
    file_hash = summary.get("file_hash")

    # ── Step 2: 检索历史记忆 ──────────────────────────────
    similar = search_qa(question, memory_dir, file_hash, limit=3)
    if similar and similar[0]["score"] >= 3:
        cached = similar[0]
        answer = cached["answer"]
        answer += f"\n\n> 📦 *此答案来自记忆缓存（问题相似度: {cached['score']}，历史使用: {cached['used_count']} 次）*"
        save_qa_pair(question, answer, memory_dir, file_hash)
        return {
            "answer": answer,
            "from_memory": True,
            "intent": {"intent": "cached"},
            "file_hash": file_hash,
        }

    # ── Step 3: 加载数据并执行查询 ────────────────────────
    df = load_file(file_path)
    intent = detect_intent(question)
    col_info = find_relevant_columns(df, intent, summary)
    result = execute_intent(df, intent, col_info, question)
    answer = format_result(result, question)

    # ── Step 4: 存储新问答 ────────────────────────────────
    save_qa_pair(question, answer, memory_dir, file_hash)

    return {
        "answer": answer,
        "from_memory": False,
        "intent": intent,
        "file_hash": file_hash,
        "result_type": result.get("type"),
    }


def main():
    parser = argparse.ArgumentParser(description="数据问答引擎 v2.0")
    parser.add_argument("--file", required=True, help="xlsx 或 csv 文件路径")
    parser.add_argument("--question", required=True, help="自然语言问题")
    parser.add_argument("--memory-dir", default=None, help="记忆存储目录")
    parser.add_argument("--output-json", action="store_true", help="以 JSON 格式输出结果")
    args = parser.parse_args()

    result = answer_question(args.file, args.question, args.memory_dir)

    if args.output_json:
        out = {k: v for k, v in result.items() if k != "data"}
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(result["answer"])
        if result.get("from_memory"):
            print("\n[来自记忆缓存]", file=sys.stderr)
        else:
            print(f"\n[意图: {result['intent']['intent']}]", file=sys.stderr)


if __name__ == "__main__":
    main()
