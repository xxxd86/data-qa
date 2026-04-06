#!/usr/bin/env python3
"""
memory_manager.py - 记忆管理模块

功能：
- SQLite：存储问答历史（Q&A 对），支持相似度检索
- JSON：存储数据集记忆（字段摘要、文件哈希）
- 提供清理、搜索、列举等管理接口

用法：
    python memory_manager.py --list-datasets
    python memory_manager.py --list-qa [--limit N]
    python memory_manager.py --search-qa "<关键词>"
    python memory_manager.py --clear-dataset <file_hash>
    python memory_manager.py --clear-qa-all
    python memory_manager.py --stats
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime


# ── 默认路径 ──────────────────────────────────────────────────────────────────
DEFAULT_MEMORY_DIR = os.path.join(os.path.dirname(__file__), "..", "memory")
DB_NAME = "qa_memory.db"
DATASETS_NAME = "datasets.json"


def get_db_path(memory_dir: str) -> str:
    return os.path.join(memory_dir, DB_NAME)


def get_datasets_path(memory_dir: str) -> str:
    return os.path.join(memory_dir, DATASETS_NAME)


# ── SQLite 初始化 ─────────────────────────────────────────────────────────────
def init_db(memory_dir: str) -> sqlite3.Connection:
    """初始化 SQLite 数据库，返回连接"""
    os.makedirs(memory_dir, exist_ok=True)
    db_path = get_db_path(memory_dir)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS qa_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            question_normalized TEXT,
            answer TEXT NOT NULL,
            data_snapshot TEXT,
            file_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_question
        ON qa_history(question_normalized)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_file_hash
        ON qa_history(file_hash)
    """)
    conn.commit()
    return conn


def normalize_question(question: str) -> str:
    """标准化问题字符串，用于模糊匹配"""
    import re
    q = question.strip().lower()
    # 去除标点
    q = re.sub(r"[^\w\s]", "", q, flags=re.UNICODE)
    # 去除多余空格
    q = re.sub(r"\s+", " ", q)
    return q


# ── Q&A 记忆操作 ──────────────────────────────────────────────────────────────
def save_qa_pair(question: str, answer: str, memory_dir: str = None,
                 file_hash: str = None, data_snapshot: str = None) -> int:
    """
    存储问答对到 SQLite。
    返回插入的 id。
    """
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    conn = init_db(memory_dir)
    q_norm = normalize_question(question)

    # 检查是否已存在完全相同的问题（避免重复存储）
    existing = conn.execute(
        "SELECT id FROM qa_history WHERE question_normalized = ?",
        (q_norm,)
    ).fetchone()

    if existing:
        # 更新已有记录
        conn.execute(
            "UPDATE qa_history SET answer = ?, used_count = used_count + 1, created_at = ? WHERE id = ?",
            (answer, datetime.now().isoformat(), existing[0])
        )
        conn.commit()
        conn.close()
        return existing[0]

    cursor = conn.execute(
        """INSERT INTO qa_history
           (question, question_normalized, answer, data_snapshot, file_hash, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (question, q_norm, answer, data_snapshot, file_hash, datetime.now().isoformat())
    )
    row_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return row_id


def search_qa(question: str, memory_dir: str = None, file_hash: str = None,
              limit: int = 5) -> list:
    """
    检索相似历史问答。
    策略：关键词包含匹配（分词后逐词搜索），返回最相关的 N 条。
    返回 [{"question": ..., "answer": ..., "score": ...}, ...]
    """
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    db_path = get_db_path(memory_dir)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    q_norm = normalize_question(question)
    # 提取关键词（空格分割后长度>=1的词，同时对中文做字符级切分）
    raw_words = q_norm.split()
    keywords = []
    for w in raw_words:
        if len(w) >= 2:
            keywords.append(w)
        elif len(w) == 1 and w.strip():
            keywords.append(w)
    # 额外：对整个问题做3-gram子串提取，提高中文匹配率
    if len(q_norm) >= 3:
        for i in range(len(q_norm) - 1):
            substr = q_norm[i:i+2]
            if substr not in keywords and substr.strip():
                keywords.append(substr)
    keywords = list(dict.fromkeys(keywords))[:15]  # 去重，最多15个关键词

    if not keywords:
        conn.close()
        return []

    # 构建 LIKE 条件
    conditions = " OR ".join(["question_normalized LIKE ?" for _ in keywords])
    params = [f"%{kw}%" for kw in keywords]

    if file_hash:
        query = f"""
            SELECT *, (
                {' + '.join(['CASE WHEN question_normalized LIKE ? THEN 1 ELSE 0 END' for _ in keywords])}
            ) as score
            FROM qa_history
            WHERE ({conditions}) AND (file_hash = ? OR file_hash IS NULL)
            ORDER BY score DESC, used_count DESC, created_at DESC
            LIMIT ?
        """
        rows = conn.execute(query, params * 2 + [file_hash, limit]).fetchall()
    else:
        query = f"""
            SELECT *, (
                {' + '.join(['CASE WHEN question_normalized LIKE ? THEN 1 ELSE 0 END' for _ in keywords])}
            ) as score
            FROM qa_history
            WHERE {conditions}
            ORDER BY score DESC, used_count DESC, created_at DESC
            LIMIT ?
        """
        rows = conn.execute(query, params * 2 + [limit]).fetchall()

    results = []
    for row in rows:
        if row["score"] > 0:
            results.append({
                "id": row["id"],
                "question": row["question"],
                "answer": row["answer"],
                "score": row["score"],
                "used_count": row["used_count"],
                "created_at": row["created_at"],
            })

    conn.close()
    return results


def list_qa(memory_dir: str = None, limit: int = 20) -> list:
    """列举最近的问答记录"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    db_path = get_db_path(memory_dir)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, question, answer, used_count, created_at FROM qa_history ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def clear_qa_all(memory_dir: str = None):
    """清除所有问答记忆"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    db_path = get_db_path(memory_dir)
    if not os.path.exists(db_path):
        print("问答记忆库为空，无需清除。")
        return
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM qa_history").fetchone()[0]
    conn.execute("DELETE FROM qa_history")
    conn.commit()
    conn.close()
    print(f"已清除 {count} 条问答记忆。")


def get_stats(memory_dir: str = None) -> dict:
    """获取记忆统计信息"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    stats = {"memory_dir": memory_dir}

    # SQLite 统计
    db_path = get_db_path(memory_dir)
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        stats["qa_count"] = conn.execute("SELECT COUNT(*) FROM qa_history").fetchone()[0]
        stats["qa_total_used"] = conn.execute("SELECT SUM(used_count) FROM qa_history").fetchone()[0] or 0
        stats["db_size_kb"] = round(os.path.getsize(db_path) / 1024, 1)
        conn.close()
    else:
        stats["qa_count"] = 0

    # JSON 统计
    datasets_path = get_datasets_path(memory_dir)
    if os.path.exists(datasets_path):
        with open(datasets_path, "r", encoding="utf-8") as f:
            datasets = json.load(f)
        stats["dataset_count"] = len(datasets)
        stats["datasets"] = [
            {"hash": k[:8] + "...", "file_name": v.get("file_name"), "created_at": v.get("created_at")}
            for k, v in datasets.items()
        ]
    else:
        stats["dataset_count"] = 0

    return stats


# ── 数据集记忆操作 ────────────────────────────────────────────────────────────
def save_data_memory(file_hash: str, summary: dict, memory_dir: str = None):
    """存储数据集记忆到 JSON"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    os.makedirs(memory_dir, exist_ok=True)
    datasets_path = get_datasets_path(memory_dir)

    if os.path.exists(datasets_path):
        with open(datasets_path, "r", encoding="utf-8") as f:
            datasets = json.load(f)
    else:
        datasets = {}

    datasets[file_hash] = summary
    with open(datasets_path, "w", encoding="utf-8") as f:
        json.dump(datasets, f, ensure_ascii=False, indent=2)


def load_data_memory(file_hash: str, memory_dir: str = None) -> dict:
    """从 JSON 加载数据集记忆"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    datasets_path = get_datasets_path(memory_dir)
    if not os.path.exists(datasets_path):
        return None
    with open(datasets_path, "r", encoding="utf-8") as f:
        datasets = json.load(f)
    return datasets.get(file_hash)


def clear_dataset(file_hash: str, memory_dir: str = None):
    """清除指定数据集记忆"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    datasets_path = get_datasets_path(memory_dir)
    if not os.path.exists(datasets_path):
        print("数据集记忆为空。")
        return
    with open(datasets_path, "r", encoding="utf-8") as f:
        datasets = json.load(f)
    if file_hash in datasets:
        del datasets[file_hash]
        with open(datasets_path, "w", encoding="utf-8") as f:
            json.dump(datasets, f, ensure_ascii=False, indent=2)
        print(f"已清除数据集记忆: {file_hash[:8]}...")
    else:
        print(f"未找到记忆: {file_hash}")


def list_datasets(memory_dir: str = None) -> list:
    """列举所有数据集记忆"""
    memory_dir = memory_dir or DEFAULT_MEMORY_DIR
    datasets_path = get_datasets_path(memory_dir)
    if not os.path.exists(datasets_path):
        return []
    with open(datasets_path, "r", encoding="utf-8") as f:
        datasets = json.load(f)
    return [
        {
            "hash": k,
            "file_name": v.get("file_name"),
            "rows": v.get("shape", {}).get("rows"),
            "cols": v.get("shape", {}).get("cols"),
            "created_at": v.get("created_at"),
        }
        for k, v in datasets.items()
    ]


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="数据问答记忆管理工具")
    parser.add_argument("--memory-dir", default=DEFAULT_MEMORY_DIR, help="记忆存储目录")
    parser.add_argument("--list-datasets", action="store_true", help="列举所有数据集记忆")
    parser.add_argument("--list-qa", action="store_true", help="列举历史问答")
    parser.add_argument("--limit", type=int, default=20, help="列举数量限制")
    parser.add_argument("--search-qa", metavar="KEYWORD", help="搜索历史问答")
    parser.add_argument("--clear-dataset", metavar="HASH", help="清除指定数据集记忆")
    parser.add_argument("--clear-qa-all", action="store_true", help="清除所有问答记忆")
    parser.add_argument("--stats", action="store_true", help="显示记忆统计信息")
    args = parser.parse_args()

    if args.list_datasets:
        datasets = list_datasets(args.memory_dir)
        if not datasets:
            print("暂无数据集记忆。")
        else:
            print(f"共 {len(datasets)} 个数据集记忆:")
            for d in datasets:
                print(f"  [{d['hash'][:8]}...] {d['file_name']}  {d['rows']}行×{d['cols']}列  {d['created_at']}")

    elif args.list_qa:
        records = list_qa(args.memory_dir, args.limit)
        if not records:
            print("暂无问答记忆。")
        else:
            print(f"最近 {len(records)} 条问答记忆:")
            for r in records:
                answer_preview = r["answer"][:80].replace("\n", " ")
                print(f"  [{r['id']}] Q: {r['question'][:50]}")
                print(f"       A: {answer_preview}...")
                print(f"       使用次数: {r['used_count']}  时间: {r['created_at']}")

    elif args.search_qa:
        results = search_qa(args.search_qa, args.memory_dir)
        if not results:
            print("未找到相关历史问答。")
        else:
            print(f"找到 {len(results)} 条相关记录:")
            for r in results:
                print(f"  [相关度:{r['score']}] Q: {r['question']}")
                print(f"   A: {r['answer'][:100]}...")

    elif args.clear_dataset:
        clear_dataset(args.clear_dataset, args.memory_dir)

    elif args.clear_qa_all:
        clear_qa_all(args.memory_dir)

    elif args.stats:
        stats = get_stats(args.memory_dir)
        print(f"记忆统计:")
        print(f"  存储目录: {stats['memory_dir']}")
        print(f"  数据集记忆: {stats['dataset_count']} 个")
        print(f"  问答记忆: {stats.get('qa_count', 0)} 条")
        print(f"  历史复用次数: {stats.get('qa_total_used', 0)} 次")
        if stats.get("db_size_kb"):
            print(f"  数据库大小: {stats['db_size_kb']} KB")
        if stats.get("datasets"):
            print(f"\n  数据集列表:")
            for d in stats["datasets"]:
                print(f"    [{d['hash']}] {d['file_name']}  {d['created_at']}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
