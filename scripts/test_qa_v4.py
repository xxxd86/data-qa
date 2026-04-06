#!/usr/bin/env python3
"""
test_qa_v4.py - 测试问答引擎 v4.0 的完整能力
"""

import subprocess
import sys


def run_test(question: str, description: str):
    """运行单个测试"""
    print(f"\n{'='*80}")
    print(f"测试：{description}")
    print(f"{'='*80}")
    print(f"问题：{question}")
    print(f"{'-'*80}\n")

    cmd = [
        sys.executable,
        "scripts/qa_engine_v4.py",
        "--file", "D:/OneProject/OpenAgent/test.csv",
        "--question", question
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", cwd="C:/Users/28767/.workbuddy/skills/data-qa")

    if result.stdout:
        # 过滤掉 PowerShell 的 CLIXML 输出
        output = result.stdout
        if "#< CLIXML" in output:
            # 提取真实输出
            lines = output.split('\n')
            output_lines = []
            skip_clixml = False
            for line in lines:
                if "#< CLIXML" in line:
                    skip_clixml = True
                    continue
                if skip_clixml:
                    if line.strip() == "":
                        skip_clixml = False
                        continue
                    if "<Objs" in line or "</Objs>" in line:
                        continue
                    continue
                output_lines.append(line)
            output = '\n'.join(output_lines).strip()

        print(output)
    else:
        print(f"错误：{result.stderr}")

    print(f"\n{'='*80}\n")


def main():
    """运行所有测试"""
    print("\n" + "="*80)
    print("数据问答引擎 v4.0 - 完整能力测试")
    print("="*80)

    tests = [
        ("最新月的数据", "基础查询 - 最新月份数据"),
        ("2026/3 的费用疑义场景数据", "场景查询 - 特定场景详情"),
        ("各场景的 TPO", "场景查询 - 所有场景概览"),
        ("最新月的趋势", "趋势查询 - 同比和环比"),
        ("排名前3的场景", "排名查询 - TPO 排名"),
        ("检测异常", "异常检测 - 检测数据异常"),
        ("对比最近两个月", "对比查询 - 月度对比"),
        ("营销活动的占比", "占比查询 - 特定场景占比"),
        ("2026/2 的数据", "指定月份 - 历史数据查询"),
    ]

    for question, description in tests:
        run_test(question, description)

    print("\n" + "="*80)
    print("测试完成！")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
