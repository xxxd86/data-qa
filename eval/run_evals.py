"""
评估运行脚本 - 基于 evals.json 执行测试用例
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
import json
from pathlib import Path

evals_path = Path(__file__).parent / 'evals.json'
with open(evals_path, encoding='utf-8') as f:
    evals = json.load(f)

print(f"📊 评估执行 - {len(evals['evaluations'])} 个测试用例\n")
print("=" * 60)

results = []
for i, ev in enumerate(evals['evaluations'], 1):
    print(f"\n[{i}/{len(evals['evaluations'])}] {ev['id']}: {ev['description']}")
    print(f"Prompt: {ev['prompt'][:80]}...")
    
    # TODO: 实际调用 data-qa skill 执行查询
    # 这里暂时标记为手动验证
    print(f"状态: ⏳ 需要手动验证")
    results.append({
        'id': ev['id'],
        'status': 'pending',
        'prompt': ev['prompt'],
        'assertions': len(ev['assertions'])
    })

print(f"\n{'='*60}")
print(f"✅ 完成测试用例准备：{len(results)} 个")
print(f"⚠️  注意：需要手动调用 data-qa skill 验证每个用例")
