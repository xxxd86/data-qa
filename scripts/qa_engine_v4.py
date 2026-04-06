#!/usr/bin/env python3
"""
qa_engine_v4.py - 智能问答引擎（v4.0，增强版）

核心能力：
1. 深度理解 CSV 数据结构
2. 多维度分析能力（环比、同比、趋势、对比）
3. 场景排名、异常检测、数据验证
4. 智能意图识别和答案验证
5. 支持自然语言问答

用法：
    python qa_engine_v4.py --file <csv_path> --question "<问题>"
"""

import argparse
import csv
import re
import sys
import io
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

# 修复 Windows PowerShell 编码问题
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# 业务语义常量
CHANNEL_MAP = {
    "费用疑义tpo": "费用疑义",
    "营销活动tpo": "营销活动",
    "售后服务": "售后服务",
    "资产权益": "资产权益",
    "二次号": "二次号",
}


class QAEValidator:
    """答案验证器：确保答案的准确性和完整性"""

    @staticmethod
    def validate_number(value: Any, field_name: str) -> Tuple[bool, str, Optional[float]]:
        """验证数值数据"""
        try:
            num = float(value) if value is not None and value != '' else None
            if num is not None:
                return True, "", num
            return False, f"{field_name} 无数据", None
        except:
            return False, f"{field_name} 格式错误", None

    @staticmethod
    def validate_percentage(value: Any, field_name: str) -> Tuple[bool, str, Optional[float]]:
        """验证百分比数据"""
        if value is None or value == '':
            return False, f"{field_name} 无数据", None

        try:
            if isinstance(value, str):
                val_str = value.strip()
                if '%' in val_str:
                    num = float(val_str.replace('%', '').strip())
                else:
                    num = float(val_str)
                    # 如果数字大于1，假设是百分比形式（如 30 表示 30%）
                    if num > 1:
                        num = num / 100
                return True, "", num
            else:
                return True, "", float(value)
        except:
            return False, f"{field_name} 格式错误", None

    @staticmethod
    def validate_answer(answer: Dict) -> List[str]:
        """验证完整答案，返回错误列表"""
        errors = []

        if not answer.get("month"):
            errors.append("缺少月份信息")

        if not answer.get("conclusion"):
            errors.append("缺少结论")

        # 验证数值是否合理
        if "total_tpo" in answer:
            valid, msg, _ = QAEValidator.validate_number(answer["total_tpo"], "总TPO")
            if not valid:
                errors.append(msg)

        if "yoy" in answer and answer["yoy"] is not None:
            valid, msg, _ = QAEValidator.validate_percentage(answer["yoy"], "年度YoY")
            if not valid:
                errors.append(msg)

        return errors

    @staticmethod
    def cross_check(data1: Dict, data2: Dict, field: str, tolerance: float = 0.01) -> bool:
        """交叉验证两个字段是否一致"""
        val1 = data1.get(field)
        val2 = data2.get(field)

        if val1 is None or val2 is None:
            return True  # 至少一个为空时跳过验证

        try:
            diff = abs(float(val1) - float(val2))
            return diff <= tolerance
        except:
            return False


class DataAnalyzer:
    """数据分析器：提供深度分析能力"""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.csv_data = self.load_csv()
        self.headers = self.csv_data["headers"]
        self.data = self.csv_data["data"]
        self.validator = QAEValidator()

    def load_csv(self) -> Dict:
        """读取 CSV 文件"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            return {"headers": [], "data": []}

        headers = rows[0]
        data = []

        for row in rows[1:]:
            if len(row) < 3:
                continue
            cleaned_row = [col.strip() for col in row]
            data.append(cleaned_row)

        return {"headers": headers, "data": data}

    def get_months(self) -> List[str]:
        """获取所有月份"""
        return [row[0].strip() for row in self.data]

    def get_month_data(self, month: str) -> Optional[Dict]:
        """获取指定月份数据"""
        for row in self.data:
            if month in row[0]:
                return {
                    "row": row,
                    "headers": self.headers,
                    "month": month
                }
        return None

    def get_all_months_data(self) -> List[Dict]:
        """获取所有月份数据"""
        return [self.get_month_data(month) for month in self.get_months()]

    def calculate_mom(self, current_val: float, prev_val: float) -> Optional[float]:
        """计算环比"""
        if prev_val is None or prev_val == 0:
            return None
        return (current_val - prev_val) / prev_val

    def get_scenario_mom(self, current_scenario: str, current_tpo: float, prev_month: str) -> Optional[float]:
        """计算场景的环比"""
        prev_data = self.get_month_data(prev_month)
        if not prev_data:
            return None

        prev_scenarios = self.get_scenarios(prev_data)
        prev_scenario = next((s for s in prev_scenarios if s["name"] == current_scenario), None)

        if prev_scenario and prev_scenario.get("tpo") and prev_scenario["tpo"] != 0:
            return (current_tpo - prev_scenario["tpo"]) / prev_scenario["tpo"]

        return None

    def get_scenarios(self, month_data: Dict) -> List[Dict]:
        """提取场景数据"""
        row = month_data["row"]

        scenarios = []

        # 固定列索引
        idx_fee_tpo = 4
        idx_fee_yoy = 5
        idx_fee_ratio = 6
        idx_promo_tpo = 7
        idx_promo_yoy = 8
        idx_promo_ratio = 9
        idx_second_tpo = 10
        idx_second_yoy = 11
        idx_second_ratio = 12

        def get_col_val(idx, default=None):
            if idx is None or idx >= len(row):
                return default
            val = row[idx]
            return val if val and val != '' else default

        def parse_ratio(ratio_raw):
            """解析占比"""
            if not ratio_raw or ratio_raw == '':
                return None
            try:
                ratio_str = str(ratio_raw).strip()
                if '%' in ratio_str:
                    return float(ratio_str.replace('%', '').strip()) / 100
                else:
                    val = float(ratio_str)
                    return val / 100 if val > 1 else val
            except:
                return None

        def parse_yoy(yoy_raw):
            """解析 YoY"""
            if not yoy_raw or yoy_raw == '':
                return None
            try:
                yoy_str = str(yoy_raw).strip()
                if '%' in yoy_str:
                    return float(yoy_str.replace('%', '').strip()) / 100
                return float(yoy_str)
            except:
                return None

        # 费用疑义
        fee_tpo = get_col_val(idx_fee_tpo)
        if fee_tpo is not None and fee_tpo != '':
            try:
                scenarios.append({
                    "name": "费用疑义",
                    "tpo": float(fee_tpo),
                    "yoy": parse_yoy(get_col_val(idx_fee_yoy)),
                    "ratio": parse_ratio(get_col_val(idx_fee_ratio))
                })
            except:
                pass

        # 营销活动
        promo_tpo = get_col_val(idx_promo_tpo)
        if promo_tpo is not None and promo_tpo != '':
            try:
                scenarios.append({
                    "name": "营销活动",
                    "tpo": float(promo_tpo),
                    "yoy": parse_yoy(get_col_val(idx_promo_yoy)),
                    "ratio": parse_ratio(get_col_val(idx_promo_ratio))
                })
            except:
                pass

        # 二次号
        second_tpo = get_col_val(idx_second_tpo)
        if second_tpo is not None and second_tpo != '':
            try:
                scenarios.append({
                    "name": "二次号",
                    "tpo": float(second_tpo),
                    "yoy": parse_yoy(get_col_val(idx_second_yoy)),
                    "ratio": parse_ratio(get_col_val(idx_second_ratio))
                })
            except:
                pass

        # 售后服务和资产权益（可能在列10-15）
        for idx in range(10, 15):
            val = get_col_val(idx)
            if val and ("售后服务" in val or "资产权益" in val):
                match = re.match(r'^(.*?)(\d+\.?\d*)$', str(val))
                if match:
                    name = match.group(1).strip()
                    tpo_val = match.group(2)

                    # 向后查找占比
                    ratio = None
                    for ratio_idx in range(idx + 1, len(row)):
                        ratio_raw = row[ratio_idx] if ratio_idx < len(row) else None
                        if ratio_raw and str(ratio_raw).strip():
                            ratio_str = str(ratio_raw).strip()
                            if '%' in ratio_str:
                                try:
                                    ratio = float(ratio_str.replace('%', '').strip()) / 100
                                except:
                                    pass
                                break

                    try:
                        scenarios.append({
                            "name": name,
                            "tpo": float(tpo_val),
                            "yoy": None,
                            "ratio": ratio
                        })
                    except:
                        pass

        return scenarios

    def analyze_month_full(self, month: str) -> Dict:
        """完整分析单月数据"""
        month_data = self.get_month_data(month)
        if not month_data:
            return {"error": f"未找到 {month} 的数据"}

        row = month_data["row"]
        months = self.get_months()

        def get_col_val(idx, default=None):
            if idx is None or idx >= len(row):
                return default
            val = row[idx]
            return val if val and val != '' else default

        # 基础数据
        idx_month = 0
        idx_order_volume = 1
        idx_total_tpo = 2
        idx_total_yoy = 3

        order_volume = get_col_val(idx_order_volume)
        total_tpo = get_col_val(idx_total_tpo)
        yoy_raw = get_col_val(idx_total_yoy)

        # 转换数值
        try:
            order_volume = int(order_volume) if order_volume else None
        except:
            order_volume = None

        try:
            total_tpo = float(total_tpo) if total_tpo else None
        except:
            total_tpo = None

        # 解析 YoY
        yoy = None
        try:
            if yoy_raw and yoy_raw != '':
                if '%' in str(yoy_raw):
                    yoy = float(str(yoy_raw).replace('%', '').strip()) / 100
                else:
                    yoy = float(yoy_raw)
        except:
            pass

        # 场景数据
        scenarios = self.get_scenarios(month_data)

        # 计算场景环比
        months = self.get_months()
        if month in months:
            idx = months.index(month)
            if idx > 0:
                prev_month = months[idx - 1]
                for scenario in scenarios:
                    scenario["mom"] = self.get_scenario_mom(scenario["name"], scenario["tpo"], prev_month)

        # 计算 MoM（总TPO）
        mom_change = None
        if month in months:
            idx = months.index(month)
            if idx > 0:
                prev_month = months[idx - 1]
                prev_data = self.get_month_data(prev_month)
                if prev_data:
                    prev_row = prev_data["row"]
                    prev_total_tpo_raw = prev_row[idx_total_tpo] if len(prev_row) > idx_total_tpo else None
                    try:
                        prev_total_tpo = float(prev_total_tpo_raw) if prev_total_tpo_raw else None
                        if prev_total_tpo and total_tpo and prev_total_tpo != 0:
                            mom_change = self.calculate_mom(total_tpo, prev_total_tpo)
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

    def compare_scenarios(self, month1: str, month2: str) -> Dict:
        """对比两个月份的场景数据"""
        data1 = self.analyze_month_full(month1)
        data2 = self.analyze_month_full(month2)

        if "error" in data1 or "error" in data2:
            return {"error": "月份数据不存在"}

        scenarios1 = {s["name"]: s for s in data1["scenarios"]}
        scenarios2 = {s["name"]: s for s in data2["scenarios"]}

        comparison = []

        for name in set(scenarios1.keys()) | set(scenarios2.keys()):
            s1 = scenarios1.get(name)
            s2 = scenarios2.get(name)

            if s1 and s2:
                change = s2["tpo"] - s1["tpo"]
                change_pct = (change / s1["tpo"] * 100) if s1["tpo"] != 0 else None
                comparison.append({
                    "name": name,
                    "tpo1": s1["tpo"],
                    "tpo2": s2["tpo"],
                    "change": change,
                    "change_pct": change_pct
                })

        return {
            "month1": month1,
            "month2": month2,
            "comparisons": comparison
        }

    def rank_scenarios(self, month: str, by: str = "tpo", top_n: int = None) -> List[Dict]:
        """场景排名"""
        analysis = self.analyze_month_full(month)
        if "error" in analysis:
            return []

        scenarios = analysis["scenarios"]

        if by == "tpo":
            scenarios.sort(key=lambda x: x["tpo"], reverse=True)
        elif by == "yoy":
            scenarios.sort(key=lambda x: x["yoy"] or 0, reverse=True)
        elif by == "ratio":
            scenarios.sort(key=lambda x: x["ratio"] or 0, reverse=True)

        if top_n:
            scenarios = scenarios[:top_n]

        return scenarios

    def detect_anomalies(self, month: str, threshold: float = 0.3) -> List[Dict]:
        """检测异常数据（YoY 或 MoM 变化超过阈值）"""
        analysis = self.analyze_month_full(month)
        if "error" in analysis:
            return []

        anomalies = []

        # 检查总 TPO 的 YoY
        if analysis.get("yoy"):
            if abs(analysis["yoy"]) > threshold:
                anomalies.append({
                    "type": "总TPO",
                    "metric": "YoY",
                    "value": analysis["yoy"],
                    "threshold": threshold
                })

        # 检查总 TPO 的 MoM
        if analysis.get("mom_change"):
            if abs(analysis["mom_change"]) > threshold:
                anomalies.append({
                    "type": "总TPO",
                    "metric": "MoM",
                    "value": analysis["mom_change"],
                    "threshold": threshold
                })

        # 检查场景 YoY
        for scenario in analysis["scenarios"]:
            if scenario.get("yoy"):
                if abs(scenario["yoy"]) > threshold:
                    anomalies.append({
                        "type": scenario["name"],
                        "metric": "YoY",
                        "value": scenario["yoy"],
                        "threshold": threshold
                    })

        return anomalies


class IntentRecognizer:
    """意图识别器：理解用户问题的意图"""

    INTENT_PATTERNS = {
        "query_month": [
            r"最新月|本月|当前月|当月",
            r"(\d{4}/\d{1,2})\s*(?:月|的|数据)",
        ],
        "query_scenario": [
            r"(?:场景|渠道).*?(?:TPO|tpo)",
            r"(\w+)(?:场景|渠道)(?:.*?TPO|tpo)",
        ],
        "query_trend": [
            r"(?:趋势|变化|增长|下降|波动)",
            r"(?:同比|环比|YoY|MoM)",
        ],
        "query_rank": [
            r"(?:排名|最高|最低|最大|最小)",
            r"Top\s*\d+|前\s*\d+",
        ],
        "query_anomaly": [
            r"(?:异常|波动|突增|突降)",
            r"(?:明显|剧烈|显著).*(?:变化|波动)",
        ],
        "query_compare": [
            r"(?:对比|比较|差异)",
            r"(?:vs|VS|和|与).*?(?:相比|对比)",
        ],
    }

    @staticmethod
    def recognize(question: str) -> Dict:
        """识别用户问题意图"""
        intent = {
            "primary": None,
            "secondary": [],
            "entities": {},
            "question": question
        }

        question_lower = question.lower()

        # 识别主要意图
        for intent_type, patterns in IntentRecognizer.INTENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, question_lower):
                    if intent["primary"] is None:
                        intent["primary"] = intent_type
                    else:
                        intent["secondary"].append(intent_type)
                    break

        # 提取月份
        month_match = re.search(r'(\d{4}/\d{1,2})', question)
        if month_match:
            intent["entities"]["month"] = month_match.group(1)

        # 提取场景名称
        for name in CHANNEL_MAP.values():
            if name in question:
                intent["entities"]["scenario"] = name
                break

        # 如果没有匹配到意图，默认为查询
        if intent["primary"] is None:
            intent["primary"] = "query_month"

        return intent


class QAEEngine:
    """问答引擎：生成答案并验证"""

    def __init__(self, file_path: str):
        self.analyzer = DataAnalyzer(file_path)
        self.validator = QAEValidator()

    def answer(self, question: str) -> Dict:
        """回答用户问题"""
        # 识别意图
        intent = IntentRecognizer.recognize(question)

        # 确定月份
        month = intent["entities"].get("month")
        if not month:
            month = self.analyzer.get_months()[-1]  # 默认最新月

        # 根据意图生成答案
        answer = self._generate_answer(intent, month)

        # 验证答案
        errors = self.validator.validate_answer(answer)
        if errors:
            answer["validation_errors"] = errors
            answer["status"] = "warning"
        else:
            answer["status"] = "success"

        return answer

    def _generate_answer(self, intent: Dict, month: str) -> Dict:
        """生成答案"""
        primary_intent = intent["primary"]

        if primary_intent == "query_month":
            return self._answer_month_query(month)
        elif primary_intent == "query_scenario":
            return self._answer_scenario_query(month, intent["entities"].get("scenario"))
        elif primary_intent == "query_trend":
            return self._answer_trend_query(month)
        elif primary_intent == "query_rank":
            return self._answer_rank_query(month)
        elif primary_intent == "query_anomaly":
            return self._answer_anomaly_query(month)
        elif primary_intent == "query_compare":
            return self._answer_compare_query(intent["entities"])
        else:
            return self._answer_month_query(month)

    def _answer_month_query(self, month: str) -> Dict:
        """回答月份查询"""
        analysis = self.analyzer.analyze_month_full(month)

        if "error" in analysis:
            return {
                "question": f"{month} 的数据",
                "error": analysis["error"],
                "status": "error"
            }

        # 生成结论
        conclusion_parts = [f"{month} 总TPO为 {analysis['total_tpo']:.2f}"]

        if analysis.get("yoy") is not None:
            yoy_pct = analysis["yoy"] * 100
            yoy_text = "增长" if analysis["yoy"] >= 0 else "下降"
            conclusion_parts.append(f"{yoy_text} {abs(yoy_pct):.1f}%（同比）")

        if analysis.get("mom_change") is not None:
            mom_pct = analysis["mom_change"] * 100
            mom_text = "增长" if analysis["mom_change"] >= 0 else "下降"
            conclusion_parts.append(f"{mom_text} {abs(mom_pct):.1f}%（环比）")

        conclusion = "，".join(conclusion_parts) + "。"

        return {
            "question": f"{month} 的数据",
            "month": month,
            "conclusion": conclusion,
            "data": analysis,
            "status": "success"
        }

    def _answer_scenario_query(self, month: str, scenario: str = None) -> Dict:
        """回答场景查询"""
        analysis = self.analyzer.analyze_month_full(month)

        if "error" in analysis:
            return {
                "question": f"{month} 的场景数据",
                "error": analysis["error"],
                "status": "error"
            }

        scenarios = analysis["scenarios"]

        if scenario:
            # 查询特定场景
            scenario_data = next((s for s in scenarios if s["name"] == scenario), None)
            if scenario_data:
                conclusion = f"{month} {scenario} 场景 TPO 为 {scenario_data['tpo']:.2f}"
                if scenario_data.get("yoy") is not None:
                    yoy_pct = scenario_data["yoy"] * 100
                    yoy_text = "增长" if scenario_data["yoy"] >= 0 else "下降"
                    conclusion += f"，同比 {yoy_text} {abs(yoy_pct):.1f}%"
                if scenario_data.get("ratio") is not None:
                    ratio_pct = scenario_data["ratio"] * 100
                    conclusion += f"，占比 {ratio_pct:.1f}%"
                conclusion += "。"

                return {
                    "question": f"{month} {scenario} 的数据",
                    "month": month,
                    "conclusion": conclusion,
                    "data": {"scenario": scenario_data},
                    "status": "success"
                }
            else:
                return {
                    "question": f"{month} {scenario} 的数据",
                    "error": f"未找到 {scenario} 场景数据",
                    "status": "error"
                }
        else:
            # 查询所有场景
            scenario_names = [s["name"] for s in scenarios]
            conclusion = f"{month} 共有 {len(scenarios)} 个场景：{', '.join(scenario_names)}。"

            return {
                "question": f"{month} 的场景数据",
                "month": month,
                "conclusion": conclusion,
                "data": {"scenarios": scenarios},
                "status": "success"
            }

    def _answer_trend_query(self, month: str) -> Dict:
        """回答趋势查询"""
        analysis = self.analyzer.analyze_month_full(month)

        if "error" in analysis:
            return {
                "question": f"{month} 的趋势",
                "error": analysis["error"],
                "status": "error"
            }

        trend_parts = []

        if analysis.get("yoy") is not None:
            yoy_pct = analysis["yoy"] * 100
            yoy_text = "上升" if analysis["yoy"] >= 0 else "下降"
            trend_parts.append(f"同比 {yoy_text} {abs(yoy_pct):.1f}%")

        if analysis.get("mom_change") is not None:
            mom_pct = analysis["mom_change"] * 100
            mom_text = "上升" if analysis["mom_change"] >= 0 else "下降"
            trend_parts.append(f"环比 {mom_text} {abs(mom_pct):.1f}%")

        if trend_parts:
            conclusion = f"{month} 总TPO {'，'.join(trend_parts)}。"
        else:
            conclusion = f"{month} 趋势数据不足。"

        return {
            "question": f"{month} 的趋势",
            "month": month,
            "conclusion": conclusion,
            "data": analysis,
            "status": "success"
        }

    def _answer_rank_query(self, month: str, top_n: int = 3) -> Dict:
        """回答排名查询"""
        ranked = self.analyzer.rank_scenarios(month, by="tpo", top_n=top_n)

        if not ranked:
            return {
                "question": f"{month} TPO 排名",
                "error": "无场景数据",
                "status": "error"
            }

        top_scenarios = ranked[:top_n]
        rank_text = "、".join([f"{s['name']}({s['tpo']:.2f})" for s in top_scenarios])
        conclusion = f"{month} TPO 最高的场景为：{rank_text}。"

        return {
            "question": f"{month} TPO 排名",
            "month": month,
            "conclusion": conclusion,
            "data": {"ranked": ranked, "top_n": top_n},
            "status": "success"
        }

    def _answer_anomaly_query(self, month: str, threshold: float = 0.3) -> Dict:
        """回答异常查询"""
        anomalies = self.analyzer.detect_anomalies(month, threshold)

        if not anomalies:
            return {
                "question": f"{month} 异常检测",
                "month": month,
                "conclusion": f"{month} 未检测到明显异常（阈值 {threshold*100}%）。",
                "data": {"anomalies": [], "threshold": threshold},
                "status": "success"
            }

        anomaly_texts = []
        for a in anomalies:
            value_pct = a["value"] * 100
            direction = "上升" if a["value"] >= 0 else "下降"
            anomaly_texts.append(f"{a['type']} 的 {a['metric']} {direction} {abs(value_pct):.1f}%")

        conclusion = f"{month} 检测到 {len(anomalies)} 个异常：{'；'.join(anomaly_texts)}。"

        return {
            "question": f"{month} 异常检测",
            "month": month,
            "conclusion": conclusion,
            "data": {"anomalies": anomalies, "threshold": threshold},
            "status": "success"
        }

    def _answer_compare_query(self, entities: Dict) -> Dict:
        """回答对比查询"""
        months = self.analyzer.get_months()

        # 如果没有指定对比月份，对比相邻月
        if len(months) >= 2:
            month1, month2 = months[-2], months[-1]
        else:
            return {
                "question": "对比查询",
                "error": "数据不足，无法对比",
                "status": "error"
            }

        comparison = self.analyzer.compare_scenarios(month1, month2)

        if "error" in comparison:
            return {
                "question": f"{month1} vs {month2} 对比",
                "error": comparison["error"],
                "status": "error"
            }

        changes = []
        for c in comparison["comparisons"]:
            if c["change_pct"] is not None:
                direction = "上升" if c["change"] >= 0 else "下降"
                changes.append(f"{c['name']} {direction} {abs(c['change_pct']):.1f}%")

        if changes:
            conclusion = f"{month2} vs {month1}：{', '.join(changes)}。"
        else:
            conclusion = f"{month2} vs {month1} 无明显变化。"

        return {
            "question": f"{month1} vs {month2} 对比",
            "conclusion": conclusion,
            "data": comparison,
            "status": "success"
        }


def format_answer(answer: Dict) -> str:
    """格式化答案输出"""
    lines = []

    # 标题
    lines.append("## 查询结果")
    lines.append("")

    # 问题
    lines.append(f"**问题**：{answer.get('question', '未知问题')}")
    lines.append("")

    # 月份
    if "month" in answer:
        lines.append(f"**月份**：{answer['month']}")
        lines.append("")

    # 错误信息
    if "error" in answer:
        lines.append(f"[X] **错误**：{answer['error']}")
        lines.append("")
        return "\n".join(lines)

    # 结论
    if "conclusion" in answer:
        lines.append(f"**结论**：{answer['conclusion']}")
        lines.append("")

    # 详细数据
    if "data" in answer:
        data = answer["data"]
        lines.append("**详细数据**：")
        lines.append("")

        # 单月分析数据
        if "month" in data and "total_tpo" in data:
            lines.append(f"- 完单量：{data.get('order_volume', '-'):,.0f}")
            lines.append(f"- 总 TPO：{data.get('total_tpo', '-'):.2f}")

            if data.get("yoy") is not None:
                yoy_pct = data["yoy"] * 100
                yoy_text = "[UP]" if data["yoy"] >= 0 else "[DOWN]"
                lines.append(f"- 年度 YoY：{yoy_text} {yoy_pct:.1f}%")

            if data.get("mom_change") is not None:
                mom_pct = data["mom_change"] * 100
                mom_text = "[UP]" if data["mom_change"] >= 0 else "[DOWN]"
                lines.append(f"- 环比（vs上月）：{mom_text} {mom_pct:.1f}%")

            lines.append("")

        # 场景数据
        if "scenarios" in data and data["scenarios"]:
            lines.append(f"**场景 TPO（{len(data['scenarios'])} 个场景）**：")
            for s in data["scenarios"]:
                s_text = f"- {s['name']}：{s['tpo']:.2f}"
                if s.get("yoy") is not None:
                    yoy_pct = s["yoy"] * 100
                    yoy_text = "[UP]" if s["yoy"] >= 0 else "[DOWN]"
                    s_text += f" ({yoy_text} YoY: {yoy_pct:.1f}%)"
                if s.get("mom") is not None:
                    mom_pct = s["mom"] * 100
                    mom_text = "[UP]" if s["mom"] >= 0 else "[DOWN]"
                    s_text += f"，环比 {mom_text} {mom_pct:.1f}%"
                if s.get("ratio") is not None:
                    ratio_pct = s["ratio"] * 100
                    s_text += f"，占比 {ratio_pct:.1f}%"
                lines.append(s_text)
            lines.append("")

        # 场景详情
        if "scenario" in data:
            s = data["scenario"]
            lines.append(f"**{s['name']} 场景详情**：")
            lines.append(f"- TPO：{s['tpo']:.2f}")
            if s.get("yoy") is not None:
                yoy_pct = s["yoy"] * 100
                yoy_text = "[UP]" if s["yoy"] >= 0 else "[DOWN]"
                lines.append(f"- YoY：{yoy_text} {yoy_pct:.1f}%")
            if s.get("ratio") is not None:
                ratio_pct = s["ratio"] * 100
                lines.append(f"- 占比：{ratio_pct:.1f}%")
            lines.append("")

        # 排名数据
        if "ranked" in data:
            lines.append(f"**TPO 排名（Top {data.get('top_n', 3)}）**：")
            for i, s in enumerate(data["ranked"], 1):
                lines.append(f"{i}. {s['name']}：{s['tpo']:.2f}")
            lines.append("")

        # 异常数据
        if "anomalies" in data:
            if data["anomalies"]:
                lines.append(f"**检测到异常（阈值 {data['threshold']*100}%）**：")
                for a in data["anomalies"]:
                    value_pct = a["value"] * 100
                    direction = "[UP]" if a["value"] >= 0 else "[DOWN]"
                    lines.append(f"- {a['type']} 的 {a['metric']} {direction} {abs(value_pct):.1f}%")
                lines.append("")
            else:
                lines.append(f"**未检测到异常**（阈值 {data['threshold']*100}%）")
                lines.append("")

        # 对比数据
        if "comparisons" in data:
            lines.append(f"**场景对比**：")
            for c in data["comparisons"]:
                if c["change_pct"] is not None:
                    direction = "[UP]" if c["change"] >= 0 else "[DOWN]"
                    lines.append(f"- {c['name']}：{c['tpo1']:.2f} → {c['tpo2']:.2f} ({direction} {abs(c['change_pct']):.1f}%)")
            lines.append("")

    # 验证错误
    if "validation_errors" in answer:
        lines.append("[!] **验证警告**：")
        for error in answer["validation_errors"]:
            lines.append(f"- {error}")
        lines.append("")

    # 状态
    status_text = {
        "success": "[OK]",
        "warning": "[!]",
        "error": "[X]"
    }
    lines.append(f"**状态**：{status_text.get(answer.get('status'), '[?]')}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="智能问答引擎 v4.0")
    parser.add_argument("--file", required=True, help="csv 文件路径")
    parser.add_argument("--question", required=True, help="问题")
    args = parser.parse_args()

    # 创建问答引擎
    engine = QAEEngine(args.file)

    # 回答问题
    answer = engine.answer(args.question)

    # 格式化输出
    formatted_answer = format_answer(answer)
    print(formatted_answer)


if __name__ == "__main__":
    main()
