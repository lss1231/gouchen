#!/usr/bin/env python3
"""
Phase 2 Multi-domain SQL 生成准确性测试执行器

使用方法:
    python tests/test_cases/run_phase1_tests.py [--case A01] [--category single_table] [--domain ecommerce]

选项:
    --case: 运行指定测试用例 (如 A01)
    --category: 运行指定类别 (如 single_table, join, aggregation, user_analytics, supply_chain, saas, clarification)
    --domain: 运行指定业务域 (如 ecommerce, supply_chain, saas)
    --list: 列出所有测试用例
"""

import json
import os
import sys
import argparse
import requests
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

API_BASE_URL = "http://localhost:8000/api/v1"


@dataclass
class TestResult:
    case_id: str
    query: str
    passed: bool
    actual_datasource: Optional[str]
    actual_tables: Optional[List[str]]
    generated_sql: Optional[str]
    errors: List[str]
    execution_time_ms: int


class Phase1TestRunner:
    """Phase 2 多域测试执行器"""

    def __init__(self):
        self.test_cases = []
        self.results: List[TestResult] = []
        self._load_test_cases()

    def _load_test_cases(self):
        """加载测试用例"""
        case_file = os.path.join(os.path.dirname(__file__), 'phase1_test_cases.json')
        with open(case_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.test_cases = data['test_cases']

    def list_cases(self):
        """列出所有测试用例"""
        print("\n" + "=" * 80)
        print("Phase 2 多域测试用例列表")
        print("=" * 80)

        categories = {}
        for case in self.test_cases:
            cat = case['category']
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(case)

        for cat, cases in categories.items():
            print(f"\n【{cat.upper()}】({len(cases)} 个用例)")
            print("-" * 80)
            for case in cases:
                status = "[C]" if case['category'] == 'clarification' else "[Q]"
                domain = case.get('domain', 'ecommerce')
                print(f"  {status} [{domain}] {case['id']}: {case['name']}")
                print(f"      查询: {case['query']}")
                if 'expected_datasource' in case:
                    print(f"      期望数据源: {case['expected_datasource']}")
                print()

    def run_test(self, case: Dict) -> TestResult:
        """执行单个测试用例，调用API服务"""
        start_time = datetime.now()
        errors = []
        generated_sql = None
        actual_datasource = None
        actual_tables = []

        print(f"\n  测试: {case['id']} - {case['name']}")
        print(f"  查询: \"{case['query']}\"")

        try:
            # 调用API创建查询
            response = requests.post(
                f"{API_BASE_URL}/query/",
                json={
                    "query": case['query'],
                    "thread_id": f"test_{case['id']}",
                    "user_role": "analyst"
                },
                timeout=60
            )

            if response.status_code != 200:
                errors.append(f"API请求失败: {response.status_code} - {response.text}")
            else:
                result = response.json()

                # 根据状态处理
                if result.get('status') == 'pending_approval':
                    # 自动审批以继续获取最终结果
                    try:
                        approve_resp = requests.post(
                            f"{API_BASE_URL}/approve/",
                            json={
                                "thread_id": f"test_{case['id']}",
                                "decision": "approve"
                            },
                            timeout=60
                        )
                        if approve_resp.status_code != 200:
                            errors.append(f"审批请求失败: {approve_resp.status_code} - {approve_resp.text}")
                        else:
                            result = approve_resp.json()
                    except Exception as e:
                        errors.append(f"自动审批异常: {str(e)}")

                if result.get('status') == 'completed':
                    result_data = result.get('result', {})
                    generated_sql = result_data.get('generated_sql')
                    actual_datasource = result_data.get('datasource')
                    actual_tables = result_data.get('tables', [])
                    execution_result = result_data.get('execution_result', {})

                    # 1. 验证 SQL 是否生成
                    if not generated_sql:
                        errors.append("SQL 生成失败: generated_sql 为空")

                    # 2. 验证数据源
                    if 'expected_datasource' in case:
                        if actual_datasource != case['expected_datasource']:
                            errors.append(f"数据源不匹配: 期望 {case['expected_datasource']}, 实际 {actual_datasource}")

                    # 3. 弱匹配：表召回（仅记录 warning，不直接失败）
                    if 'expected_tables' in case and generated_sql:
                        expected_tables = set(case['expected_tables'])
                        actual_tables_set = set(actual_tables) if actual_tables else set()
                        sql_lower = generated_sql.lower()
                        matched_any = (
                            any(t in actual_tables_set for t in expected_tables) or
                            any(t.lower() in sql_lower for t in expected_tables)
                        )
                        if not matched_any:
                            print(f"    [WARN] 表弱匹配: 期望涉及 {expected_tables}, SQL 未明显使用这些表")

                    # 4. 验证 SQL 执行结果
                    if execution_result is None:
                        errors.append("SQL 执行失败: 未返回 execution_result")
                    elif execution_result.get('error'):
                        errors.append(f"SQL 执行错误: {execution_result.get('error')}")
                    elif execution_result.get('row_count', 0) <= 0:
                        errors.append(f"SQL 执行返回空结果: row_count={execution_result.get('row_count')}")

                elif result.get('status') == 'needs_clarification':
                    if case['category'] == 'clarification':
                        # 澄清类用例期望触发澄清
                        pass
                    else:
                        errors.append(f"意外触发澄清: {result.get('clarification_info')}")

                elif result.get('status') == 'error':
                    errors.append(f"处理错误: {result.get('error')}")

        except requests.exceptions.ConnectionError:
            errors.append("无法连接到API服务，请确认服务是否运行在 http://localhost:8000")
        except Exception as e:
            errors.append(f"测试执行异常: {str(e)}")

        end_time = datetime.now()
        execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

        return TestResult(
            case_id=case['id'],
            query=case['query'],
            passed=len(errors) == 0,
            actual_datasource=actual_datasource,
            actual_tables=actual_tables,
            generated_sql=generated_sql,
            errors=errors,
            execution_time_ms=execution_time_ms
        )

    def run_tests(self, case_id: Optional[str] = None, category: Optional[str] = None, domain: Optional[str] = None):
        """运行测试用例"""
        # 筛选测试用例
        cases_to_run = self.test_cases
        if case_id:
            cases_to_run = [c for c in cases_to_run if c['id'] == case_id]
            if not cases_to_run:
                print(f"错误: 未找到测试用例 {case_id}")
                return
        else:
            if category:
                cases_to_run = [c for c in cases_to_run if c['category'] == category]
            if domain:
                cases_to_run = [c for c in cases_to_run if c.get('domain') == domain]

        if not cases_to_run:
            print("错误: 没有符合条件的测试用例")
            return

        print("\n" + "=" * 80)
        print(f"Phase 2 多域测试执行 ({len(cases_to_run)} 个用例)")
        print("=" * 80)

        # 执行测试
        self.results = []
        for case in cases_to_run:
            result = self.run_test(case)
            self.results.append(result)

        # 输出报告
        self._print_report()

    def _print_report(self):
        """打印测试报告"""
        print("\n" + "=" * 80)
        print("测试结果汇总")
        print("=" * 80)

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        print(f"\n  总计: {len(self.results)} 个用例")
        print(f"  通过: {passed} 个")
        print(f"  失败: {failed} 个")
        if len(self.results) > 0:
            print(f"  通过率: {passed/len(self.results)*100:.1f}%")

        # 按 domain 统计
        domain_stats = {}
        for result in self.results:
            case = next((c for c in self.test_cases if c['id'] == result.case_id), {})
            dom = case.get('domain', 'unknown')
            if dom not in domain_stats:
                domain_stats[dom] = {'total': 0, 'passed': 0}
            domain_stats[dom]['total'] += 1
            if result.passed:
                domain_stats[dom]['passed'] += 1

        if len(domain_stats) > 1:
            print("\n  按域统计:")
            for dom, stats in sorted(domain_stats.items()):
                pct = stats['passed'] / stats['total'] * 100 if stats['total'] > 0 else 0
                print(f"    {dom}: {stats['passed']}/{stats['total']} ({pct:.1f}%)")

        if failed > 0:
            print("\n  失败用例详情:")
            for result in self.results:
                if not result.passed:
                    print(f"    - {result.case_id}: {result.query}")
                    for error in result.errors:
                        print(f"      错误: {error}")

        # 保存详细报告
        self._save_report()

    def _save_report(self):
        """保存详细报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total': len(self.results),
                'passed': sum(1 for r in self.results if r.passed),
                'failed': sum(1 for r in self.results if not r.passed),
            },
            'results': [
                {
                    'case_id': r.case_id,
                    'query': r.query,
                    'passed': r.passed,
                    'actual_datasource': r.actual_datasource,
                    'actual_tables': r.actual_tables,
                    'generated_sql': r.generated_sql,
                    'errors': r.errors,
                    'execution_time_ms': r.execution_time_ms
                }
                for r in self.results
            ]
        }

        report_file = os.path.join(os.path.dirname(__file__), 'test_report.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n  详细报告已保存: {report_file}")


def main():
    parser = argparse.ArgumentParser(description='Phase 2 多域SQL生成测试')
    parser.add_argument('--list', action='store_true', help='列出所有测试用例')
    parser.add_argument('--case', type=str, help='运行指定测试用例 (如 A01)')
    parser.add_argument('--category', type=str,
                        choices=['single_table', 'join', 'aggregation', 'ranking', 'user_analytics',
                                 'supply_chain', 'saas', 'clarification'],
                        help='运行指定类别的测试用例')
    parser.add_argument('--domain', type=str,
                        choices=['ecommerce', 'supply_chain', 'saas'],
                        help='运行指定业务域的测试用例')

    args = parser.parse_args()

    runner = Phase1TestRunner()

    if args.list:
        runner.list_cases()
    else:
        runner.run_tests(case_id=args.case, category=args.category, domain=args.domain)


if __name__ == '__main__':
    main()
