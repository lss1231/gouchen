#!/usr/bin/env python3
"""
Phase 1 MVP SQL 生成准确性测试执行器

使用方法:
    python tests/test_cases/run_phase1_tests.py [--case A01] [--category single_table]

选项:
    --case: 运行指定测试用例 (如 A01)
    --category: 运行指定类别 (如 single_table, join, aggregation, user_analytics, clarification)
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
    """Phase 1 MVP 测试执行器"""

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
        print("Phase 1 MVP 测试用例列表")
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
                print(f"  {status} {case['id']}: {case['name']}")
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
                timeout=30
            )

            if response.status_code != 200:
                errors.append(f"API请求失败: {response.status_code} - {response.text}")
            else:
                result = response.json()

                # 根据状态处理
                if result.get('status') == 'completed':
                    result_data = result.get('result', {})
                    generated_sql = result_data.get('sql')
                    actual_datasource = result_data.get('datasource')
                    actual_tables = result_data.get('tables', [])

                    # 验证结果
                    if 'expected_datasource' in case:
                        if actual_datasource != case['expected_datasource']:
                            errors.append(f"数据源不匹配: 期望 {case['expected_datasource']}, 实际 {actual_datasource}")

                    if 'expected_tables' in case:
                        expected_tables = set(case['expected_tables'])
                        actual_tables_set = set(actual_tables) if actual_tables else set()
                        if not expected_tables.intersection(actual_tables_set):
                            errors.append(f"表不匹配: 期望 {expected_tables}, 实际 {actual_tables_set}")

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

    def run_tests(self, case_id: Optional[str] = None, category: Optional[str] = None):
        """运行测试用例"""
        # 筛选测试用例
        cases_to_run = self.test_cases
        if case_id:
            cases_to_run = [c for c in cases_to_run if c['id'] == case_id]
            if not cases_to_run:
                print(f"错误: 未找到测试用例 {case_id}")
                return
        elif category:
            cases_to_run = [c for c in cases_to_run if c['category'] == category]

        print("\n" + "=" * 80)
        print(f"Phase 1 MVP 测试执行 ({len(cases_to_run)} 个用例)")
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
        print(f"  通过率: {passed/len(self.results)*100:.1f}%")

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
    parser = argparse.ArgumentParser(description='Phase 1 MVP SQL生成测试')
    parser.add_argument('--list', action='store_true', help='列出所有测试用例')
    parser.add_argument('--case', type=str, help='运行指定测试用例 (如 A01)')
    parser.add_argument('--category', type=str,
                        choices=['single_table', 'join', 'aggregation', 'user_analytics', 'clarification'],
                        help='运行指定类别的测试用例')

    args = parser.parse_args()

    runner = Phase1TestRunner()

    if args.list:
        runner.list_cases()
    else:
        runner.run_tests(case_id=args.case, category=args.category)


if __name__ == '__main__':
    main()
