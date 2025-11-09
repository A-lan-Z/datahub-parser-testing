"""
Test Results Analysis Script

Analyzes CSV test results from parse_sql_test.py and generates:
- Statistical summaries
- Success rate breakdowns by category
- Complexity analysis
- Performance metrics
- Visualization-ready data
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


def load_results(csv_file: Path) -> List[Dict[str, Any]]:
    """Load test results from CSV file"""
    results = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row['success'] = row['success'].lower() == 'true'
            row['table_error'] = row['table_error'].lower() == 'true'
            row['confidence'] = float(row['confidence'])
            row['parse_time_ms'] = float(row['parse_time_ms'])
            row['in_tables_count'] = int(row['in_tables_count'])
            row['out_tables_count'] = int(row['out_tables_count'])
            row['column_lineage_count'] = int(row['column_lineage_count'])
            row['cte_count'] = int(row['cte_count'])
            row['max_cte_depth'] = int(row['max_cte_depth'])
            row['subquery_count'] = int(row['subquery_count'])
            row['join_count'] = int(row['join_count'])
            row['union_count'] = int(row['union_count'])
            row['window_function_count'] = int(row['window_function_count'])
            row['line_count'] = int(row['line_count'])
            row['char_count'] = int(row['char_count'])
            if row.get('tables_precision'):
                row['tables_precision'] = float(row['tables_precision'])
            if row.get('tables_recall'):
                row['tables_recall'] = float(row['tables_recall'])
            if row.get('columns_precision'):
                row['columns_precision'] = float(row['columns_precision'])
            if row.get('columns_recall'):
                row['columns_recall'] = float(row['columns_recall'])
            results.append(row)
    return results


def analyze_overall_stats(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate overall statistics"""
    total = len(results)
    if total == 0:
        return {}

    successful = sum(1 for r in results if r['success'])
    high_confidence = sum(1 for r in results if r['confidence'] >= 0.9)
    medium_confidence = sum(1 for r in results if 0.7 <= r['confidence'] < 0.9)
    low_confidence = sum(1 for r in results if 0 < r['confidence'] < 0.7)
    with_column_lineage = sum(1 for r in results if r['column_lineage_count'] > 0)
    with_table_lineage = sum(1 for r in results if r['in_tables_count'] > 0 or r['out_tables_count'] > 0)

    parse_times = [r['parse_time_ms'] for r in results]
    avg_parse_time = sum(parse_times) / len(parse_times)
    min_parse_time = min(parse_times)
    max_parse_time = max(parse_times)

    return {
        'total_queries': total,
        'successful': successful,
        'success_rate': successful / total,
        'high_confidence': high_confidence,
        'high_confidence_rate': high_confidence / total,
        'medium_confidence': medium_confidence,
        'medium_confidence_rate': medium_confidence / total,
        'low_confidence': low_confidence,
        'low_confidence_rate': low_confidence / total,
        'with_table_lineage': with_table_lineage,
        'table_lineage_rate': with_table_lineage / total,
        'with_column_lineage': with_column_lineage,
        'column_lineage_rate': with_column_lineage / total,
        'avg_parse_time_ms': avg_parse_time,
        'min_parse_time_ms': min_parse_time,
        'max_parse_time_ms': max_parse_time,
    }


def analyze_by_statement_type(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Analyze results grouped by statement type"""
    by_type = defaultdict(list)
    for r in results:
        by_type[r['statement_type']].append(r)

    analysis = {}
    for stmt_type, type_results in by_type.items():
        total = len(type_results)
        successful = sum(1 for r in type_results if r['success'])
        high_conf = sum(1 for r in type_results if r['confidence'] >= 0.9)
        with_col_lineage = sum(1 for r in type_results if r['column_lineage_count'] > 0)

        analysis[stmt_type] = {
            'total': total,
            'successful': successful,
            'success_rate': successful / total if total > 0 else 0,
            'high_confidence': high_conf,
            'high_confidence_rate': high_conf / total if total > 0 else 0,
            'with_column_lineage': with_col_lineage,
            'column_lineage_rate': with_col_lineage / total if total > 0 else 0,
        }

    return analysis


def analyze_by_complexity(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze relationship between complexity and success"""

    # Categorize by CTE complexity
    by_cte = {'none': [], 'simple': [], 'moderate': [], 'complex': []}
    for r in results:
        if r['cte_count'] == 0:
            by_cte['none'].append(r)
        elif r['cte_count'] <= 2:
            by_cte['simple'].append(r)
        elif r['cte_count'] <= 5:
            by_cte['moderate'].append(r)
        else:
            by_cte['complex'].append(r)

    cte_analysis = {}
    for level, level_results in by_cte.items():
        if level_results:
            total = len(level_results)
            successful = sum(1 for r in level_results if r['success'])
            cte_analysis[level] = {
                'total': total,
                'success_rate': successful / total,
            }

    # Categorize by JOIN complexity
    by_join = {'none': [], 'simple': [], 'moderate': [], 'complex': []}
    for r in results:
        if r['join_count'] == 0:
            by_join['none'].append(r)
        elif r['join_count'] <= 2:
            by_join['simple'].append(r)
        elif r['join_count'] <= 5:
            by_join['moderate'].append(r)
        else:
            by_join['complex'].append(r)

    join_analysis = {}
    for level, level_results in by_join.items():
        if level_results:
            total = len(level_results)
            successful = sum(1 for r in level_results if r['success'])
            join_analysis[level] = {
                'total': total,
                'success_rate': successful / total,
            }

    # Categorize by subquery complexity
    by_subquery = {'none': [], 'simple': [], 'moderate': [], 'complex': []}
    for r in results:
        if r['subquery_count'] == 0:
            by_subquery['none'].append(r)
        elif r['subquery_count'] <= 2:
            by_subquery['simple'].append(r)
        elif r['subquery_count'] <= 5:
            by_subquery['moderate'].append(r)
        else:
            by_subquery['complex'].append(r)

    subquery_analysis = {}
    for level, level_results in by_subquery.items():
        if level_results:
            total = len(level_results)
            successful = sum(1 for r in level_results if r['success'])
            subquery_analysis[level] = {
                'total': total,
                'success_rate': successful / total,
            }

    return {
        'by_cte_complexity': cte_analysis,
        'by_join_complexity': join_analysis,
        'by_subquery_complexity': subquery_analysis,
    }


def analyze_validation(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze validation results (if available)"""
    validated = [r for r in results if r['validation_status'] != 'no_expected']
    if not validated:
        return {'validated_count': 0}

    total_validated = len(validated)
    passed = sum(1 for r in validated if r['validation_status'] == 'passed')

    # Calculate average precision/recall
    table_precisions = [r['tables_precision'] for r in validated if r.get('tables_precision') is not None]
    table_recalls = [r['tables_recall'] for r in validated if r.get('tables_recall') is not None]
    column_precisions = [r['columns_precision'] for r in validated if r.get('columns_precision') is not None]
    column_recalls = [r['columns_recall'] for r in validated if r.get('columns_recall') is not None]

    return {
        'validated_count': total_validated,
        'passed': passed,
        'pass_rate': passed / total_validated,
        'avg_table_precision': sum(table_precisions) / len(table_precisions) if table_precisions else 0,
        'avg_table_recall': sum(table_recalls) / len(table_recalls) if table_recalls else 0,
        'avg_column_precision': sum(column_precisions) / len(column_precisions) if column_precisions else 0,
        'avg_column_recall': sum(column_recalls) / len(column_recalls) if column_recalls else 0,
    }


def analyze_errors(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze error patterns"""
    errors = [r for r in results if r['error_message']]

    if not errors:
        return {'error_count': 0}

    # Categorize errors by type
    error_types = defaultdict(list)
    for r in errors:
        # Extract error type (first part before colon)
        error_msg = r['error_message']
        error_type = error_msg.split(':')[0] if ':' in error_msg else 'Unknown'
        error_types[error_type].append(r)

    error_breakdown = {}
    for error_type, type_errors in error_types.items():
        error_breakdown[error_type] = {
            'count': len(type_errors),
            'example_query': type_errors[0]['query_id'],
            'example_message': type_errors[0]['error_message'][:200],
        }

    return {
        'error_count': len(errors),
        'error_rate': len(errors) / len(results),
        'error_breakdown': error_breakdown,
    }


def analyze_by_test_category(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Analyze results by test category (extracted from query_file path)"""
    by_category = defaultdict(list)

    for r in results:
        # Extract category from path like "test-queries/teradata/01-basic/..."
        query_file = r['query_file']
        parts = Path(query_file).parts
        if len(parts) >= 3:
            category = parts[-2]  # e.g., "01-basic", "02-ctes"
        else:
            category = "unknown"
        by_category[category].append(r)

    analysis = {}
    for category, cat_results in sorted(by_category.items()):
        total = len(cat_results)
        successful = sum(1 for r in cat_results if r['success'])
        high_conf = sum(1 for r in cat_results if r['confidence'] >= 0.9)
        with_col_lineage = sum(1 for r in cat_results if r['column_lineage_count'] > 0)

        analysis[category] = {
            'total': total,
            'successful': successful,
            'success_rate': successful / total if total > 0 else 0,
            'high_confidence': high_conf,
            'high_confidence_rate': high_conf / total if total > 0 else 0,
            'with_column_lineage': with_col_lineage,
            'column_lineage_rate': with_col_lineage / total if total > 0 else 0,
        }

    return analysis


def print_report(analysis: Dict[str, Any]) -> None:
    """Print formatted analysis report"""

    print("\n" + "="*80)
    print("SQL PARSER TEST RESULTS ANALYSIS")
    print("="*80)

    # Overall stats
    overall = analysis['overall']
    print("\nOVERALL STATISTICS:")
    print(f"  Total queries tested: {overall['total_queries']}")
    print(f"  Successful parses: {overall['successful']} ({overall['success_rate']*100:.1f}%)")
    print(f"  High confidence (>=0.9): {overall['high_confidence']} ({overall['high_confidence_rate']*100:.1f}%)")
    print(f"  Medium confidence (0.7-0.9): {overall['medium_confidence']} ({overall['medium_confidence_rate']*100:.1f}%)")
    print(f"  Low confidence (<0.7): {overall['low_confidence']} ({overall['low_confidence_rate']*100:.1f}%)")
    print(f"  With table lineage: {overall['with_table_lineage']} ({overall['table_lineage_rate']*100:.1f}%)")
    print(f"  With column lineage: {overall['with_column_lineage']} ({overall['column_lineage_rate']*100:.1f}%)")

    print(f"\nPERFORMANCE:")
    print(f"  Average parse time: {overall['avg_parse_time_ms']:.3f} ms")
    print(f"  Min parse time: {overall['min_parse_time_ms']:.3f} ms")
    print(f"  Max parse time: {overall['max_parse_time_ms']:.3f} ms")

    # By statement type
    print("\nBY STATEMENT TYPE:")
    by_type = analysis['by_statement_type']
    for stmt_type in sorted(by_type.keys()):
        stats = by_type[stmt_type]
        print(f"  {stmt_type}:")
        print(f"    Total: {stats['total']}")
        print(f"    Success rate: {stats['success_rate']*100:.1f}%")
        print(f"    High confidence rate: {stats['high_confidence_rate']*100:.1f}%")
        print(f"    Column lineage rate: {stats['column_lineage_rate']*100:.1f}%")

    # By test category
    if 'by_category' in analysis:
        print("\nBY TEST CATEGORY:")
        by_cat = analysis['by_category']
        for category in sorted(by_cat.keys()):
            stats = by_cat[category]
            print(f"  {category}:")
            print(f"    Total: {stats['total']}")
            print(f"    Success rate: {stats['success_rate']*100:.1f}%")
            print(f"    High confidence rate: {stats['high_confidence_rate']*100:.1f}%")
            print(f"    Column lineage rate: {stats['column_lineage_rate']*100:.1f}%")

    # Complexity analysis
    complexity = analysis['complexity']
    print("\nCOMPLEXITY IMPACT:")
    print("  CTE Complexity:")
    for level in ['none', 'simple', 'moderate', 'complex']:
        if level in complexity['by_cte_complexity']:
            stats = complexity['by_cte_complexity'][level]
            print(f"    {level}: {stats['total']} queries, {stats['success_rate']*100:.1f}% success")

    print("  JOIN Complexity:")
    for level in ['none', 'simple', 'moderate', 'complex']:
        if level in complexity['by_join_complexity']:
            stats = complexity['by_join_complexity'][level]
            print(f"    {level}: {stats['total']} queries, {stats['success_rate']*100:.1f}% success")

    print("  Subquery Complexity:")
    for level in ['none', 'simple', 'moderate', 'complex']:
        if level in complexity['by_subquery_complexity']:
            stats = complexity['by_subquery_complexity'][level]
            print(f"    {level}: {stats['total']} queries, {stats['success_rate']*100:.1f}% success")

    # Validation results
    validation = analysis['validation']
    if validation['validated_count'] > 0:
        print("\nVALIDATION RESULTS:")
        print(f"  Validated queries: {validation['validated_count']}")
        print(f"  Pass rate: {validation['pass_rate']*100:.1f}%")
        print(f"  Table lineage - Precision: {validation['avg_table_precision']*100:.1f}%, Recall: {validation['avg_table_recall']*100:.1f}%")
        print(f"  Column lineage - Precision: {validation['avg_column_precision']*100:.1f}%, Recall: {validation['avg_column_recall']*100:.1f}%")

    # Error analysis
    errors = analysis['errors']
    if errors['error_count'] > 0:
        print("\nERROR ANALYSIS:")
        print(f"  Total errors: {errors['error_count']} ({errors['error_rate']*100:.1f}%)")
        print("  Top error types:")
        sorted_errors = sorted(
            errors['error_breakdown'].items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )
        for error_type, details in sorted_errors[:5]:
            print(f"    {error_type}: {details['count']} occurrences")
            print(f"      Example: {details['example_message']}")

    print("\n" + "="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze SQL parser test results"
    )
    parser.add_argument(
        "results_csv",
        help="Path to CSV results file from parse_sql_test.py"
    )
    parser.add_argument(
        "--output-json",
        help="Path to write detailed analysis as JSON"
    )

    args = parser.parse_args()

    # Load results
    results = load_results(Path(args.results_csv))

    if not results:
        print("No results found in CSV file", file=sys.stderr)
        sys.exit(1)

    # Perform analysis
    analysis = {
        'overall': analyze_overall_stats(results),
        'by_statement_type': analyze_by_statement_type(results),
        'by_category': analyze_by_test_category(results),
        'complexity': analyze_by_complexity(results),
        'validation': analyze_validation(results),
        'errors': analyze_errors(results),
    }

    # Print report
    print_report(analysis)

    # Write JSON if requested
    if args.output_json:
        with open(args.output_json, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2)
        print(f"Detailed analysis written to: {args.output_json}")


if __name__ == "__main__":
    main()
