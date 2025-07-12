#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
フィルタ率機能統合スクリプト
databricks_sql_profiler_analysis.pyにフィルタ率計算機能を統合します
"""

import re

def integrate_filter_rate_functionality():
    """
    メインファイルにフィルタ率計算機能を統合
    """
    
    # フィルタ率計算関数のコード
    filter_rate_functions = '''
def calculate_filter_rate(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    ノードからSize of files prunedとSize of files readメトリクスを抽出してフィルタ率を計算
    
    Args:
        node: ノードデータ
        
    Returns:
        Dict: フィルタ率計算結果
    """
    filter_rate = None
    files_pruned_bytes = 0
    files_read_bytes = 0
    
    # detailed_metricsから検索
    detailed_metrics = node.get('detailed_metrics', {})
    for metric_key, metric_info in detailed_metrics.items():
        metric_label = metric_info.get('label', '')
        metric_value = metric_info.get('value', 0)
        
        if metric_label == "Size of files pruned" and metric_value > 0:
            files_pruned_bytes = metric_value
        elif metric_label == "Size of files read" and metric_value > 0:
            files_read_bytes = metric_value
    
    # raw_metricsから検索（フォールバック）
    if files_pruned_bytes == 0 or files_read_bytes == 0:
        raw_metrics = node.get('metrics', [])
        for metric in raw_metrics:
            metric_label = metric.get('label', '')
            metric_value = metric.get('value', 0)
            
            if metric_label == "Size of files pruned" and metric_value > 0:
                files_pruned_bytes = metric_value
            elif metric_label == "Size of files read" and metric_value > 0:
                files_read_bytes = metric_value
    
    # フィルタ率計算（両方の値が存在する場合のみ）
    if files_read_bytes > 0 and files_pruned_bytes > 0:
        filter_rate = files_pruned_bytes / files_read_bytes
    
    return {
        "filter_rate": filter_rate,
        "files_pruned_bytes": files_pruned_bytes,
        "files_read_bytes": files_read_bytes,
        "has_filter_metrics": files_read_bytes > 0 and files_pruned_bytes > 0
    }

def format_filter_rate_display(filter_result: Dict[str, Any]) -> str:
    """
    フィルタ率計算結果を表示用文字列に変換
    
    Args:
        filter_result: calculate_filter_rate()の結果
        
    Returns:
        str: 表示用文字列
    """
    if not filter_result["has_filter_metrics"]:
        return None
    
    filter_rate = filter_result["filter_rate"]
    files_read_gb = filter_result["files_read_bytes"] / (1024 * 1024 * 1024)
    files_pruned_gb = filter_result["files_pruned_bytes"] / (1024 * 1024 * 1024)
    
    return f"📂 フィルタ率: {filter_rate:.1%} (読み込み: {files_read_gb:.2f}GB, プルーン: {files_pruned_gb:.2f}GB)"

'''

    # メインファイルを読み込み
    with open('databricks_sql_profiler_analysis.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. フィルタ率計算関数をファイル末尾に追加
    if 'def calculate_filter_rate(' not in content:
        content += '\n' + filter_rate_functions
        print("✅ フィルタ率計算関数を追加しました")
    else:
        print("ℹ️ フィルタ率計算関数は既に存在します")
    
    # 2. extract_detailed_bottleneck_analysis関数のnode_analysis辞書にフィルタ率フィールドを追加
    # 既存の"severity"行の後にフィルタ率フィールドを追加
    severity_pattern = r'"severity": "CRITICAL" if duration_ms >= 10000 else "HIGH" if duration_ms >= 5000 else "MEDIUM" if duration_ms >= 1000 else "LOW"'
    if severity_pattern in content and '"filter_rate"' not in content:
        new_severity_section = severity_pattern + ''',
            "filter_rate": None,
            "files_pruned_bytes": 0,
            "files_read_bytes": 0,
            "has_filter_metrics": False'''
        content = content.replace(severity_pattern, new_severity_section)
        print("✅ node_analysis辞書にフィルタ率フィールドを追加しました")
    
    # 3. フィルタ率計算ロジックを追加（node_analysis辞書作成後）
    node_analysis_creation_pattern = r'(        detailed_analysis\["top_bottleneck_nodes"\]\.append\(node_analysis\))'
    if not re.search(r'filter_result = calculate_filter_rate\(node\)', content):
        filter_calculation_code = '''
        # フィルタ率計算と情報更新
        filter_result = calculate_filter_rate(node)
        node_analysis.update({
            "filter_rate": filter_result["filter_rate"],
            "files_pruned_bytes": filter_result["files_pruned_bytes"],
            "files_read_bytes": filter_result["files_read_bytes"],
            "has_filter_metrics": filter_result["has_filter_metrics"]
        })
        
        \\1'''
        content = re.sub(node_analysis_creation_pattern, filter_calculation_code, content)
        print("✅ extract_detailed_bottleneck_analysis関数にフィルタ率計算を追加しました")
    
    # 4. TOP10レポート関数にフィルタ率表示を追加
    efficiency_pattern = r'(            if duration_ms > 0:\n                rows_per_sec = \(rows_num \* 1000\) / duration_ms\n                report_lines\.append\(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒"\))'
    if not re.search(r'フィルタ率表示', content):
        filter_display_code = '''\\1
            
            # フィルタ率表示
            filter_result = calculate_filter_rate(node)
            filter_display = format_filter_rate_display(filter_result)
            if filter_display:
                report_lines.append(f"    {filter_display}")'''
        content = re.sub(efficiency_pattern, filter_display_code, content)
        print("✅ TOP10レポート関数にフィルタ率表示を追加しました")
    
    # 変更されたファイルを書き込み
    with open('databricks_sql_profiler_analysis.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("🎉 フィルタ率機能の統合が完了しました！")
    
    # 統合後の説明
    print("\n=== 統合された機能 ===")
    print("1. ✅ calculate_filter_rate() - フィルタ率計算関数")
    print("2. ✅ format_filter_rate_display() - フィルタ率表示フォーマット関数") 
    print("3. ✅ extract_detailed_bottleneck_analysis() - セル33詳細分析にフィルタ率追加")
    print("4. ✅ generate_top10_time_consuming_processes_report() - TOP10レポートにフィルタ率表示追加")
    print("5. ✅ ファイル出力 - node_analysisに含まれるフィルタ率情報が自動的にファイル出力に含まれる")
    
    print("\n=== 表示される情報 ===")
    print("- 📂 フィルタ率: XX.X% (読み込み: X.XXG, プルーン: X.XXGB)")
    print("- Size of files prunedとSize of files readの両方が検出されたノードのみ表示")

if __name__ == "__main__":
    integrate_filter_rate_functionality() 