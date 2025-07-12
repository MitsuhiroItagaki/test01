#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
フィルタ率計算機能追加スクリプト
Size of files pruned / Size of files read のメトリクスを使用してフィルタ率を計算
"""

def calculate_filter_rate(node):
    """
    ノードからSize of files prunedとSize of files readメトリクスを抽出してフィルタ率を計算
    
    Args:
        node: ノードデータ
        
    Returns:
        dict: フィルタ率計算結果
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

def format_filter_rate_display(filter_result):
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

# 使用例とテスト用の関数
def test_filter_rate_calculation():
    """
    フィルタ率計算のテスト例
    """
    # サンプルノードデータ
    sample_node = {
        'detailed_metrics': {
            'metric1': {
                'label': 'Size of files read',
                'value': 1073741824  # 1GB
            },
            'metric2': {
                'label': 'Size of files pruned', 
                'value': 536870912   # 0.5GB
            }
        }
    }
    
    result = calculate_filter_rate(sample_node)
    display = format_filter_rate_display(result)
    
    print("=== フィルタ率計算テスト ===")
    print(f"フィルタ率: {result['filter_rate']:.1%}" if result['filter_rate'] else "フィルタ率: 計算不可")
    print(f"表示文字列: {display}")
    print()

# TOP5処理に組み込むためのサンプルコード
def add_filter_rate_to_top5_node_analysis(node_analysis, node):
    """
    既存のnode_analysis辞書にフィルタ率情報を追加
    
    Args:
        node_analysis: 既存のノード分析結果辞書
        node: 元のノードデータ
        
    Returns:
        dict: フィルタ率情報が追加されたnode_analysis
    """
    filter_result = calculate_filter_rate(node)
    
    # フィルタ率情報を追加
    node_analysis.update({
        "filter_rate": filter_result["filter_rate"],
        "files_pruned_bytes": filter_result["files_pruned_bytes"],
        "files_read_bytes": filter_result["files_read_bytes"],
        "has_filter_metrics": filter_result["has_filter_metrics"]
    })
    
    return node_analysis

def add_filter_rate_to_report_lines(report_lines, node):
    """
    TOP10レポートの行リストにフィルタ率表示を追加
    
    Args:
        report_lines: レポート行のリスト
        node: ノードデータ
    """
    filter_result = calculate_filter_rate(node)
    display = format_filter_rate_display(filter_result)
    
    if display:
        report_lines.append(f"    {display}")

if __name__ == "__main__":
    test_filter_rate_calculation()
    
    print("=== 統合ガイド ===")
    print("1. extract_detailed_bottleneck_analysis関数のnode_analysis辞書作成後に以下を追加:")
    print("   node_analysis = add_filter_rate_to_top5_node_analysis(node_analysis, node)")
    print()
    print("2. generate_top10_time_consuming_processes_report関数の効率性表示後に以下を追加:")
    print("   add_filter_rate_to_report_lines(report_lines, node)")
    print()
    print("3. ファイル出力時は既にnode_analysisに含まれているfilter_rate情報を使用可能") 