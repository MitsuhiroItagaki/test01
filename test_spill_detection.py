#!/usr/bin/env python3
"""
スピル検出テストスクリプト
セル11とセル12でスピル検出が異なる問題を調査・修正
"""

import json

def extract_spill_from_overall_metrics(profiler_data):
    """セル11相当: overall_metricsからのスピル検出"""
    spill_bytes = 0
    if 'query' in profiler_data and 'metrics' in profiler_data['query']:
        query_metrics = profiler_data['query']['metrics']
        spill_bytes = query_metrics.get('spillToDiskBytes', 0)
    
    print(f"セル11スタイル検出:")
    print(f"  spillToDiskBytes: {spill_bytes:,} bytes")
    print(f"  スピル検出: {'あり' if spill_bytes > 0 else 'なし'}")
    return spill_bytes > 0

def extract_spill_from_node_metrics(profiler_data):
    """セル12相当: ノードメトリクスからのスピル検出"""
    print(f"\nセル12スタイル検出:")
    
    if 'graphs' not in profiler_data or not profiler_data['graphs']:
        print("  グラフデータが見つかりません")
        return False
    
    graph = profiler_data['graphs'][0]
    nodes = graph.get('nodes', [])
    print(f"  {len(nodes)}個のノードを検査中...")
    
    total_spill_detected = False
    spill_details = []
    
    for i, node in enumerate(nodes):
        if node.get('hidden', False):
            continue
            
        node_name = node.get('name', '')
        node_id = node.get('id', '')
        
        # メトリクスの検査
        node_spill_detected = False
        node_spill_bytes = 0
        node_spill_details = []
        
        # 1. detailed_metrics相当の検査 (node.metrics)
        for metric in node.get('metrics', []):
            metric_key = metric.get('key', '')
            metric_label = metric.get('label', '')
            metric_value = metric.get('value', 0)
            
            # スピル関連キーワードの検査
            spill_keywords = ['SPILL', 'DISK', 'PRESSURE', 'SINK']
            
            is_spill_metric = False
            for keyword in spill_keywords:
                if keyword in metric_key.upper() or keyword in metric_label.upper():
                    is_spill_metric = True
                    break
            
            if is_spill_metric:
                print(f"    ノード{i+1} ({node_id}): {metric_key}")
                print(f"      ラベル: {metric_label}")
                print(f"      値: {metric_value}")
                
                # 「Sink - Num bytes spilled to disk due to memory pressure」のような
                # 具体的なスピル関連メトリクスを検出
                if 'SPILL' in metric_key.upper() and 'DISK' in metric_key.upper():
                    if metric_value > 0:
                        node_spill_detected = True
                        node_spill_bytes += metric_value
                        node_spill_details.append({
                            'metric_name': metric_key,
                            'value': metric_value,
                            'label': metric_label
                        })
                        print(f"      *** スピル検出! ***")
        
        # 2. keyMetrics相当の検査
        key_metrics = node.get('keyMetrics', {})
        for key_metric_name, key_metric_value in key_metrics.items():
            if ('spill' in key_metric_name.lower() or 'disk' in key_metric_name.lower()) and key_metric_value > 0:
                node_spill_detected = True
                node_spill_bytes += key_metric_value
                node_spill_details.append({
                    'metric_name': f"keyMetrics.{key_metric_name}",
                    'value': key_metric_value,
                    'label': f"Key metric: {key_metric_name}"
                })
                print(f"    ノード{i+1} ({node_id}): keyMetrics.{key_metric_name} = {key_metric_value}")
                print(f"      *** キーメトリクスでスピル検出! ***")
        
        if node_spill_detected:
            total_spill_detected = True
            spill_details.extend(node_spill_details)
            print(f"    ノード{i+1} 合計スピル: {node_spill_bytes:,} bytes")
        
        # 最初の10ノードのみ詳細表示
        if i >= 9:
            break
    
    print(f"  総合スピル検出: {'あり' if total_spill_detected else 'なし'}")
    print(f"  検出されたスピルメトリクス数: {len(spill_details)}")
    
    return total_spill_detected

def analyze_spill_detection_difference(file_path):
    """スピル検出の差異を分析"""
    print(f"🔍 ファイル分析: {file_path}")
    print("=" * 80)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            profiler_data = json.load(f)
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return
    
    # セル11とセル12の検出ロジックを比較
    cell11_result = extract_spill_from_overall_metrics(profiler_data)
    cell12_result = extract_spill_from_node_metrics(profiler_data)
    
    print(f"\n📊 結果比較:")
    print(f"  セル11 (overall_metrics): {'スピルあり' if cell11_result else 'スピルなし'}")
    print(f"  セル12 (node_metrics): {'スピルあり' if cell12_result else 'スピルなし'}")
    
    if cell11_result != cell12_result:
        print(f"⚠️ 検出結果に差異があります!")
        print(f"   セル11: {cell11_result}, セル12: {cell12_result}")
    else:
        print(f"✅ 両方の検出結果が一致しています")

def create_sample_spill_data():
    """スピルデータのサンプルを作成"""
    sample_data = {
        "query": {
            "metrics": {
                "spillToDiskBytes": 1234567890,  # セル11で検出されるスピル
                "totalTimeMs": 60000
            }
        },
        "graphs": [{
            "nodes": [
                {
                    "id": "test_node_1",
                    "name": "Test Shuffle Exchange",
                    "hidden": False,
                    "keyMetrics": {
                        "rowsNum": 1000,
                        "durationMs": 5000
                    },
                    "metrics": [
                        {
                            "key": "Sink - Num bytes spilled to disk due to memory pressure",
                            "label": "Number of bytes spilled to disk due to memory pressure",
                            "value": 987654321,  # セル12で検出されるべきスピル
                            "metricType": "SIZE_METRIC_BYTES"
                        },
                        {
                            "key": "Sink - Num rows spilled to disk due to memory pressure", 
                            "label": "Number of rows spilled to disk due to memory pressure",
                            "value": 50000,
                            "metricType": "UNKNOWN_TYPE"
                        },
                        {
                            "key": "Other metric",
                            "label": "Some other metric",
                            "value": 100,
                            "metricType": "UNKNOWN_TYPE"
                        }
                    ]
                },
                {
                    "id": "test_node_2", 
                    "name": "Test Aggregation",
                    "hidden": False,
                    "keyMetrics": {
                        "rowsNum": 500,
                        "durationMs": 3000,
                        "spillBytes": 123456  # keyMetricsでのスピル
                    },
                    "metrics": []
                }
            ]
        }]
    }
    
    with open('sample_spill_data.json', 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=2, ensure_ascii=False)
    
    print("📁 サンプルスピルデータを作成しました: sample_spill_data.json")
    return sample_data

if __name__ == "__main__":
    # 1. 既存のsimple0.jsonを分析
    print("1️⃣ 既存ファイルの分析")
    analyze_spill_detection_difference('simple0.json')
    
    print("\n" + "=" * 80)
    
    # 2. スピルデータありのサンプルを作成して分析
    print("2️⃣ スピルデータ付きサンプルの分析")
    create_sample_spill_data()
    analyze_spill_detection_difference('sample_spill_data.json')