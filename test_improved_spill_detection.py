#!/usr/bin/env python3
"""
改善されたスピル検出ロジックのテストスクリプト
セル12の強化されたスピル検出機能を検証
"""

import json

def test_improved_spill_detection(profiler_data):
    """改善されたセル12スタイルのスピル検出をテスト"""
    print(f"🔬 改善されたセル12スピル検出テスト:")
    
    if 'graphs' not in profiler_data or not profiler_data['graphs']:
        print("  グラフデータが見つかりません")
        return False
    
    graph = profiler_data['graphs'][0]
    nodes = graph.get('nodes', [])
    print(f"  {len(nodes)}個のノードを検査中...")
    
    total_spill_detected = False
    total_spill_bytes = 0
    spill_details = []
    
    for i, node in enumerate(nodes):
        if node.get('hidden', False):
            continue
            
        node_name = node.get('name', '')
        node_id = node.get('id', '')
        
        # ノードメトリクスの詳細分析
        node_spill_detected = False
        node_spill_bytes = 0
        node_spill_details = []
        spill_metrics_found = 0
        
        print(f"\n    📋 ノード{i+1}: {node_name} (ID: {node_id})")
        
        # 1. node.metricsからの詳細検査
        for metric in node.get('metrics', []):
            metric_key = metric.get('key', '')
            metric_label = metric.get('label', '')
            metric_value = metric.get('value', 0)
            
            # 改善されたスピル検出ロジック
            spill_patterns = ['SPILL', 'DISK', 'PRESSURE']
            
            is_spill_metric = False
            metric_key_clean = metric_key.upper().replace(' ', '').replace('-', '').replace('_', '')
            metric_label_clean = metric_label.upper().replace(' ', '').replace('-', '').replace('_', '')
            
            # 基本的なスピル関連キーワードの検査
            for pattern in spill_patterns:
                if pattern in metric_key_clean or pattern in metric_label_clean:
                    is_spill_metric = True
                    break
            
            # より具体的なスピル関連の組み合わせパターン
            spill_combinations = [
                ('SPILL', 'DISK'),      # "spilled to disk"
                ('SPILL', 'MEMORY'),    # "spilled due to memory"
                ('BYTES', 'SPILL'),     # "bytes spilled"
                ('ROWS', 'SPILL'),      # "rows spilled"
                ('SINK', 'SPILL'),      # "Sink spill"
                ('SPILL', 'PRESSURE'),  # "spilled due to pressure"
            ]
            
            for word1, word2 in spill_combinations:
                if (word1 in metric_key_clean and word2 in metric_key_clean) or \
                   (word1 in metric_label_clean and word2 in metric_label_clean):
                    is_spill_metric = True
                    break
            
            if is_spill_metric:
                spill_metrics_found += 1
                status = "🔴 値あり" if metric_value > 0 else "⚪ 値ゼロ"
                print(f"      {status} {metric_key}")
                if metric_label and metric_label != metric_key:
                    print(f"        ラベル: {metric_label}")
                
                if metric_value > 0:
                    print(f"        値: {metric_value:,} bytes ({metric_value/1024/1024:.2f} MB)")
                    node_spill_detected = True
                    node_spill_bytes += metric_value
                    node_spill_details.append({
                        'metric_name': metric_key,
                        'value': metric_value,
                        'label': metric_label
                    })
                else:
                    print(f"        値: {metric_value}")
        
        # 2. keyMetricsからの検査
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
                print(f"      🔴 keyMetrics.{key_metric_name} = {key_metric_value:,}")
        
        # ノード結果のサマリー
        if node_spill_detected:
            total_spill_detected = True
            total_spill_bytes += node_spill_bytes
            spill_details.extend(node_spill_details)
            print(f"      ✅ ノードスピル検出: {node_spill_bytes:,} bytes")
        elif spill_metrics_found > 0:
            print(f"      ⚪ {spill_metrics_found}個のスピル関連メトリクスあり（すべて値ゼロ）")
        else:
            print(f"      ❌ スピル関連メトリクス未検出")
        
        # 最初の5ノードのみ詳細表示
        if i >= 4:
            remaining_nodes = len(nodes) - i - 1
            if remaining_nodes > 0:
                print(f"    ... 他 {remaining_nodes} ノード")
            break
    
    print(f"\n  📊 総合結果:")
    print(f"    スピル検出: {'あり' if total_spill_detected else 'なし'}")
    print(f"    総スピル量: {total_spill_bytes:,} bytes ({total_spill_bytes/1024/1024:.2f} MB)")
    print(f"    検出されたスピルメトリクス数: {len(spill_details)}")
    
    return total_spill_detected

def create_largeplan_mock_data():
    """test01/largeplan.json相当のモックデータを作成"""
    # 実際のスピルが発生しているシナリオを模擬
    mock_data = {
        "query": {
            "metrics": {
                "spillToDiskBytes": 5000000000,  # 5GB - セル11で検出される
                "totalTimeMs": 180000  # 3分
            }
        },
        "graphs": [{
            "nodes": [
                {
                    "id": "large_shuffle_1",
                    "name": "Large Shuffle Exchange Sink",
                    "hidden": False,
                    "keyMetrics": {
                        "rowsNum": 10000000,
                        "durationMs": 45000,
                        "peakMemoryBytes": 8589934592  # 8GB
                    },
                    "metrics": [
                        {
                            "key": "Sink - Num bytes spilled to disk due to memory pressure",
                            "label": "Number of bytes spilled to disk due to memory pressure",
                            "value": 2147483648,  # 2GB スピル
                            "metricType": "SIZE_METRIC_BYTES"
                        },
                        {
                            "key": "Sink - Num rows spilled to disk due to memory pressure", 
                            "label": "Number of rows spilled to disk due to memory pressure",
                            "value": 500000,
                            "metricType": "UNKNOWN_TYPE"
                        },
                        {
                            "key": "Sink - Num spills to disk due to memory pressure",
                            "label": "Number of spills to disk due to memory pressure", 
                            "value": 5,
                            "metricType": "UNKNOWN_TYPE"
                        }
                    ]
                },
                {
                    "id": "large_aggregate_1",
                    "name": "Large Grouping Aggregate", 
                    "hidden": False,
                    "keyMetrics": {
                        "rowsNum": 5000000,
                        "durationMs": 60000,
                        "peakMemoryBytes": 4294967296  # 4GB
                    },
                    "metrics": [
                        {
                            "key": "Num bytes spilled to disk due to memory pressure",
                            "label": "Number of bytes spilled to disk due to memory pressure",
                            "value": 1073741824,  # 1GB スピル
                            "metricType": "SIZE_METRIC_BYTES"
                        },
                        {
                            "key": "Num rows spilled to disk due to memory pressure",
                            "label": "Number of rows spilled to disk due to memory pressure", 
                            "value": 250000,
                            "metricType": "UNKNOWN_TYPE"
                        }
                    ]
                },
                {
                    "id": "normal_scan_1",
                    "name": "Scan large_table",
                    "hidden": False,
                    "keyMetrics": {
                        "rowsNum": 100000000,
                        "durationMs": 30000
                    },
                    "metrics": [
                        {
                            "key": "Size of data read with io requests",
                            "label": "Size of data read with io requests",
                            "value": 10737418240,  # 10GB読み込み、スピルなし
                            "metricType": "SIZE_METRIC_BYTES"
                        }
                    ]
                }
            ]
        }]
    }
    
    with open('largeplan_mock.json', 'w', encoding='utf-8') as f:
        json.dump(mock_data, f, indent=2, ensure_ascii=False)
    
    print("📁 largeplan.json相当のモックデータを作成しました: largeplan_mock.json")
    return mock_data

if __name__ == "__main__":
    print("🧪 改善されたスピル検出ロジックのテスト")
    print("=" * 80)
    
    # 1. 既存のsimple0.jsonをテスト
    print("1️⃣ simple0.json での改善テスト")
    try:
        with open('simple0.json', 'r', encoding='utf-8') as f:
            simple_data = json.load(f)
        test_improved_spill_detection(simple_data)
    except Exception as e:
        print(f"❌ simple0.json読み込みエラー: {e}")
    
    print("\n" + "=" * 80)
    
    # 2. スピルありのサンプルデータでテスト
    print("2️⃣ スピル付きサンプルデータでの改善テスト")
    try:
        with open('sample_spill_data.json', 'r', encoding='utf-8') as f:
            sample_data = json.load(f)
        test_improved_spill_detection(sample_data)
    except Exception as e:
        print(f"❌ sample_spill_data.json読み込みエラー: {e}")
    
    print("\n" + "=" * 80)
    
    # 3. largeplan.json相当の大規模スピルシナリオをテスト
    print("3️⃣ largeplan.json相当の大規模スピルシナリオテスト")
    mock_data = create_largeplan_mock_data()
    test_improved_spill_detection(mock_data)