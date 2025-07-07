# Databricks SQLプロファイラー分析ツール
# このスクリプトは、DatabricksのSQLプロファイラーJSONログファイルを読み込み、
# ボトルネック特定と改善案の提示に必要なメトリクスを抽出して分析を行います。

import json
import pandas as pd
from typing import Dict, List, Any
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def load_profiler_json(file_path: str) -> Dict[str, Any]:
    """
    SQLプロファイラーJSONファイルを読み込む
    
    Args:
        file_path: JSONファイルのパス
        
    Returns:
        Dict: パースされたJSONデータ
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        print(f"✓ JSONファイルを正常に読み込みました: {file_path}")
        return data
    except Exception as e:
        print(f"✗ ファイル読み込みエラー: {str(e)}")
        return {}

def extract_performance_metrics(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからボトルネック分析に必要なメトリクスを抽出
    
    Args:
        profiler_data: SQLプロファイラーJSONデータ
        
    Returns:
        Dict: 抽出されたメトリクスデータ
    """
    metrics = {
        "query_info": {},
        "overall_metrics": {},
        "stage_metrics": [],
        "node_metrics": [],
        "bottleneck_indicators": {}
    }
    
    # 基本的なクエリ情報
    if 'query' in profiler_data:
        query = profiler_data['query']
        metrics["query_info"] = {
            "query_id": query.get('id', ''),
            "status": query.get('status', ''),
            "query_start_time": query.get('queryStartTimeMs', 0),
            "query_end_time": query.get('queryEndTimeMs', 0),
            "user": query.get('user', {}).get('displayName', ''),
            "query_text": query.get('queryText', '')[:500] + "..." if len(query.get('queryText', '')) > 500 else query.get('queryText', '')
        }
        
        # 全体的なメトリクス
        if 'metrics' in query:
            query_metrics = query['metrics']
            metrics["overall_metrics"] = {
                "total_time_ms": query_metrics.get('totalTimeMs', 0),
                "compilation_time_ms": query_metrics.get('compilationTimeMs', 0),
                "execution_time_ms": query_metrics.get('executionTimeMs', 0),
                "read_bytes": query_metrics.get('readBytes', 0),
                "read_remote_bytes": query_metrics.get('readRemoteBytes', 0),
                "read_cache_bytes": query_metrics.get('readCacheBytes', 0),
                "rows_produced_count": query_metrics.get('rowsProducedCount', 0),
                "rows_read_count": query_metrics.get('rowsReadCount', 0),
                "task_total_time_ms": query_metrics.get('taskTotalTimeMs', 0),
                "photon_total_time_ms": query_metrics.get('photonTotalTimeMs', 0),
                "spill_to_disk_bytes": query_metrics.get('spillToDiskBytes', 0),
                "read_files_count": query_metrics.get('readFilesCount', 0),
                "result_fetch_time_ms": query_metrics.get('resultFetchTimeMs', 0)
            }
    
    # グラフデータからステージとノードのメトリクスを抽出
    if 'graphs' in profiler_data and profiler_data['graphs']:
        graph = profiler_data['graphs'][0]  # 通常は1つのグラフ
        
        # ステージデータ
        if 'stageData' in graph:
            for stage in graph['stageData']:
                stage_metric = {
                    "stage_id": stage.get('stageId', ''),
                    "status": stage.get('status', ''),
                    "duration_ms": stage.get('keyMetrics', {}).get('durationMs', 0),
                    "num_tasks": stage.get('numTasks', 0),
                    "num_failed_tasks": stage.get('numFailedTasks', 0),
                    "num_complete_tasks": stage.get('numCompleteTasks', 0),
                    "start_time_ms": stage.get('startTimeMs', 0),
                    "end_time_ms": stage.get('endTimeMs', 0),
                    "name": stage.get('name', ''),
                    "description": stage.get('description', '')
                }
                metrics["stage_metrics"].append(stage_metric)
        
        # ノードデータ（実行プランの各操作）
        if 'nodes' in graph:
            for node in graph['nodes']:
                node_metric = {
                    "node_id": node.get('id', ''),
                    "name": node.get('name', ''),
                    "tag": node.get('tag', ''),
                    "key_metrics": node.get('keyMetrics', {}),
                    "hidden": node.get('hidden', False)
                }
                
                # 詳細メトリクス
                detailed_metrics = {}
                for metric in node.get('metrics', []):
                    metric_key = metric.get('key', '')
                    metric_value = metric.get('value', 0)
                    metric_type = metric.get('metricType', '')
                    metric_label = metric.get('label', '')
                    
                    # ボトルネック分析に重要なメトリクスのみ抽出
                    important_keywords = [
                        'TIME', 'DURATION', 'MEMORY', 'ROWS', 'BYTES', 'SPILL',
                        'PEAK', 'CUMULATIVE', 'EXCLUSIVE', 'WAIT', 'CPU'
                    ]
                    
                    if (any(keyword in metric_key.upper() for keyword in important_keywords) or
                        any(keyword in metric_type.upper() for keyword in important_keywords) or
                        any(keyword in metric_label.upper() for keyword in important_keywords)):
                        
                        detailed_metrics[metric_key] = {
                            'value': metric_value,
                            'type': metric_type,
                            'label': metric_label
                        }
                
                node_metric['detailed_metrics'] = detailed_metrics
                
                # 隠されていない重要なノードのみ保存
                if not node_metric['hidden'] or len(detailed_metrics) > 0:
                    metrics["node_metrics"].append(node_metric)
    
    # ボトルネック指標の計算
    metrics["bottleneck_indicators"] = calculate_bottleneck_indicators(metrics)
    
    return metrics

def calculate_bottleneck_indicators(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    ボトルネック指標を計算
    """
    indicators = {}
    
    # 全体的な実行時間の分析
    overall = metrics.get('overall_metrics', {})
    total_time = overall.get('total_time_ms', 0)
    execution_time = overall.get('execution_time_ms', 0)
    compilation_time = overall.get('compilation_time_ms', 0)
    
    if total_time > 0:
        indicators['compilation_ratio'] = compilation_time / total_time
        indicators['execution_ratio'] = execution_time / total_time
    
    # キャッシュ効率
    read_bytes = overall.get('read_bytes', 0)
    cache_bytes = overall.get('read_cache_bytes', 0)
    if read_bytes > 0:
        indicators['cache_hit_ratio'] = cache_bytes / read_bytes
    
    # データ処理効率
    rows_read = overall.get('rows_read_count', 0)
    rows_produced = overall.get('rows_produced_count', 0)
    if rows_read > 0:
        indicators['data_selectivity'] = rows_produced / rows_read
    
    # スピル検出
    spill_bytes = overall.get('spill_to_disk_bytes', 0)
    indicators['has_spill'] = spill_bytes > 0
    indicators['spill_bytes'] = spill_bytes
    
    # 最も時間のかかるステージ
    stage_durations = [(s['stage_id'], s['duration_ms']) for s in metrics.get('stage_metrics', []) if s['duration_ms'] > 0]
    if stage_durations:
        slowest_stage = max(stage_durations, key=lambda x: x[1])
        indicators['slowest_stage_id'] = slowest_stage[0]
        indicators['slowest_stage_duration'] = slowest_stage[1]
    
    # 最もメモリを使用するノード
    memory_usage = []
    for node in metrics.get('node_metrics', []):
        peak_memory = node.get('key_metrics', {}).get('peakMemoryBytes', 0)
        if peak_memory > 0:
            memory_usage.append((node['node_id'], node['name'], peak_memory))
    
    if memory_usage:
        highest_memory_node = max(memory_usage, key=lambda x: x[2])
        indicators['highest_memory_node_id'] = highest_memory_node[0]
        indicators['highest_memory_node_name'] = highest_memory_node[1]
        indicators['highest_memory_bytes'] = highest_memory_node[2]
    
    return indicators

def save_extracted_metrics(metrics: Dict[str, Any], output_path: str) -> None:
    """
    抽出したメトリクスをJSONファイルとして保存
    
    Args:
        metrics: 抽出されたメトリクスデータ
        output_path: 出力ファイルパス
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(metrics, file, indent=2, ensure_ascii=False)
        print(f"✓ 抽出メトリクスを保存しました: {output_path}")
    except Exception as e:
        print(f"✗ ファイル保存エラー: {str(e)}")

def analyze_bottlenecks_with_claude(metrics: Dict[str, Any]) -> str:
    """
    Databricks Claude 3.7 Sonnetエンドポイントを使用してボトルネック分析を行う
    
    Args:
        metrics: 抽出されたメトリクスデータ
        
    Returns:
        str: 分析結果テキスト
    """
    
    # メトリクス要約の準備
    analysis_prompt = f"""
あなたはDatabricksのSQLパフォーマンス分析の専門家です。以下のSQLプロファイラーメトリクスを分析し、ボトルネックを特定して改善案を提示してください。

【分析対象のメトリクス】

クエリ基本情報:
- クエリID: {metrics['query_info'].get('query_id', 'N/A')}
- ステータス: {metrics['query_info'].get('status', 'N/A')}
- 実行ユーザー: {metrics['query_info'].get('user', 'N/A')}

全体パフォーマンス:
- 総実行時間: {metrics['overall_metrics'].get('total_time_ms', 0):,} ms ({metrics['overall_metrics'].get('total_time_ms', 0)/1000:.2f} sec)
- コンパイル時間: {metrics['overall_metrics'].get('compilation_time_ms', 0):,} ms
- 実行時間: {metrics['overall_metrics'].get('execution_time_ms', 0):,} ms
- 読み込みデータ量: {metrics['overall_metrics'].get('read_bytes', 0):,} bytes ({metrics['overall_metrics'].get('read_bytes', 0)/1024/1024/1024:.2f} GB)
- キャッシュヒット量: {metrics['overall_metrics'].get('read_cache_bytes', 0):,} bytes ({metrics['overall_metrics'].get('read_cache_bytes', 0)/1024/1024/1024:.2f} GB)
- 読み込み行数: {metrics['overall_metrics'].get('rows_read_count', 0):,} 行
- 出力行数: {metrics['overall_metrics'].get('rows_produced_count', 0):,} 行
- スピルサイズ: {metrics['overall_metrics'].get('spill_to_disk_bytes', 0):,} bytes
- 読み込みファイル数: {metrics['overall_metrics'].get('read_files_count', 0):,} ファイル

ボトルネック指標:
- コンパイル時間比率: {metrics['bottleneck_indicators'].get('compilation_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('compilation_ratio', 0)*100:.1f}%)
- 実行時間比率: {metrics['bottleneck_indicators'].get('execution_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('execution_ratio', 0)*100:.1f}%)
- キャッシュヒット率: {metrics['bottleneck_indicators'].get('cache_hit_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('cache_hit_ratio', 0)*100:.1f}%)
- データ選択性: {metrics['bottleneck_indicators'].get('data_selectivity', 0):.3f} ({metrics['bottleneck_indicators'].get('data_selectivity', 0)*100:.1f}%)
- スピル発生: {'あり' if metrics['bottleneck_indicators'].get('has_spill', False) else 'なし'}
- 最も遅いステージID: {metrics['bottleneck_indicators'].get('slowest_stage_id', 'N/A')}
- 最も遅いステージ時間: {metrics['bottleneck_indicators'].get('slowest_stage_duration', 0):,} ms
- 最高メモリ使用ノード: {metrics['bottleneck_indicators'].get('highest_memory_node_name', 'N/A')}
- 最高メモリ使用量: {metrics['bottleneck_indicators'].get('highest_memory_bytes', 0)/1024/1024:.2f} MB

ステージ詳細:
{chr(10).join([f"- ステージ{s['stage_id']}: {s['duration_ms']:,}ms, タスク数:{s['num_tasks']}, 完了タスク:{s['num_complete_tasks']}, 失敗タスク:{s['num_failed_tasks']}" for s in metrics['stage_metrics'][:10]])}

主要ノード詳細（非隠匿のみ）:
{chr(10).join([f"- {n['name']} (ID:{n['node_id']}): 行数={n['key_metrics'].get('rowsNum', 0):,}, 時間={n['key_metrics'].get('durationMs', 0):,}ms, メモリ={n['key_metrics'].get('peakMemoryBytes', 0)/1024/1024:.2f}MB" for n in metrics['node_metrics'][:15] if not n['hidden']])}

【分析して欲しい内容】
1. 主要なボトルネックの特定と原因分析
2. パフォーマンス改善の優先順位付け
3. 具体的な最適化案の提示（SQL改善、設定変更、インフラ最適化など）
4. 予想される改善効果
5. 重要な注意点や推奨事項

日本語で詳細な分析結果を提供してください。
"""
    
    try:
        # Databricks Model Serving APIを使用
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        
        endpoint_url = f"https://{workspace_url}/serving-endpoints/databricks-claude-3-7-sonnet/invocations"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": analysis_prompt
                }
            ],
            "max_tokens": 4000,
            "temperature": 0.1
        }
        
        print("Databricks Claude 3.7 Sonnetエンドポイントに分析リクエストを送信中...")
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result.get('choices', [{}])[0].get('message', {}).get('content', '')
            print("✓ ボトルネック分析が完了しました")
            return analysis_text
        else:
            error_msg = f"APIエラー: ステータスコード {response.status_code}, レスポンス: {response.text}"
            print(f"✗ {error_msg}")
            return error_msg
            
    except Exception as e:
        error_msg = f"分析エラー: {str(e)}"
        print(f"✗ {error_msg}")
        return error_msg

def save_analysis_result(analysis_text: str, output_path: str) -> None:
    """
    分析結果をテキストファイルとして保存
    
    Args:
        analysis_text: 分析結果テキスト
        output_path: 出力ファイルパス
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write("Databricks SQLプロファイラー ボトルネック分析結果\n")
            file.write("=" * 60 + "\n\n")
            file.write(analysis_text)
        print(f"✓ 分析結果を保存しました: {output_path}")
    except Exception as e:
        print(f"✗ ファイル保存エラー: {str(e)}")

def main():
    """
    メイン処理関数
    """
    print("=" * 80)
    print("Databricks SQLプロファイラー分析ツール")
    print("=" * 80)
    
    # Spark環境の確認
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark Version: {spark.version}")
    print()
    
    # 1. SQLプロファイラーJSONファイルの読み込み
    profiler_data = load_profiler_json('simple0.json')
    if not profiler_data:
        print("JSONファイルの読み込みに失敗しました。処理を終了します。")
        return
    
    print(f"データサイズ: {len(str(profiler_data))} characters")
    print()
    
    # 2. パフォーマンスメトリクスの抽出
    extracted_metrics = extract_performance_metrics(profiler_data)
    print("✓ パフォーマンスメトリクスを抽出しました")
    
    # 3. 抽出されたメトリクスの確認
    print("\n" + "=" * 50)
    print("抽出されたメトリクス概要")
    print("=" * 50)
    
    print("=== クエリ基本情報 ===")
    for key, value in extracted_metrics['query_info'].items():
        if key == 'query_text':
            print(f"{key}: {value[:100]}..." if len(str(value)) > 100 else f"{key}: {value}")
        else:
            print(f"{key}: {value}")
    
    print("\n=== 全体メトリクス ===")
    for key, value in extracted_metrics['overall_metrics'].items():
        if 'bytes' in key:
            print(f"{key}: {value:,} bytes ({value/1024/1024:.2f} MB)")
        elif 'time' in key or 'ms' in key:
            print(f"{key}: {value:,} ms ({value/1000:.2f} sec)")
        else:
            print(f"{key}: {value:,}")
    
    print("\n=== ボトルネック指標 ===")
    for key, value in extracted_metrics['bottleneck_indicators'].items():
        if 'ratio' in key:
            print(f"{key}: {value:.3f} ({value*100:.1f}%)")
        elif 'bytes' in key:
            print(f"{key}: {value:,} bytes ({value/1024/1024:.2f} MB)")
        else:
            print(f"{key}: {value}")
    
    print(f"\n=== 統計 ===")
    print(f"ステージ数: {len(extracted_metrics['stage_metrics'])}")
    print(f"ノード数: {len(extracted_metrics['node_metrics'])}")
    print()
    
    # 4. 抽出したメトリクスをJSONファイルとして保存
    save_extracted_metrics(extracted_metrics, 'extracted_metrics.json')
    
    # 5. Databricks Claude 3.7 Sonnetを使用してボトルネック分析
    analysis_result = analyze_bottlenecks_with_claude(extracted_metrics)
    
    # 6. 分析結果の表示
    print("\n" + "=" * 80)
    print("【Databricks Claude 3.7 Sonnet による SQLボトルネック分析結果】")
    print("=" * 80)
    print()
    print(analysis_result)
    print()
    print("=" * 80)
    
    # 7. 分析結果の保存
    save_analysis_result(analysis_result, 'bottleneck_analysis_result.txt')
    
    # 8. 最終的なサマリー
    print("\n" + "=" * 60)
    print("【処理完了サマリー】")
    print("=" * 60)
    print("✓ SQLプロファイラーJSONファイル読み込み完了")
    print("✓ パフォーマンスメトリクス抽出完了 (extracted_metrics.json)")
    print("✓ Databricks Claude 3.7 Sonnetによるボトルネック分析完了")
    print("✓ 分析結果保存完了 (bottleneck_analysis_result.txt)")
    print("=" * 60)

if __name__ == "__main__":
    main()