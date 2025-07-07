# ===============================================
# Databricks SQLプロファイラー分析ツール - Notebook版
# ===============================================
# 
# 以下の各セクションを、Databricksのnotebookで新しいセルにコピーペーストして使用してください。
# 各セクションは "# === セル X ===" で区切られています。

# === セル 1: マークダウンセル ===
# セルタイプを「Markdown」に変更して以下をコピー:
"""
# Databricks SQLプロファイラー分析ツール

このnotebookは、DatabricksのSQLプロファイラーJSONログファイルを読み込み、ボトルネック特定と改善案の提示に必要なメトリクスを抽出して分析を行います。

## 機能概要

1. **SQLプロファイラーJSONファイルの読み込み**
   - Databricksで出力されたプロファイラーログの解析
   - `graphs`キーに格納された実行プランメトリクスの抽出

2. **重要メトリクスの抽出**
   - クエリ基本情報（ID、ステータス、実行時間など）
   - 全体パフォーマンス（実行時間、データ量、キャッシュ効率など）
   - ステージ・ノード詳細メトリクス
   - ボトルネック指標の計算

3. **AI によるボトルネック分析**
   - Databricks Claude 3.7 Sonnetエンドポイントを使用
   - 抽出メトリクスからボトルネック特定
   - 具体的な改善案の提示

**事前準備:**
- Databricks Claude 3.7 Sonnetエンドポイントの設定
- SQLプロファイラーJSONファイルの準備（DBFS または ローカル）
"""

# === セル 2: ライブラリインポート ===
# 必要なライブラリのインポート
import json
import pandas as pd
from typing import Dict, List, Any
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Databricks環境の確認
spark = SparkSession.builder.getOrCreate()
print(f"Spark Version: {spark.version}")
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'Unknown')}")

# === セル 3: JSONファイル読み込み関数 ===
def load_profiler_json(file_path: str) -> Dict[str, Any]:
    """
    SQLプロファイラーJSONファイルを読み込む
    
    Args:
        file_path: JSONファイルのパス（DBFS または ローカルパス）
        
    Returns:
        Dict: パースされたJSONデータ
    """
    try:
        # DBFSパスの場合は適切に処理
        if file_path.startswith('/dbfs/'):
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        elif file_path.startswith('dbfs:/'):
            # dbfs: プレフィックスを/dbfs/に変換
            local_path = file_path.replace('dbfs:', '/dbfs')
            with open(local_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        else:
            # ローカルファイル
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        
        print(f"✓ JSONファイルを正常に読み込みました: {file_path}")
        print(f"データサイズ: {len(str(data))} characters")
        return data
    except Exception as e:
        print(f"✗ ファイル読み込みエラー: {str(e)}")
        return {}

print("関数定義完了: load_profiler_json")

# === セル 4: メトリクス抽出関数 ===
def extract_performance_metrics(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからボトルネック分析に必要なメトリクスを抽出
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
            "query_text": query.get('queryText', '')[:300] + "..." if len(query.get('queryText', '')) > 300 else query.get('queryText', '')
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
                "spill_to_disk_bytes": query_metrics.get('spillToDiskBytes', 0),
                "read_files_count": query_metrics.get('readFilesCount', 0)
            }
    
    # グラフデータからステージとノードのメトリクスを抽出
    if 'graphs' in profiler_data and profiler_data['graphs']:
        graph = profiler_data['graphs'][0]
        
        # ステージデータ
        if 'stageData' in graph:
            for stage in graph['stageData']:
                stage_metric = {
                    "stage_id": stage.get('stageId', ''),
                    "status": stage.get('status', ''),
                    "duration_ms": stage.get('keyMetrics', {}).get('durationMs', 0),
                    "num_tasks": stage.get('numTasks', 0),
                    "num_failed_tasks": stage.get('numFailedTasks', 0),
                    "num_complete_tasks": stage.get('numCompleteTasks', 0)
                }
                metrics["stage_metrics"].append(stage_metric)
        
        # ノードデータ（重要なもののみ）
        if 'nodes' in graph:
            for node in graph['nodes']:
                if not node.get('hidden', False):
                    node_metric = {
                        "node_id": node.get('id', ''),
                        "name": node.get('name', ''),
                        "tag": node.get('tag', ''),
                        "key_metrics": node.get('keyMetrics', {})
                    }
                    
                    # 重要なメトリクスのみ詳細抽出
                    detailed_metrics = {}
                    for metric in node.get('metrics', []):
                        metric_key = metric.get('key', '')
                        if any(keyword in metric_key.upper() for keyword in 
                               ['TIME', 'MEMORY', 'ROWS', 'BYTES', 'DURATION', 'PEAK']):
                            detailed_metrics[metric_key] = {
                                'value': metric.get('value', 0),
                                'label': metric.get('label', '')
                            }
                    node_metric['detailed_metrics'] = detailed_metrics
                    metrics["node_metrics"].append(node_metric)
    
    # ボトルネック指標の計算
    metrics["bottleneck_indicators"] = calculate_bottleneck_indicators(metrics)
    
    return metrics

print("関数定義完了: extract_performance_metrics")

# === セル 5: ボトルネック指標計算関数 ===
def calculate_bottleneck_indicators(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """ボトルネック指標を計算"""
    indicators = {}
    
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

print("関数定義完了: calculate_bottleneck_indicators")

# === セル 6: Claude 3.7 Sonnet分析関数 ===
def analyze_bottlenecks_with_claude(metrics: Dict[str, Any]) -> str:
    """
    Databricks Claude 3.7 Sonnetエンドポイントを使用してボトルネック分析を行う
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
- キャッシュヒット量: {metrics['overall_metrics'].get('read_cache_bytes', 0):,} bytes
- 読み込み行数: {metrics['overall_metrics'].get('rows_read_count', 0):,} 行
- 出力行数: {metrics['overall_metrics'].get('rows_produced_count', 0):,} 行
- スピルサイズ: {metrics['overall_metrics'].get('spill_to_disk_bytes', 0):,} bytes

ボトルネック指標:
- コンパイル時間比率: {metrics['bottleneck_indicators'].get('compilation_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('compilation_ratio', 0)*100:.1f}%)
- キャッシュヒット率: {metrics['bottleneck_indicators'].get('cache_hit_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('cache_hit_ratio', 0)*100:.1f}%)
- データ選択性: {metrics['bottleneck_indicators'].get('data_selectivity', 0):.3f} ({metrics['bottleneck_indicators'].get('data_selectivity', 0)*100:.1f}%)
- スピル発生: {'あり' if metrics['bottleneck_indicators'].get('has_spill', False) else 'なし'}
- 最も遅いステージID: {metrics['bottleneck_indicators'].get('slowest_stage_id', 'N/A')}
- 最高メモリ使用ノード: {metrics['bottleneck_indicators'].get('highest_memory_node_name', 'N/A')}

ステージ詳細:
{chr(10).join([f"- ステージ{s['stage_id']}: {s['duration_ms']:,}ms, タスク数:{s['num_tasks']}, 完了:{s['num_complete_tasks']}, 失敗:{s['num_failed_tasks']}" for s in metrics['stage_metrics'][:10]])}

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

print("関数定義完了: analyze_bottlenecks_with_claude")

# === セル 7: マークダウンセル - メイン処理の説明 ===
# セルタイプを「Markdown」に変更して以下をコピー:
"""
## メイン処理の実行

以下のセルを順番に実行して、SQLプロファイラー分析を実行します。
"""

# === セル 8: 設定とファイル読み込み ===
# 設定: 分析対象のJSONファイルパスを指定
# ローカルファイル、DBFS、またはクラウドストレージのパスを指定できます
JSON_FILE_PATH = 'simple0.json'  # デフォルト: サンプルファイル

# または DBFS のファイルを指定する場合:
# JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'
# JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'

print("=" * 80)
print("Databricks SQLプロファイラー分析ツール")
print("=" * 80)
print(f"分析対象ファイル: {JSON_FILE_PATH}")
print()

# SQLプロファイラーJSONファイルの読み込み
profiler_data = load_profiler_json(JSON_FILE_PATH)
if not profiler_data:
    print("JSONファイルの読み込みに失敗しました。ファイルパスを確認してください。")
    dbutils.notebook.exit("File loading failed")

print(f"✓ データ読み込み完了")
print()

# === セル 9: メトリクス抽出と表示 ===
# パフォーマンスメトリクスの抽出
extracted_metrics = extract_performance_metrics(profiler_data)
print("✓ パフォーマンスメトリクスを抽出しました")

# 抽出されたメトリクスの概要表示
print("\n=== 抽出されたメトリクス概要 ===")
print(f"クエリID: {extracted_metrics['query_info']['query_id']}")
print(f"ステータス: {extracted_metrics['query_info']['status']}")
print(f"実行時間: {extracted_metrics['overall_metrics']['total_time_ms']:,} ms")
print(f"読み込みデータ: {extracted_metrics['overall_metrics']['read_bytes']/1024/1024/1024:.2f} GB")
print(f"ステージ数: {len(extracted_metrics['stage_metrics'])}")
print(f"ノード数: {len(extracted_metrics['node_metrics'])}")

# ボトルネック指標の表示
print("\n=== ボトルネック指標 ===")
for key, value in extracted_metrics['bottleneck_indicators'].items():
    if 'ratio' in key:
        print(f"{key}: {value:.3f} ({value*100:.1f}%)")
    elif 'bytes' in key:
        print(f"{key}: {value:,} bytes ({value/1024/1024:.2f} MB)")
    else:
        print(f"{key}: {value}")
print()

# === セル 10: メトリクス保存とDataFrame表示 ===
# 抽出したメトリクスをJSONファイルとして保存
output_path = 'extracted_metrics.json'
with open(output_path, 'w', encoding='utf-8') as file:
    json.dump(extracted_metrics, file, indent=2, ensure_ascii=False)
print(f"✓ 抽出メトリクスを保存しました: {output_path}")

# DataFrameとしても確認
if extracted_metrics['stage_metrics']:
    stage_df = spark.createDataFrame(extracted_metrics['stage_metrics'])
    print("\n=== ステージメトリクス (DataFrame) ===")
    stage_df.show(truncate=False)

# ノードメトリクスの概要
print(f"\n=== ノードメトリクス概要 ===")
for node in extracted_metrics['node_metrics'][:10]:  # 上位10件表示
    print(f"- {node['name']} (ID:{node['node_id']}): 行数={node['key_metrics'].get('rowsNum', 0):,}, "
          f"時間={node['key_metrics'].get('durationMs', 0):,}ms, "
          f"メモリ={node['key_metrics'].get('peakMemoryBytes', 0)/1024/1024:.2f}MB")
print()

# === セル 11: AI分析実行 ===
# Databricks Claude 3.7 Sonnetを使用してボトルネック分析
print("Claude 3.7 Sonnetによるボトルネック分析を開始します...")
print("※ Model Servingエンドポイント 'databricks-claude-3-7-sonnet' が必要です")
print()

analysis_result = analyze_bottlenecks_with_claude(extracted_metrics)

# === セル 12: 分析結果表示と保存 ===
# 分析結果の表示
print("\n" + "=" * 80)
print("【Databricks Claude 3.7 Sonnet による SQLボトルネック分析結果】")
print("=" * 80)
print()
print(analysis_result)
print()
print("=" * 80)

# 分析結果の保存
result_output_path = 'bottleneck_analysis_result.txt'
with open(result_output_path, 'w', encoding='utf-8') as file:
    file.write("Databricks SQLプロファイラー ボトルネック分析結果\n")
    file.write("=" * 60 + "\n\n")
    file.write(analysis_result)
print(f"\n✓ 分析結果を保存しました: {result_output_path}")

# 最終的なサマリー
print("\n" + "=" * 60)
print("【処理完了サマリー】")
print("=" * 60)
print("✓ SQLプロファイラーJSONファイル読み込み完了")
print("✓ パフォーマンスメトリクス抽出完了 (extracted_metrics.json)")
print("✓ Databricks Claude 3.7 Sonnetによるボトルネック分析完了")
print("✓ 分析結果保存完了 (bottleneck_analysis_result.txt)")
print("=" * 60)

# === セル 13: マークダウンセル - 追加の使用方法 ===
# セルタイプを「Markdown」に変更して以下をコピー:
"""
## 追加の使用方法とカスタマイズ

### ファイルパスの指定例

```python
# ローカルファイル
JSON_FILE_PATH = 'simple0.json'

# DBFS ファイル
JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/user@company.com/profiler.json'
JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/user@company.com/profiler.json'

# S3 マウントファイル（事前にマウント設定が必要）
JSON_FILE_PATH = '/mnt/s3bucket/profiler_logs/query_profile.json'
```

### ファイルアップロード方法

```python
# ローカルファイルをDBFSにアップロード
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# 外部ストレージからのコピー
dbutils.fs.cp("s3a://bucket/profiler.json", "dbfs:/FileStore/profiler.json")
```

### エラー対処方法

1. **Claude エンドポイントエラー**
   - Model Serving でエンドポイント `databricks-claude-3-7-sonnet` が稼働中か確認
   - Personal Access Token の権限を確認

2. **ファイル読み込みエラー**
   - ファイルパスが正しいか確認
   - `dbutils.fs.ls("/FileStore/")` でファイル存在を確認

3. **メモリエラー**
   - 大きなJSONファイルの場合はクラスタのメモリ設定を確認

### 出力ファイルのDBFS保存

```python
# DBFS への保存
dbutils.fs.cp("file:/databricks/driver/extracted_metrics.json", "dbfs:/FileStore/output/")
dbutils.fs.cp("file:/databricks/driver/bottleneck_analysis_result.txt", "dbfs:/FileStore/output/")
```

### カスタマイズポイント

- `extract_performance_metrics` 関数内の重要キーワードリスト
- `analyze_bottlenecks_with_claude` 関数内の分析プロンプト
- メトリクス表示形式とフィルタリング条件
"""

print("=" * 60)
print("Databricks Notebook セル分割版 - 作成完了")
print("=" * 60)
print("上記の各セルをDatabricksのNotebookにコピーペーストして使用してください。")
print("マークダウンセルは、セルタイプを「Markdown」に変更してから貼り付けてください。")