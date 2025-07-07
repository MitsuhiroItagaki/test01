# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQLプロファイラー分析ツール
# MAGIC 
# MAGIC このnotebookは、DatabricksのSQLプロファイラーJSONログファイルを読み込み、ボトルネック特定と改善案の提示に必要なメトリクスを抽出して分析を行います。
# MAGIC 
# MAGIC ## 機能概要
# MAGIC 
# MAGIC 1. **SQLプロファイラーJSONファイルの読み込み**
# MAGIC    - Databricksで出力されたプロファイラーログの解析
# MAGIC    - `graphs`キーに格納された実行プランメトリクスの抽出
# MAGIC 
# MAGIC 2. **重要メトリクスの抽出**
# MAGIC    - クエリ基本情報（ID、ステータス、実行時間など）
# MAGIC    - 全体パフォーマンス（実行時間、データ量、キャッシュ効率など）
# MAGIC    - ステージ・ノード詳細メトリクス
# MAGIC    - ボトルネック指標の計算
# MAGIC 
# MAGIC 3. **AI によるボトルネック分析**
# MAGIC    - Databricks Claude 3.7 Sonnetエンドポイントを使用
# MAGIC    - 抽出メトリクスからボトルネック特定
# MAGIC    - 具体的な改善案の提示
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **事前準備:**
# MAGIC - Databricks Claude 3.7 Sonnetエンドポイントの設定
# MAGIC - SQLプロファイラーJSONファイルの準備（DBFS または FileStore）

# COMMAND ----------

# 🔧 設定: 分析対象のJSONファイルパスを指定
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'  # デフォルト: サンプルファイル

# 以下から選択して変更してください:
# JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler_log.json'
# JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'
# JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'

print("🔧 設定完了")
print(f"📁 分析対象ファイル: {JSON_FILE_PATH}")
print()

# 必要なライブラリのインポート
import json
import pandas as pd
from typing import Dict, List, Any
import requests
from pyspark.sql import SparkSession

# PySpark関数を安全にインポート
try:
    from pyspark.sql.functions import col, lit, when
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
    print("✅ PySpark関数のインポート完了")
except ImportError as e:
    print(f"⚠️ PySpark関数のインポートをスキップ: {e}")
    # 基本的な分析には影響しないためスキップ

# Databricks環境の確認
spark = SparkSession.builder.getOrCreate()
print(f"✅ Spark Version: {spark.version}")

# Databricks Runtime情報を安全に取得
try:
    runtime_version = spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')
    print(f"✅ Databricks Runtime: {runtime_version}")
except Exception:
    try:
        # 代替手段でDBR情報を取得
        dbr_version = spark.conf.get('spark.databricks.clusterUsageTags.clusterName', 'Unknown')
        print(f"✅ Databricks Cluster: {dbr_version}")
    except Exception:
        print("✅ Databricks Environment: 設定情報の取得をスキップしました")

# COMMAND ----------

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
        elif file_path.startswith('/FileStore/'):
            # FileStore パスを /dbfs/FileStore/ に変換
            local_path = '/dbfs' + file_path
            with open(local_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        else:
            # ローカルファイル
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        
        print(f"✅ JSONファイルを正常に読み込みました: {file_path}")
        print(f"📊 データサイズ: {len(str(data)):,} characters")
        return data
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {str(e)}")
        return {}

print("✅ 関数定義完了: load_profiler_json")

# COMMAND ----------

def extract_performance_metrics(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからボトルネック分析に必要なメトリクスを抽出
    """
    metrics = {
        "query_info": {},
        "overall_metrics": {},
        "stage_metrics": [],
        "node_metrics": [],
        "bottleneck_indicators": {},
        "liquid_clustering_analysis": {}
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
                "read_files_count": query_metrics.get('readFilesCount', 0),
                "task_total_time_ms": query_metrics.get('taskTotalTimeMs', 0),
                "photon_total_time_ms": query_metrics.get('photonTotalTimeMs', 0),
                # Photon利用状況の分析
                "photon_enabled": query_metrics.get('photonTotalTimeMs', 0) > 0,
                "photon_utilization_ratio": query_metrics.get('photonTotalTimeMs', 0) / max(query_metrics.get('totalTimeMs', 1), 1)
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
                    "num_complete_tasks": stage.get('numCompleteTasks', 0),
                    "start_time_ms": stage.get('startTimeMs', 0),
                    "end_time_ms": stage.get('endTimeMs', 0)
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
                               ['TIME', 'MEMORY', 'ROWS', 'BYTES', 'DURATION', 'PEAK', 'CUMULATIVE', 'EXCLUSIVE']):
                            detailed_metrics[metric_key] = {
                                'value': metric.get('value', 0),
                                'label': metric.get('label', ''),
                                'type': metric.get('metricType', '')
                            }
                    node_metric['detailed_metrics'] = detailed_metrics
                    metrics["node_metrics"].append(node_metric)
    
    # ボトルネック指標の計算
    metrics["bottleneck_indicators"] = calculate_bottleneck_indicators(metrics)
    
    # Liquid Clustering分析
    metrics["liquid_clustering_analysis"] = analyze_liquid_clustering_opportunities(profiler_data, metrics)
    
    return metrics

print("✅ 関数定義完了: extract_performance_metrics")

# COMMAND ----------

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
    
    # Photon使用率
    task_time = overall.get('task_total_time_ms', 0)
    photon_time = overall.get('photon_total_time_ms', 0)
    if task_time > 0:
        indicators['photon_ratio'] = photon_time / task_time
    
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
    
    # 並列度とシャッフル問題の検出
    shuffle_nodes = []
    low_parallelism_stages = []
    
    # シャッフルノードの特定
    for node in metrics.get('node_metrics', []):
        node_name = node.get('name', '').upper()
        if any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_nodes.append({
                'node_id': node['node_id'],
                'name': node['name'],
                'duration_ms': node.get('key_metrics', {}).get('durationMs', 0),
                'rows': node.get('key_metrics', {}).get('rowsNum', 0)
            })
    
    # 低並列度ステージの検出
    for stage in metrics.get('stage_metrics', []):
        num_tasks = stage.get('num_tasks', 0)
        duration_ms = stage.get('duration_ms', 0)
        
        # 並列度が低い（タスク数が少ない）かつ実行時間が長いステージ
        if num_tasks > 0 and num_tasks < 10 and duration_ms > 5000:  # 10タスク未満、5秒以上
            low_parallelism_stages.append({
                'stage_id': stage['stage_id'],
                'num_tasks': num_tasks,
                'duration_ms': duration_ms,
                'avg_task_duration': duration_ms / max(num_tasks, 1)
            })
    
    indicators['shuffle_operations_count'] = len(shuffle_nodes)
    indicators['low_parallelism_stages_count'] = len(low_parallelism_stages)
    indicators['has_shuffle_bottleneck'] = len(shuffle_nodes) > 0 and any(s['duration_ms'] > 10000 for s in shuffle_nodes)
    indicators['has_low_parallelism'] = len(low_parallelism_stages) > 0
    
    # シャッフルの詳細情報
    if shuffle_nodes:
        total_shuffle_time = sum(s['duration_ms'] for s in shuffle_nodes)
        indicators['total_shuffle_time_ms'] = total_shuffle_time
        indicators['shuffle_time_ratio'] = total_shuffle_time / max(total_time, 1)
        
        # 最も時間のかかるシャッフル操作
        slowest_shuffle = max(shuffle_nodes, key=lambda x: x['duration_ms'])
        indicators['slowest_shuffle_duration_ms'] = slowest_shuffle['duration_ms']
        indicators['slowest_shuffle_node'] = slowest_shuffle['name']
    
    # 低並列度の詳細情報
    if low_parallelism_stages:
        indicators['low_parallelism_details'] = low_parallelism_stages
        avg_parallelism = sum(s['num_tasks'] for s in low_parallelism_stages) / len(low_parallelism_stages)
        indicators['average_low_parallelism'] = avg_parallelism
    
    return indicators

print("✅ 関数定義完了: calculate_bottleneck_indicators")

# COMMAND ----------

def analyze_liquid_clustering_opportunities(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからLiquid Clusteringに効果的なカラムを特定（プロファイラーデータベース分析）
    """
    
    clustering_analysis = {
        "recommended_tables": {},
        "filter_columns": [],
        "join_columns": [],
        "groupby_columns": [],
        "pushdown_filters": [],
        "data_skew_indicators": {},
        "performance_impact": {},
        "detailed_column_analysis": {},
        "summary": {}
    }
    
    print(f"🔍 デバッグ: プロファイラーデータからカラム情報を直接抽出開始")
    
    # プロファイラーデータから実行グラフ情報を取得
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        print("⚠️ グラフデータが見つかりません")
        return clustering_analysis
    
    # 実行プランノードからメタデータを解析
    nodes = graphs[0].get('nodes', []) if graphs else []
    print(f"🔍 デバッグ: {len(nodes)}個のノードを分析中")
    
    # プロファイラーデータのノードからカラム情報を直接抽出
    for node in nodes:
        node_name = node.get('name', '')
        node_tag = node.get('tag', '')
        node_metadata = node.get('metadata', [])
        
        print(f"🔍 ノード分析: {node_name} ({node_tag})")
        
        # メタデータから重要な情報を抽出
        for metadata_item in node_metadata:
            key = metadata_item.get('key', '')
            label = metadata_item.get('label', '')
            values = metadata_item.get('values', [])
            value = metadata_item.get('value', '')
            
            # フィルター条件の抽出
            if key == 'FILTERS' and values:
                for filter_expr in values:
                    clustering_analysis["pushdown_filters"].append({
                        "node_name": node_name,
                        "filter_expression": filter_expr,
                        "metadata_key": key
                    })
                    print(f"   ✅ フィルター抽出: {filter_expr}")
                    
                    # フィルター式からカラム名を抽出
                    if '=' in filter_expr:
                        # "cs_sold_date_sk = 2451659" のような形式からカラム名を抽出
                        parts = filter_expr.split('=')
                        if len(parts) >= 2:
                            column_name = parts[0].strip().replace('(', '').replace(')', '').replace('tpcds.tpcds_sf1000_delta_lc.detail_itagaki.', '')
                            if column_name.endswith('_sk') or column_name.endswith('_date') or column_name.endswith('_id'):
                                clustering_analysis["filter_columns"].append(column_name)
                                print(f"     → フィルターカラム: {column_name}")
            
            # GROUP BY式の抽出
            elif key == 'GROUPING_EXPRESSIONS' and values:
                for group_expr in values:
                    # "tpcds.tpcds_sf1000_delta_lc.detail_itagaki.cs_bill_customer_sk" からカラム名を抽出
                    column_name = group_expr.replace('tpcds.tpcds_sf1000_delta_lc.detail_itagaki.', '')
                    if column_name.endswith('_sk') or column_name.endswith('_date') or column_name.endswith('_id'):
                        clustering_analysis["groupby_columns"].append(column_name)
                        print(f"   ✅ GROUP BYカラム: {column_name}")
            
            # テーブルスキャンの出力列情報
            elif key == 'OUTPUT' and values and 'SCAN' in node_name.upper():
                table_name = value if key == 'SCAN_IDENTIFIER' else f"table_{node.get('id', 'unknown')}"
                for output_col in values:
                    column_name = output_col.split('.')[-1] if '.' in output_col else output_col
                    if column_name.endswith('_sk') or column_name.endswith('_date') or column_name.endswith('_id'):
                        print(f"   📊 出力カラム: {column_name}")
            
            # スキャンテーブル情報の抽出
            elif key == 'SCAN_IDENTIFIER':
                table_name = value
                print(f"   🏷️ テーブル識別子: {table_name}")
    
    # ノードメトリクスからの補完分析
    node_metrics = metrics.get('node_metrics', [])
    table_scan_nodes = []
    join_nodes = []
    shuffle_nodes = []
    filter_nodes = []
    
    for node in node_metrics:
        node_name = node.get('name', '').upper()
        node_tag = node.get('tag', '').upper()
        detailed_metrics = node.get('detailed_metrics', {})
        
        # 補完的メトリクス分析（プロファイラーデータの補完）
        for metric_key, metric_info in detailed_metrics.items():
            metric_label = metric_info.get('label', '')
            metric_value = metric_info.get('value', '')
            
            # フィルター条件の補完抽出
            if any(filter_keyword in metric_key.upper() for filter_keyword in ['FILTER', 'PREDICATE', 'CONDITION']):
                if metric_label or metric_value:
                    clustering_analysis["pushdown_filters"].append({
                        "node_id": node.get('node_id', ''),
                        "node_name": node_name,
                        "filter_expression": metric_label or str(metric_value),
                        "metric_key": metric_key
                    })
        
        # テーブルスキャンノードの補完分析
        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            table_scan_nodes.append(node)
            
            # データスキュー指標の計算
            key_metrics = node.get('key_metrics', {})
            rows_num = key_metrics.get('rowsNum', 0)
            duration_ms = key_metrics.get('durationMs', 0)
            
            # シンプルなテーブル名抽出
            table_name = f"table_{node.get('node_id', 'unknown')}"
            
            # ノード名から単語を抽出してテーブル名らしいものを検索
            words = node_name.replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ').split()
            for word in words:
                if '.' in word and len(word.split('.')) >= 2:
                    table_name = word.lower()
                    break
                elif not word.isupper() and len(word) > 5 and '_' in word:
                    table_name = word.lower()
                    break
            
            # スキューデータ指標の記録
            clustering_analysis["data_skew_indicators"][table_name] = {
                "rows_scanned": rows_num,
                "scan_duration_ms": duration_ms,
                "avg_rows_per_ms": rows_num / max(duration_ms, 1),
                "node_name": node_name,
                "node_id": node.get('node_id', '')
            }
        
        # フィルターノードの特定
        elif any(keyword in node_name for keyword in ['FILTER']):
            filter_nodes.append(node)
        
        # JOINノードの補完分析
        elif any(keyword in node_name for keyword in ['JOIN', 'HASH']):
            join_nodes.append(node)
        
        # Shuffleノードの特定
        elif any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_nodes.append(node)
    
    # プロファイラーデータからの抽出結果を整理
    print(f"\n🔍 デバッグ: プロファイラーデータから抽出されたカラム一覧")
    print(f"   フィルターカラム: {clustering_analysis['filter_columns']}")
    print(f"   JOINカラム: {clustering_analysis['join_columns']}")
    print(f"   GROUP BYカラム: {clustering_analysis['groupby_columns']}")
    
    # 重複除去
    clustering_analysis["filter_columns"] = list(set(clustering_analysis["filter_columns"]))
    clustering_analysis["join_columns"] = list(set(clustering_analysis["join_columns"]))
    clustering_analysis["groupby_columns"] = list(set(clustering_analysis["groupby_columns"]))
    
    # すべてのカラムを収集
    all_columns = set()
    all_columns.update(clustering_analysis["filter_columns"])
    all_columns.update(clustering_analysis["join_columns"])
    all_columns.update(clustering_analysis["groupby_columns"])
    
    print(f"\n🔍 デバッグ: 最終的な有効カラム数: {len(all_columns)}")
    print(f"   有効カラム: {list(all_columns)}")
    
    # カラム別の詳細分析
    for column in all_columns:
        column_analysis = {
            "filter_usage_count": clustering_analysis["filter_columns"].count(column),
            "join_usage_count": clustering_analysis["join_columns"].count(column),
            "groupby_usage_count": clustering_analysis["groupby_columns"].count(column),
            "total_usage": 0,
            "usage_contexts": [],
            "associated_tables": set(),
            "performance_impact": "low"
        }
        
        # 使用回数の合計計算
        column_analysis["total_usage"] = (
            column_analysis["filter_usage_count"] * 3 +
            column_analysis["join_usage_count"] * 2 +
            column_analysis["groupby_usage_count"] * 1
        )
        
        # 使用コンテキストの記録
        if column_analysis["filter_usage_count"] > 0:
            column_analysis["usage_contexts"].append("WHERE/Filter条件")
        if column_analysis["join_usage_count"] > 0:
            column_analysis["usage_contexts"].append("JOIN条件")
        if column_analysis["groupby_usage_count"] > 0:
            column_analysis["usage_contexts"].append("GROUP BY")
        
        # 関連テーブルの特定
        column_parts = column.split('.')
        if len(column_parts) >= 2:
            if len(column_parts) == 3:  # schema.table.column
                table_name = f"{column_parts[0]}.{column_parts[1]}"
                column_analysis["associated_tables"].add(table_name)
            else:  # table.column
                column_analysis["associated_tables"].add(column_parts[0])
        
        # パフォーマンス影響度の評価
        if column_analysis["total_usage"] >= 6:
            column_analysis["performance_impact"] = "high"
        elif column_analysis["total_usage"] >= 3:
            column_analysis["performance_impact"] = "medium"
        
        # set型をlist型に変換
        column_analysis["associated_tables"] = list(column_analysis["associated_tables"])
        
        clustering_analysis["detailed_column_analysis"][column] = column_analysis
    
    # テーブル毎の推奨事項（プロファイラーベース）
    for table_name, skew_info in clustering_analysis["data_skew_indicators"].items():
        print(f"\n🔍 デバッグ: テーブル {table_name} の推奨カラム分析")
        
        # カラムの重要度スコア計算（簡潔版）
        column_scores = {}
        for col in all_columns:
            # フィルター、JOIN、GROUP BYでの使用頻度ベーススコア
            score = (clustering_analysis["filter_columns"].count(col) * 3 +
                    clustering_analysis["join_columns"].count(col) * 2 +
                    clustering_analysis["groupby_columns"].count(col) * 1)
            
            if score > 0:
                clean_col = col.split('.')[-1] if '.' in col else col
                column_scores[clean_col] = score
        
        # 上位カラムを推奨
        if column_scores:
            sorted_columns = sorted(column_scores.items(), key=lambda x: x[1], reverse=True)
            recommended_cols = [col for col, score in sorted_columns[:4]]  # 最大4カラム
            
            print(f"   📊 カラムスコア: {dict(sorted_columns)}")
            print(f"   🏆 推奨カラム: {recommended_cols}")
            
            clustering_analysis["recommended_tables"][table_name] = {
                "clustering_columns": recommended_cols,
                "column_scores": column_scores,
                "scan_performance": {
                    "rows_scanned": skew_info["rows_scanned"],
                    "scan_duration_ms": skew_info["scan_duration_ms"],
                    "efficiency_score": skew_info["avg_rows_per_ms"]
                },
                "node_details": {
                    "node_id": skew_info.get("node_id", ""),
                    "node_name": skew_info.get("node_name", "")
                }
            }
        else:
            print(f"   ⚠️ 有効なカラムスコアが見つかりませんでした")
    
    # パフォーマンス向上の見込み評価
    total_scan_time = sum(info["scan_duration_ms"] for info in clustering_analysis["data_skew_indicators"].values())
    total_shuffle_time = 0
    
    for node in shuffle_nodes:
        total_shuffle_time += node.get('key_metrics', {}).get('durationMs', 0)
    
    clustering_analysis["performance_impact"] = {
        "total_scan_time_ms": total_scan_time,
        "total_shuffle_time_ms": total_shuffle_time,
        "potential_scan_improvement": "30-70%" if total_scan_time > 10000 else "10-30%",
        "potential_shuffle_reduction": "20-50%" if total_shuffle_time > 5000 else "5-20%",
        "estimated_overall_improvement": "25-60%" if (total_scan_time + total_shuffle_time) > 15000 else "10-25%"
    }
    
    # サマリー情報（プロファイラーベース）
    clustering_analysis["summary"] = {
        "tables_identified": len(clustering_analysis["recommended_tables"]),
        "total_filter_columns": len(clustering_analysis["filter_columns"]),
        "total_join_columns": len(clustering_analysis["join_columns"]),
        "total_groupby_columns": len(clustering_analysis["groupby_columns"]),
        "high_impact_tables": len([t for t, info in clustering_analysis["recommended_tables"].items() 
                                 if info["scan_performance"]["scan_duration_ms"] > 5000]),
        "unique_filter_columns": clustering_analysis["filter_columns"],
        "unique_join_columns": clustering_analysis["join_columns"],
        "unique_groupby_columns": clustering_analysis["groupby_columns"],
        "profiler_data_source": "graphs_metadata"
    }
    
    return clustering_analysis

print("✅ 関数定義完了: analyze_liquid_clustering_opportunities")

# COMMAND ----------

def analyze_bottlenecks_with_claude(metrics: Dict[str, Any]) -> str:
    """
    Databricks Claude 3.7 Sonnetエンドポイントを使用してボトルネック分析を行う
    """
    
    # メトリクス要約の準備（簡潔版）
    # 主要なメトリクスのみを抽出してリクエストサイズを削減
    total_time_sec = metrics['overall_metrics'].get('total_time_ms', 0) / 1000
    read_gb = metrics['overall_metrics'].get('read_bytes', 0) / 1024 / 1024 / 1024
    cache_ratio = metrics['bottleneck_indicators'].get('cache_hit_ratio', 0) * 100
    data_selectivity = metrics['bottleneck_indicators'].get('data_selectivity', 0) * 100
    
    # Liquid Clustering推奨テーブル（上位3テーブルのみ）
    top_tables = list(metrics['liquid_clustering_analysis']['recommended_tables'].items())[:3]
    table_recommendations = [f"- {table}: {', '.join(info['clustering_columns'])}" for table, info in top_tables]
    
    # 高インパクトカラム（上位5個のみ）
    high_impact_cols = [(col, analysis) for col, analysis in metrics['liquid_clustering_analysis']['detailed_column_analysis'].items() 
                       if analysis.get('performance_impact') == 'high'][:5]
    high_impact_summary = [f"- {col}: スコア={analysis['total_usage']}, 使用箇所=[{', '.join(analysis['usage_contexts'])}]" 
                          for col, analysis in high_impact_cols]
    
    # Photonと並列度の情報を追加
    photon_enabled = metrics['overall_metrics'].get('photon_enabled', False)
    photon_utilization = metrics['overall_metrics'].get('photon_utilization_ratio', 0) * 100
    shuffle_count = metrics['bottleneck_indicators'].get('shuffle_operations_count', 0)
    has_shuffle_bottleneck = metrics['bottleneck_indicators'].get('has_shuffle_bottleneck', False)
    has_low_parallelism = metrics['bottleneck_indicators'].get('has_low_parallelism', False)
    low_parallelism_count = metrics['bottleneck_indicators'].get('low_parallelism_stages_count', 0)
    
    analysis_prompt = f"""
あなたはDatabricksのSQLパフォーマンス分析の専門家です。以下のメトリクスを分析し、ボトルネックを特定して改善案を提示してください。

【パフォーマンス概要】
- 実行時間: {total_time_sec:.1f}秒
- 読み込みデータ: {read_gb:.1f}GB
- キャッシュ効率: {cache_ratio:.1f}%
- データ選択性: {data_selectivity:.1f}%
- スピル発生: {'あり' if metrics['bottleneck_indicators'].get('has_spill', False) else 'なし'}

【Photonエンジン分析】
- Photon有効: {'はい' if photon_enabled else 'いいえ'}
- Photon利用率: {photon_utilization:.1f}%
- Photon推奨: {'既に最適化済み' if photon_utilization > 80 else 'Photon有効化を推奨' if not photon_enabled else 'Photon利用率向上が必要'}

【並列度・シャッフル分析】
- シャッフル操作: {shuffle_count}回
- シャッフルボトルネック: {'あり' if has_shuffle_bottleneck else 'なし'}
- 低並列度ステージ: {low_parallelism_count}個
- 並列度問題: {'あり' if has_low_parallelism else 'なし'}

【Liquid Clustering推奨】
テーブル数: {metrics['liquid_clustering_analysis']['summary'].get('tables_identified', 0)}個
推奨カラム:
{chr(10).join(table_recommendations)}

高インパクトカラム:
{chr(10).join(high_impact_summary)}

【重要指標】
- 最遅ステージ: {metrics['bottleneck_indicators'].get('slowest_stage_id', 'N/A')}
- 最高メモリ: {metrics['bottleneck_indicators'].get('highest_memory_bytes', 0)/1024/1024:.0f}MB
- Photon使用率: {metrics['bottleneck_indicators'].get('photon_ratio', 0)*100:.0f}%

【求める分析】
1. 主要ボトルネックと原因（Photon、並列度、シャッフルに焦点）
2. Liquid Clustering実装の優先順位と手順（パーティショニング・ZORDER以外）
3. 各推奨カラムの選定理由と効果
4. Photonエンジンの最適化案
5. 並列度・シャッフル最適化案
6. パフォーマンス改善見込み

**重要**: パーティショニングやZORDERは提案せず、Liquid Clusteringのみを推奨してください。
簡潔で実践的な改善提案を日本語で提供してください。
"""
    
    try:
        # Databricks Model Serving APIを使用
        try:
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception:
            # 代替手段でトークンを取得
            import os
            token = os.environ.get('DATABRICKS_TOKEN')
            if not token:
                return "❌ Databricksトークンの取得に失敗しました。環境変数DATABRICKS_TOKENを設定するか、dbutils.secrets.get()を使用してください。"
        
        try:
            workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
        
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
            "max_tokens": 2000,
            "temperature": 0.1
        }
        
        print("🤖 Databricks Claude 3.7 Sonnetエンドポイントに分析リクエストを送信中...")
        print("⏳ 大きなデータのため処理に時間がかかる場合があります...")
        
        # リトライ機能を追加（最大2回試行）
        max_retries = 2
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"🔄 リトライ中... (試行 {attempt + 1}/{max_retries})")
                
                response = requests.post(endpoint_url, headers=headers, json=payload, timeout=180)
                
                if response.status_code == 200:
                    result = response.json()
                    analysis_text = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                    print("✅ ボトルネック分析が完了しました")
                    return analysis_text
                else:
                    error_msg = f"APIエラー: ステータスコード {response.status_code}"
                    if attempt == max_retries - 1:  # 最後の試行
                        print(f"❌ {error_msg}\nレスポンス: {response.text}")
                        return error_msg
                    else:
                        print(f"⚠️ {error_msg} - リトライします...")
                        continue
                        
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:  # 最後の試行
                    timeout_msg = "⏰ タイムアウトエラー: Claude 3.7 Sonnetの応答が180秒以内に完了しませんでした。\n\n【代替案】\n1. Model Servingエンドポイントの設定を確認\n2. より小さなデータセットで再試行\n3. エンドポイントの負荷状況を確認"
                    print(f"❌ {timeout_msg}")
                    return timeout_msg
                else:
                    print(f"⏰ タイムアウト発生 - リトライします... (試行 {attempt + 1}/{max_retries})")
                    continue
            
    except Exception as e:
        error_msg = f"分析エラー: {str(e)}"
        print(f"❌ {error_msg}")
        
        # フォールバック: 基本的な分析結果を提供
        fallback_analysis = f"""
🔧 **基本的なボトルネック分析結果** (Claude 3.7 Sonnet利用不可のため簡易版)

## 📊 パフォーマンス概要
- **実行時間**: {total_time_sec:.1f}秒
- **読み込みデータ量**: {read_gb:.1f}GB  
- **キャッシュ効率**: {cache_ratio:.1f}%
- **データ選択性**: {data_selectivity:.1f}%

## ⚡ Photonエンジン分析
- **Photon有効**: {'はい' if photon_enabled else 'いいえ'}
- **Photon利用率**: {photon_utilization:.1f}%
- **推奨**: {'Photon利用率向上が必要' if photon_utilization < 80 else '最適化済み'}

## � 並列度・シャッフル分析
- **シャッフル操作**: {shuffle_count}回
- **シャッフルボトルネック**: {'あり' if has_shuffle_bottleneck else 'なし'}
- **低並列度ステージ**: {low_parallelism_count}個
- **並列度問題**: {'あり' if has_low_parallelism else 'なし'}

## �️ Liquid Clustering推奨事項
**対象テーブル数**: {metrics['liquid_clustering_analysis']['summary'].get('tables_identified', 0)}個

**推奨実装**:
{chr(10).join(table_recommendations) if table_recommendations else '- 推奨カラムが見つかりませんでした'}

## ⚠️ 主要な問題点
- {'メモリスピルが発生しています' if metrics['bottleneck_indicators'].get('has_spill', False) else 'メモリ使用は正常です'}
- {'キャッシュ効率が低下しています' if cache_ratio < 50 else 'キャッシュ効率は良好です'}
- {'データ選択性が低く、大量のデータを読み込んでいます' if data_selectivity < 10 else 'データ選択性は適切です'}
- {'Photonエンジンが無効または利用率が低い' if not photon_enabled or photon_utilization < 50 else 'Photon利用は良好'}
- {'シャッフルボトルネックが発生' if has_shuffle_bottleneck else 'シャッフル処理は正常'}
- {'並列度が低いステージが存在' if has_low_parallelism else '並列度は適切'}

## 🚀 推奨アクション
1. **Liquid Clustering実装**: 上記推奨カラムでテーブルをクラスタリング（パーティショニング・ZORDER不使用）
2. **Photon有効化**: {'Photonエンジンを有効にする' if not photon_enabled else 'Photon設定を最適化'}
3. **並列度最適化**: {'クラスターサイズ・並列度設定を見直し' if has_low_parallelism else '現在の並列度は適切'}
4. **シャッフル最適化**: {'JOIN順序・GROUP BY最適化でシャッフル削減' if has_shuffle_bottleneck else 'シャッフル処理は最適'}
5. **クエリ最適化**: WHERE句の条件を適切に設定
6. **キャッシュ活用**: よく使用されるテーブルのキャッシュを検討

**重要**: パーティショニングやZORDERは使用せず、Liquid Clusteringのみで最適化してください。

**注意**: Claude 3.7 Sonnetエンドポイントの接続に問題があります。詳細な分析は手動で実施してください。
        """
        return fallback_analysis

print("✅ 関数定義完了: analyze_bottlenecks_with_claude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## メイン処理の実行
# MAGIC 
# MAGIC 以下のセルを順番に実行して、SQLプロファイラー分析を実行します。
# MAGIC 
# MAGIC ### 設定について
# MAGIC 
# MAGIC ファイルパスの設定は**セル2**で行います：
# MAGIC 
# MAGIC ```python
# MAGIC JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'
# MAGIC ```
# MAGIC 
# MAGIC **対応するファイルパス形式:**
# MAGIC - Unity Catalog Volumes: `/Volumes/catalog/schema/volume/file.json`
# MAGIC - FileStore: `/FileStore/shared_uploads/username/profiler.json`
# MAGIC - DBFS: `/dbfs/FileStore/shared_uploads/username/profiler.json`
# MAGIC - DBFS URI: `dbfs:/FileStore/shared_uploads/username/profiler.json`

# COMMAND ----------

print("=" * 80)
print("🚀 Databricks SQLプロファイラー分析ツール")
print("=" * 80)
print(f"📁 分析対象ファイル: {JSON_FILE_PATH}")
print()

# SQLプロファイラーJSONファイルの読み込み
profiler_data = load_profiler_json(JSON_FILE_PATH)
if not profiler_data:
    print("❌ JSONファイルの読み込みに失敗しました。ファイルパスを確認してください。")
    print("⚠️ 処理を停止します。")
    # dbutils.notebook.exit("File loading failed")  # 安全のためコメントアウト
    raise RuntimeError("JSONファイルの読み込みに失敗しました。")

print(f"✅ データ読み込み完了")
print()

# COMMAND ----------

# 📊 パフォーマンスメトリクスの抽出
extracted_metrics = extract_performance_metrics(profiler_data)
print("✅ パフォーマンスメトリクスを抽出しました")

# 抽出されたメトリクスの概要表示
print("\n" + "=" * 50)
print("📈 抽出されたメトリクス概要")
print("=" * 50)

query_info = extracted_metrics['query_info']
overall_metrics = extracted_metrics['overall_metrics']
bottleneck_indicators = extracted_metrics['bottleneck_indicators']

print(f"🆔 クエリID: {query_info['query_id']}")
print(f"📊 ステータス: {query_info['status']}")
print(f"👤 実行ユーザー: {query_info['user']}")
print(f"⏱️ 実行時間: {overall_metrics['total_time_ms']:,} ms ({overall_metrics['total_time_ms']/1000:.2f} sec)")
print(f"💾 読み込みデータ: {overall_metrics['read_bytes']/1024/1024/1024:.2f} GB")
print(f"📈 出力行数: {overall_metrics['rows_produced_count']:,} 行")
print(f"📉 読み込み行数: {overall_metrics['rows_read_count']:,} 行")
print(f"🎯 データ選択性: {bottleneck_indicators.get('data_selectivity', 0):.4f} ({bottleneck_indicators.get('data_selectivity', 0)*100:.2f}%)")
print(f"🔧 ステージ数: {len(extracted_metrics['stage_metrics'])}")
print(f"🏗️ ノード数: {len(extracted_metrics['node_metrics'])}")

# Liquid Clustering分析結果の表示
liquid_analysis = extracted_metrics['liquid_clustering_analysis']
liquid_summary = liquid_analysis.get('summary', {})
print(f"🗂️ Liquid Clustering対象テーブル数: {liquid_summary.get('tables_identified', 0)}")
print(f"📊 高インパクトテーブル数: {liquid_summary.get('high_impact_tables', 0)}")

# COMMAND ----------

# 📋 ボトルネック指標の詳細表示
print("\n" + "=" * 50)
print("🔍 ボトルネック指標詳細")
print("=" * 50)

# Photon関連指標
photon_enabled = overall_metrics.get('photon_enabled', False)
photon_utilization = overall_metrics.get('photon_utilization_ratio', 0) * 100
photon_emoji = "✅" if photon_enabled and photon_utilization > 80 else "⚠️" if photon_enabled else "❌"
print(f"{photon_emoji} Photonエンジン: {'有効' if photon_enabled else '無効'} (利用率: {photon_utilization:.1f}%)")

# 並列度・シャッフル関連指標
shuffle_count = bottleneck_indicators.get('shuffle_operations_count', 0)
has_shuffle_bottleneck = bottleneck_indicators.get('has_shuffle_bottleneck', False)
has_low_parallelism = bottleneck_indicators.get('has_low_parallelism', False)
low_parallelism_count = bottleneck_indicators.get('low_parallelism_stages_count', 0)

shuffle_emoji = "🚨" if has_shuffle_bottleneck else "⚠️" if shuffle_count > 5 else "✅"
print(f"{shuffle_emoji} シャッフル操作: {shuffle_count}回 ({'ボトルネックあり' if has_shuffle_bottleneck else '正常'})")

parallelism_emoji = "🚨" if has_low_parallelism else "✅"
print(f"{parallelism_emoji} 並列度: {'問題あり' if has_low_parallelism else '適切'} (低並列度ステージ: {low_parallelism_count}個)")

print()
print("📊 その他の指標:")

for key, value in bottleneck_indicators.items():
    # 新しく追加した指標は上記で表示済みなのでスキップ
    if key in ['shuffle_operations_count', 'has_shuffle_bottleneck', 'has_low_parallelism', 
               'low_parallelism_stages_count', 'total_shuffle_time_ms', 'shuffle_time_ratio',
               'slowest_shuffle_duration_ms', 'slowest_shuffle_node', 'low_parallelism_details',
               'average_low_parallelism']:
        continue
        
    if 'ratio' in key:
        emoji = "📊" if value < 0.1 else "⚠️" if value < 0.3 else "🚨"
        print(f"{emoji} {key}: {value:.3f} ({value*100:.1f}%)")
    elif 'bytes' in key and key != 'has_spill':
        if value > 0:
            emoji = "💾" if value < 1024*1024*1024 else "⚠️"  # 1GB未満は普通、以上は注意
            print(f"{emoji} {key}: {value:,} bytes ({value/1024/1024:.2f} MB)")
    elif key == 'has_spill':
        emoji = "❌" if not value else "⚠️"
        print(f"{emoji} {key}: {'あり' if value else 'なし'}")
    elif 'duration' in key:
        emoji = "⏱️"
        print(f"{emoji} {key}: {value:,} ms ({value/1000:.2f} sec)")
    else:
        emoji = "ℹ️"
        print(f"{emoji} {key}: {value}")

print()

# COMMAND ----------

# 💾 抽出したメトリクスをJSONファイルとして保存
def convert_sets_to_lists(obj):
    """set型をlist型に変換してJSONシリアライズ可能にする"""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {key: convert_sets_to_lists(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(item) for item in obj]
    else:
        return obj

output_path = 'extracted_metrics.json'
try:
    # set型をlist型に変換してからJSONに保存
    serializable_metrics = convert_sets_to_lists(extracted_metrics)
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(serializable_metrics, file, indent=2, ensure_ascii=False)
    print(f"✅ 抽出メトリクスを保存しました: {output_path}")
except Exception as e:
    print(f"⚠️ メトリクス保存でエラーが発生しましたがスキップします: {e}")
    print("✅ 分析は正常に継続されます")

# SparkDataFrameとしても表示
if extracted_metrics['stage_metrics']:
    print("\n🎭 ステージメトリクス (DataFrame)")
    print("=" * 40)
    try:
        stage_df = spark.createDataFrame(extracted_metrics['stage_metrics'])
        stage_df.show(truncate=False)
    except Exception as e:
        print(f"⚠️ SparkDataFrame表示をスキップ: {e}")
        # 代替としてPandasで表示
        import pandas as pd
        stage_pd_df = pd.DataFrame(extracted_metrics['stage_metrics'])
        print(stage_pd_df.to_string(index=False))

# 🐌 最も時間がかかっている処理TOP10
print(f"\n🐌 最も時間がかかっている処理TOP10")
print("=" * 80)
print("📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー")

# ノードを実行時間でソート
sorted_nodes = sorted(extracted_metrics['node_metrics'], 
                     key=lambda x: x['key_metrics'].get('durationMs', 0), 
                     reverse=True)

if sorted_nodes:
    # 全体の実行時間を計算
    total_duration = sum(node['key_metrics'].get('durationMs', 0) for node in sorted_nodes)
    
    print(f"📊 全体実行時間: {total_duration:,} ms ({total_duration/1000:.1f} sec)")
    print(f"📈 TOP10合計時間: {sum(node['key_metrics'].get('durationMs', 0) for node in sorted_nodes[:10]):,} ms")
    print()
    
    for i, node in enumerate(sorted_nodes[:10]):
        rows_num = node['key_metrics'].get('rowsNum', 0)
        duration_ms = node['key_metrics'].get('durationMs', 0)
        memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
        
        # 全体に対する時間の割合を計算
        time_percentage = (duration_ms / max(total_duration, 1)) * 100
        
        # 時間の重要度に基づいてアイコンを選択
        if duration_ms >= 10000:  # 10秒以上
            time_icon = "�"
            severity = "CRITICAL"
        elif duration_ms >= 5000:  # 5秒以上
            time_icon = "🟠"
            severity = "HIGH"
        elif duration_ms >= 1000:  # 1秒以上
            time_icon = "🟡"
            severity = "MEDIUM"
        else:
            time_icon = "�"
            severity = "LOW"
        
        # メモリ使用量のアイコン
        memory_icon = "�" if memory_mb < 100 else "⚠️" if memory_mb < 1000 else "🚨"
        
        # ノード名を短縮（100バイトまで）
        node_name = node['name']
        short_name = node_name[:100] + "..." if len(node_name) > 100 else node_name
        
        # 並列度情報の取得
        num_tasks = 0
        for stage in extracted_metrics.get('stage_metrics', []):
            if duration_ms > 0:  # このノードに関連するステージを推定
                num_tasks = max(num_tasks, stage.get('num_tasks', 0))
        
        # ディスクスピルアウトの検出
        detailed_metrics = node.get('detailed_metrics', {})
        spill_detected = False
        spill_bytes = 0
        for metric_key, metric_info in detailed_metrics.items():
            if 'SPILL' in metric_key.upper() or 'DISK' in metric_key.upper():
                metric_value = metric_info.get('value', 0)
                if metric_value > 0:
                    spill_detected = True
                    spill_bytes += metric_value
        
        # データスキューの検出（行数とメモリ使用量から推定）
        skew_detected = False
        if rows_num > 0 and memory_mb > 0:
            # メモリ使用量が行数に比べて異常に高い場合はスキューの可能性
            memory_per_row = memory_mb * 1024 * 1024 / rows_num  # bytes per row
            if memory_per_row > 10000:  # 1行あたり10KB以上は高い
                skew_detected = True
        
        # または実行時間が行数に比べて異常に長い場合
        if rows_num > 0 and duration_ms > 0:
            ms_per_thousand_rows = (duration_ms * 1000) / rows_num
            if ms_per_thousand_rows > 1000:  # 1000行あたり1秒以上は遅い
                skew_detected = True
        
        # 並列度アイコン
        parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
        # スピルアイコン
        spill_icon = "💿" if spill_detected else "✅"
        # スキューアイコン
        skew_icon = "⚖️" if skew_detected else "✅"
        
        print(f"{i+1:2d}. {time_icon}{memory_icon}{parallelism_icon}{spill_icon}{skew_icon} [{severity:8}] {short_name}")
        print(f"    ⏱️  実行時間: {duration_ms:>8,} ms ({duration_ms/1000:>6.1f} sec) - 全体の {time_percentage:>5.1f}%")
        print(f"    📊 処理行数: {rows_num:>8,} 行")
        print(f"    💾 ピークメモリ: {memory_mb:>6.1f} MB")
        print(f"    🔧 並列度: {num_tasks:>3d} タスク | 💿 スピル: {'あり' if spill_detected else 'なし'} | ⚖️ スキュー: {'あり' if skew_detected else 'なし'}")
        
        # 効率性指標（行/秒）を計算
        if duration_ms > 0:
            rows_per_sec = (rows_num * 1000) / duration_ms
            print(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒")
        
        # スピル詳細情報
        if spill_detected and spill_bytes > 0:
            print(f"    💿 スピル詳細: {spill_bytes/1024/1024:.1f} MB")
        
        # ノードIDも表示
        print(f"    🆔 ノードID: {node.get('node_id', 'N/A')}")
        print()
        
else:
    print("⚠️ ノードメトリクスが見つかりませんでした")

print()

# COMMAND ----------

# 🗂️ Liquid Clustering分析結果の詳細表示
print("\n" + "=" * 50)
print("🗂️ Liquid Clustering推奨分析")
print("=" * 50)

liquid_analysis = extracted_metrics['liquid_clustering_analysis']

# 推奨テーブル一覧
recommended_tables = liquid_analysis.get('recommended_tables', {})
if recommended_tables:
    print("\n📋 テーブル別推奨クラスタリングカラム:")
    for table_name, table_info in recommended_tables.items():
        clustering_cols = table_info.get('clustering_columns', [])
        scan_perf = table_info.get('scan_performance', {})
        
        # テーブル名の表示
        print(f"\n📊 テーブル: {table_name}")
        print(f"   🎯 推奨クラスタリングカラム: {', '.join(clustering_cols)}")
        print(f"   📈 スキャン行数: {scan_perf.get('rows_scanned', 0):,} 行")
        print(f"   ⏱️ スキャン時間: {scan_perf.get('scan_duration_ms', 0):,} ms")
        print(f"   🚀 スキャン効率: {scan_perf.get('efficiency_score', 0):.2f} 行/ms")
        
        # カラムスコア詳細
        column_scores = table_info.get('column_scores', {})
        if column_scores:
            sorted_scores = sorted(column_scores.items(), key=lambda x: x[1], reverse=True)
            print(f"   📊 カラム重要度: {', '.join([f'{col}({score})' for col, score in sorted_scores[:3]])}")

# パフォーマンス影響分析
performance_impact = liquid_analysis.get('performance_impact', {})
print(f"\n🔄 パフォーマンス向上見込み:")
print(f"   📈 スキャン改善: {performance_impact.get('potential_scan_improvement', 'N/A')}")
print(f"   🔀 Shuffle削減: {performance_impact.get('potential_shuffle_reduction', 'N/A')}")
print(f"   🏆 全体改善: {performance_impact.get('estimated_overall_improvement', 'N/A')}")

# カラム使用統計（詳細版）
filter_cols = set(liquid_analysis.get('filter_columns', []))
join_cols = set(liquid_analysis.get('join_columns', []))
groupby_cols = set(liquid_analysis.get('groupby_columns', []))
detailed_column_analysis = liquid_analysis.get('detailed_column_analysis', {})

if filter_cols or join_cols or groupby_cols:
    print(f"\n🔍 カラム使用パターン:")
    if filter_cols:
        print(f"   🔎 フィルターカラム ({len(filter_cols)}個): {', '.join(list(filter_cols)[:5])}")
    if join_cols:
        print(f"   🔗 JOINカラム ({len(join_cols)}個): {', '.join(list(join_cols)[:5])}")
    if groupby_cols:
        print(f"   📊 GROUP BYカラム ({len(groupby_cols)}個): {', '.join(list(groupby_cols)[:5])}")

# 高インパクトカラムの詳細表示
high_impact_columns = {col: analysis for col, analysis in detailed_column_analysis.items() 
                      if analysis.get('performance_impact') == 'high'}

if high_impact_columns:
    print(f"\n⭐ 高インパクトカラム詳細:")
    for col, analysis in list(high_impact_columns.items())[:5]:
        usage_contexts = ', '.join(analysis.get('usage_contexts', []))
        total_usage = analysis.get('total_usage', 0)
        print(f"   🎯 {col}")
        print(f"      📈 重要度スコア: {total_usage} | 使用箇所: {usage_contexts}")
        print(f"      📊 フィルター:{analysis.get('filter_usage_count', 0)} | JOIN:{analysis.get('join_usage_count', 0)} | GROUP BY:{analysis.get('groupby_usage_count', 0)}")

# プッシュダウンフィルター情報
pushdown_filters = liquid_analysis.get('pushdown_filters', [])
if pushdown_filters:
    print(f"\n🔍 プッシュダウンフィルター ({len(pushdown_filters)}件):")
    for i, filter_info in enumerate(pushdown_filters[:3]):
        node_name_display = filter_info.get('node_name', '')
        truncated_node_name = node_name_display[:100] + "..." if len(node_name_display) > 100 else node_name_display
        print(f"   {i+1}. ノード: {truncated_node_name}")
        filter_expression = filter_info.get('filter_expression', '')
        truncated_filter = filter_expression[:128] + "..." if len(filter_expression) > 128 else filter_expression
        print(f"      📋 フィルター: {truncated_filter}")
        print(f"      🔧 メトリクス: {filter_info.get('metric_key', '')}")

# SQL実装例
if recommended_tables:
    print(f"\n💡 実装例:")
    for table_name, table_info in list(recommended_tables.items())[:2]:  # 上位2テーブル
        clustering_cols = table_info.get('clustering_columns', [])
        if clustering_cols:
            cluster_by_clause = ', '.join(clustering_cols)
            print(f"   ALTER TABLE {table_name} CLUSTER BY ({cluster_by_clause});")

print()

# COMMAND ----------

# 🤖 Databricks Claude 3.7 Sonnetを使用してボトルネック分析
print("🤖 Claude 3.7 Sonnetによるボトルネック分析を開始します...")
print("⚠️  Model Servingエンドポイント 'databricks-claude-3-7-sonnet' が必要です")
print("📝 分析プロンプトを簡潔化してタイムアウトリスクを軽減しています...")
print()

analysis_result = analyze_bottlenecks_with_claude(extracted_metrics)

# COMMAND ----------

# 📊 分析結果の表示
print("\n" + "=" * 80)
print("🎯 【Databricks Claude 3.7 Sonnet による SQLボトルネック分析結果】")
print("=" * 80)
print()
print(analysis_result)
print()
print("=" * 80)

# COMMAND ----------

# 💾 分析結果の保存と完了サマリー
result_output_path = 'bottleneck_analysis_result.txt'
with open(result_output_path, 'w', encoding='utf-8') as file:
    file.write("Databricks SQLプロファイラー ボトルネック分析結果\n")
    file.write("=" * 60 + "\n\n")
    file.write(f"クエリID: {extracted_metrics['query_info']['query_id']}\n")
    file.write(f"分析日時: {pd.Timestamp.now()}\n")
    file.write(f"実行時間: {extracted_metrics['overall_metrics']['total_time_ms']:,} ms\n")
    file.write("=" * 60 + "\n\n")
    file.write(analysis_result)
print(f"✅ 分析結果を保存しました: {result_output_path}")

# 最終的なサマリー
print("\n" + "🎉" * 20)
print("🏁 【処理完了サマリー】")
print("🎉" * 20)
print("✅ SQLプロファイラーJSONファイル読み込み完了")
print("✅ パフォーマンスメトリクス抽出完了 (extracted_metrics.json)")
print("✅ Databricks Claude 3.7 Sonnetによるボトルネック分析完了")
print("✅ 分析結果保存完了 (bottleneck_analysis_result.txt)")
print()
print("📁 出力ファイル:")
print(f"   📄 {output_path}")
print(f"   📄 {result_output_path}")
print()
print("🚀 分析完了！結果を確認してクエリ最適化にお役立てください。")
print("🎉" * 20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 追加の使用方法とカスタマイズ
# MAGIC 
# MAGIC ### 🔧 ファイルアップロード方法
# MAGIC 
# MAGIC #### 方法 1: Databricks UI でアップロード
# MAGIC 1. **Data** > **Create Table** をクリック
# MAGIC 2. **Upload File** を選択
# MAGIC 3. SQLプロファイラーJSONファイルをドラッグ&ドロップ
# MAGIC 4. アップロード完了後、パスをコピー
# MAGIC 5. 上記の `JSON_FILE_PATH` に設定
# MAGIC 
# MAGIC #### 方法 2: dbutils を使用
# MAGIC ```python
# MAGIC # ローカルファイルをFileStoreにアップロード
# MAGIC dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")
# MAGIC 
# MAGIC # 外部ストレージからのコピー
# MAGIC dbutils.fs.cp("s3a://bucket/profiler.json", "dbfs:/FileStore/profiler.json")
# MAGIC ```
# MAGIC 
# MAGIC ### 🎛️ カスタマイズポイント
# MAGIC 
# MAGIC - **メトリクス抽出**: `extract_performance_metrics` 関数内の重要キーワードリスト
# MAGIC - **分析プロンプト**: `analyze_bottlenecks_with_claude` 関数内の分析指示
# MAGIC - **表示形式**: emoji と出力フォーマットの調整
# MAGIC 
# MAGIC ### 🔍 エラー対処方法
# MAGIC 
# MAGIC 1. **Claude エンドポイントエラー**: Model Serving で `databricks-claude-3-7-sonnet` が稼働中か確認
# MAGIC 2. **ファイル読み込みエラー**: `dbutils.fs.ls("/FileStore/")` でファイル存在を確認
# MAGIC 3. **メモリエラー**: 大きなJSONファイルの場合はクラスタのメモリ設定を確認
# MAGIC 
# MAGIC ### 💡 高度な使用例
# MAGIC 
# MAGIC ```python
# MAGIC # 複数ファイルの一括分析
# MAGIC profiler_files = dbutils.fs.ls("/FileStore/profiler_logs/")
# MAGIC for file_info in profiler_files:
# MAGIC     if file_info.path.endswith('.json'):
# MAGIC         profiler_data = load_profiler_json(file_info.path)
# MAGIC         metrics = extract_performance_metrics(profiler_data)
# MAGIC         # 分析処理...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 このNotebookの使用方法
# MAGIC 
# MAGIC 1. **エンドポイント設定**: Model Serving で `databricks-claude-3-7-sonnet` エンドポイントを作成
# MAGIC 2. **ファイル準備**: SQLプロファイラーJSONファイルをVolumes、FileStore、またはDBFSにアップロード
# MAGIC 3. **パス設定**: セル2で `JSON_FILE_PATH` を実際のファイルパスに変更
# MAGIC 4. **実行**: 「Run All」をクリックまたは各セルを順番に実行
# MAGIC 5. **結果確認**: 抽出されたメトリクスとAI分析結果を確認
# MAGIC 
# MAGIC **📧 サポート**: 問題が発生した場合は、エラーメッセージとDatabricks環境情報をお知らせください。