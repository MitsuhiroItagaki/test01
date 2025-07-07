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
                "photon_total_time_ms": query_metrics.get('photonTotalTimeMs', 0)
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
    
    return indicators

print("✅ 関数定義完了: calculate_bottleneck_indicators")

# COMMAND ----------

def analyze_liquid_clustering_opportunities(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからLiquid Clusteringに効果的なカラムを特定
    """
    import re
    
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
    
    # クエリテキストの解析（強化版）
    query_text = metrics.get('query_info', {}).get('query_text', '').upper()
    
    if query_text:
        # 強化されたWHERE句パターン（完全なスキーマ.テーブル.カラム形式に対応）
        where_patterns = [
            r'WHERE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',  # schema.table.column
            r'WHERE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',  # table.column
            r'WHERE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',  # column
            r'AND\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',
            r'AND\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',
            r'AND\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',
            r'OR\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',
            r'OR\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]',
            r'OR\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]'
        ]
        for pattern in where_patterns:
            matches = re.findall(pattern, query_text)
            clustering_analysis["filter_columns"].extend([col.strip() for col in matches])
        
        # 強化されたJOIN句パターン
        join_patterns = [
            r'JOIN\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*\s+[a-zA-Z_][a-zA-Z0-9_]*\s*ON\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)',
            r'LEFT\s+JOIN\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*\s+[a-zA-Z_][a-zA-Z0-9_]*\s*ON\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)',
            r'INNER\s+JOIN\s+[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*\s+[a-zA-Z_][a-zA-Z0-9_]*\s*ON\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)'
        ]
        for pattern in join_patterns:
            matches = re.findall(pattern, query_text)
            for match in matches:
                clustering_analysis["join_columns"].extend([col.strip() for col in match])
        
        # 強化されたGROUP BY句パターン
        groupby_pattern = r'GROUP\s+BY\s+((?:[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*(?:\s*,\s*)?)+)'
        groupby_matches = re.findall(groupby_pattern, query_text)
        for match in groupby_matches:
            cols = [col.strip() for col in re.split(r'\s*,\s*', match) if col.strip()]
            clustering_analysis["groupby_columns"].extend(cols)
    
    # ノードメトリクスからデータスキューとパーティション情報を分析（強化版）
    node_metrics = metrics.get('node_metrics', [])
    table_scan_nodes = []
    join_nodes = []
    shuffle_nodes = []
    filter_nodes = []
    
    for node in node_metrics:
        node_name = node.get('name', '').upper()
        node_tag = node.get('tag', '').upper()
        detailed_metrics = node.get('detailed_metrics', {})
        
        # プッシュダウンフィルターの抽出
        for metric_key, metric_info in detailed_metrics.items():
            if any(filter_keyword in metric_key.upper() for filter_keyword in ['FILTER', 'PREDICATE', 'CONDITION']):
                filter_value = metric_info.get('label', '') or str(metric_info.get('value', ''))
                if filter_value:
                    clustering_analysis["pushdown_filters"].append({
                        "node_id": node.get('node_id', ''),
                        "node_name": node_name,
                        "filter_expression": filter_value,
                        "metric_key": metric_key
                    })
        
        # テーブルスキャンノードの特定（詳細分析）
        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            table_scan_nodes.append(node)
            
            # データスキュー指標の計算
            key_metrics = node.get('key_metrics', {})
            rows_num = key_metrics.get('rowsNum', 0)
            duration_ms = key_metrics.get('durationMs', 0)
            
            # 強化されたテーブル名抽出（完全スキーマ対応）
            table_patterns = [
                r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # schema.table.subtable
                r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # schema.table
                r'([a-zA-Z_][a-zA-Z0-9_]*)'  # table
            ]
            
            table_name = None
            for pattern in table_patterns:
                table_match = re.search(pattern, node_name)
                if table_match:
                    table_name = table_match.group(1)
                    break
            
            if not table_name:
                table_name = f"table_{node.get('node_id', 'unknown')}"
            
            # 詳細メトリクスからフィルター条件情報を抽出
            filter_info = []
            column_references = []
            
            for metric_key, metric_info in detailed_metrics.items():
                label = metric_info.get('label', '')
                if label:
                    # カラム参照の抽出
                    column_matches = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', label)
                    column_references.extend(column_matches)
                    
                    # フィルター条件の抽出
                    if any(op in label for op in ['=', '<', '>', '<=', '>=', '!=', 'IN', 'LIKE']):
                        filter_info.append(label)
            
            clustering_analysis["data_skew_indicators"][table_name] = {
                "rows_scanned": rows_num,
                "scan_duration_ms": duration_ms,
                "avg_rows_per_ms": rows_num / max(duration_ms, 1),
                "node_name": node_name,
                "node_id": node.get('node_id', ''),
                "filter_conditions": filter_info,
                "column_references": list(set(column_references))
            }
            
            # カラム参照をフィルターカラムに追加
            clustering_analysis["filter_columns"].extend(column_references)
        
        # フィルターノードの特定
        elif any(keyword in node_name for keyword in ['FILTER']):
            filter_nodes.append(node)
        
        # JOINノードの特定
        elif any(keyword in node_name for keyword in ['JOIN', 'HASH']):
            join_nodes.append(node)
            
            # JOIN条件の詳細抽出
            for metric_key, metric_info in detailed_metrics.items():
                label = metric_info.get('label', '')
                if label and '=' in label:
                    # JOIN条件からカラムを抽出
                    join_cols = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', label)
                    clustering_analysis["join_columns"].extend(join_cols)
        
        # Shuffleノードの特定
        elif any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_nodes.append(node)
    
    # 詳細なカラム分析の実行
    all_columns = set()
    all_columns.update(clustering_analysis["filter_columns"])
    all_columns.update(clustering_analysis["join_columns"])
    all_columns.update(clustering_analysis["groupby_columns"])
    
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
        
        clustering_analysis["detailed_column_analysis"][column] = column_analysis
    
    # テーブル毎の推奨事項（強化版）
    for table_name, skew_info in clustering_analysis["data_skew_indicators"].items():
        # テーブルに関連するカラムの特定（より柔軟なマッチング）
        table_columns = []
        table_parts = table_name.split('.')
        
        for col in all_columns:
            col_parts = col.split('.')
            # 完全一致または部分一致でテーブル関連カラムを特定
            if len(col_parts) >= 2:
                if len(col_parts) == 3 and len(table_parts) >= 2:  # schema.table.column
                    if f"{col_parts[0]}.{col_parts[1]}" == table_name:
                        table_columns.append(col)
                elif len(col_parts) == 2:  # table.column
                    if col_parts[0] in table_name or table_name.endswith(col_parts[0]):
                        table_columns.append(col)
            else:
                # カラム名のみの場合、ノードのカラム参照と照合
                if col in skew_info.get("column_references", []):
                    table_columns.append(col)
        
        # ノードから直接抽出されたカラム参照も追加
        table_columns.extend(skew_info.get("column_references", []))
        table_columns = list(set(table_columns))  # 重複除去
        
        # カラムの重要度スコア計算（詳細版）
        column_scores = {}
        for col in table_columns:
            clean_col = col.split('.')[-1] if '.' in col else col
            
            # 詳細分析からスコアを取得
            if col in clustering_analysis["detailed_column_analysis"]:
                analysis = clustering_analysis["detailed_column_analysis"][col]
                score = analysis["total_usage"]
            else:
                # フォールバック計算
                score = (clustering_analysis["filter_columns"].count(col) * 3 +
                        clustering_analysis["join_columns"].count(col) * 2 +
                        clustering_analysis["groupby_columns"].count(col) * 1)
            
            if score > 0:
                column_scores[clean_col] = score
        
        # フィルター条件情報も含める
        filter_conditions = skew_info.get("filter_conditions", [])
        
        # 上位カラムを推奨
        if column_scores:
            sorted_columns = sorted(column_scores.items(), key=lambda x: x[1], reverse=True)
            recommended_cols = [col for col, score in sorted_columns[:4]]  # 最大4カラム
            
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
                    "node_name": skew_info.get("node_name", ""),
                    "filter_conditions": filter_conditions,
                    "column_references": skew_info.get("column_references", [])
                }
            }
    
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
    
    # サマリー情報
    clustering_analysis["summary"] = {
        "tables_identified": len(clustering_analysis["recommended_tables"]),
        "total_filter_columns": len(set(clustering_analysis["filter_columns"])),
        "total_join_columns": len(set(clustering_analysis["join_columns"])),
        "total_groupby_columns": len(set(clustering_analysis["groupby_columns"])),
        "high_impact_tables": len([t for t, info in clustering_analysis["recommended_tables"].items() 
                                 if info["scan_performance"]["scan_duration_ms"] > 5000])
    }
    
    return clustering_analysis

print("✅ 関数定義完了: analyze_liquid_clustering_opportunities")

# COMMAND ----------

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
- キャッシュヒット量: {metrics['overall_metrics'].get('read_cache_bytes', 0):,} bytes ({metrics['overall_metrics'].get('read_cache_bytes', 0)/1024/1024/1024:.2f} GB)
- 読み込み行数: {metrics['overall_metrics'].get('rows_read_count', 0):,} 行
- 出力行数: {metrics['overall_metrics'].get('rows_produced_count', 0):,} 行
- スピルサイズ: {metrics['overall_metrics'].get('spill_to_disk_bytes', 0):,} bytes
- Photon実行時間: {metrics['overall_metrics'].get('photon_total_time_ms', 0):,} ms

ボトルネック指標:
- コンパイル時間比率: {metrics['bottleneck_indicators'].get('compilation_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('compilation_ratio', 0)*100:.1f}%)
- キャッシュヒット率: {metrics['bottleneck_indicators'].get('cache_hit_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('cache_hit_ratio', 0)*100:.1f}%)
- データ選択性: {metrics['bottleneck_indicators'].get('data_selectivity', 0):.3f} ({metrics['bottleneck_indicators'].get('data_selectivity', 0)*100:.1f}%)
- Photon使用率: {metrics['bottleneck_indicators'].get('photon_ratio', 0):.3f} ({metrics['bottleneck_indicators'].get('photon_ratio', 0)*100:.1f}%)
- スピル発生: {'あり' if metrics['bottleneck_indicators'].get('has_spill', False) else 'なし'}
- 最も遅いステージID: {metrics['bottleneck_indicators'].get('slowest_stage_id', 'N/A')}
- 最高メモリ使用ノード: {metrics['bottleneck_indicators'].get('highest_memory_node_name', 'N/A')}
- 最高メモリ使用量: {metrics['bottleneck_indicators'].get('highest_memory_bytes', 0)/1024/1024:.2f} MB

ステージ詳細:
{chr(10).join([f"- ステージ{s['stage_id']}: {s['duration_ms']:,}ms, タスク数:{s['num_tasks']}, 完了:{s['num_complete_tasks']}, 失敗:{s['num_failed_tasks']}" for s in metrics['stage_metrics'][:10]])}

主要ノード詳細:
{chr(10).join([f"- {n['name']} (ID:{n['node_id']}): 行数={n['key_metrics'].get('rowsNum', 0):,}, 時間={n['key_metrics'].get('durationMs', 0):,}ms, メモリ={n['key_metrics'].get('peakMemoryBytes', 0)/1024/1024:.2f}MB" for n in metrics['node_metrics'][:15]])}

【Liquid Clustering推奨分析】
対象テーブル数: {metrics['liquid_clustering_analysis']['summary'].get('tables_identified', 0)}
高インパクトテーブル数: {metrics['liquid_clustering_analysis']['summary'].get('high_impact_tables', 0)}

テーブル別推奨クラスタリングカラム:
{chr(10).join([f"- {table}: {', '.join(info['clustering_columns'])}" for table, info in metrics['liquid_clustering_analysis']['recommended_tables'].items()])}

カラム使用パターン:
- フィルターカラム: {', '.join(list(set(metrics['liquid_clustering_analysis']['filter_columns']))[:10])}
- JOINカラム: {', '.join(list(set(metrics['liquid_clustering_analysis']['join_columns']))[:10])}
- GROUP BYカラム: {', '.join(list(set(metrics['liquid_clustering_analysis']['groupby_columns']))[:10])}

高インパクトカラム詳細:
{chr(10).join([f"- {col}: スコア={analysis['total_usage']}, 使用箇所=[{', '.join(analysis['usage_contexts'])}], フィルター={analysis['filter_usage_count']}回, JOIN={analysis['join_usage_count']}回, GROUP BY={analysis['groupby_usage_count']}回" for col, analysis in metrics['liquid_clustering_analysis']['detailed_column_analysis'].items() if analysis.get('performance_impact') == 'high'][:10])}

プッシュダウンフィルター詳細:
{chr(10).join([f"- ノード: {filter_info['node_name'][:50]} | フィルター: {filter_info['filter_expression'][:80]}" for filter_info in metrics['liquid_clustering_analysis']['pushdown_filters'][:5]])}

パフォーマンス向上見込み:
- スキャン改善: {metrics['liquid_clustering_analysis']['performance_impact'].get('potential_scan_improvement', 'N/A')}
- Shuffle削減: {metrics['liquid_clustering_analysis']['performance_impact'].get('potential_shuffle_reduction', 'N/A')}
- 全体改善: {metrics['liquid_clustering_analysis']['performance_impact'].get('estimated_overall_improvement', 'N/A')}

【分析して欲しい内容】
1. 主要なボトルネックの特定と原因分析
2. パフォーマンス改善の優先順位付け
3. 具体的な最適化案の提示（SQL改善、設定変更、インフラ最適化など）
4. **Liquid Clustering実装の具体的推奨事項と手順**
5. **各テーブルのクラスタリングカラム選定理由と期待効果**
6. **Liquid Clustering導入時の注意点と実装順序**
7. 予想される改善効果
8. Photon最適化の推奨事項
9. 重要な注意点や推奨事項

特に、Liquid Clusteringの実装については詳細な手順と期待効果を含めて、日本語で詳細な分析結果を提供してください。
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
            "max_tokens": 4000,
            "temperature": 0.1
        }
        
        print("🤖 Databricks Claude 3.7 Sonnetエンドポイントに分析リクエストを送信中...")
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result.get('choices', [{}])[0].get('message', {}).get('content', '')
            print("✅ ボトルネック分析が完了しました")
            return analysis_text
        else:
            error_msg = f"APIエラー: ステータスコード {response.status_code}\nレスポンス: {response.text}"
            print(f"❌ {error_msg}")
            return error_msg
            
    except Exception as e:
        error_msg = f"分析エラー: {str(e)}"
        print(f"❌ {error_msg}")
        return error_msg

print("✅ 関数定義完了: analyze_bottlenecks_with_claude")

# COMMAND ----------

# MAGIC %md
# MAGIC ## メイン処理の実行
# MAGIC 
# MAGIC 以下のセルを順番に実行して、SQLプロファイラー分析を実行します。
# MAGIC 
# MAGIC ### ファイルパスの設定例:
# MAGIC 
# MAGIC ```python
# MAGIC # ローカルファイル（サンプル）
# MAGIC JSON_FILE_PATH = 'simple0.json'
# MAGIC 
# MAGIC # FileStore アップロードファイル
# MAGIC JSON_FILE_PATH = '/FileStore/shared_uploads/username/profiler.json'
# MAGIC 
# MAGIC # DBFS ファイル
# MAGIC JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/username/profiler.json'
# MAGIC JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/username/profiler.json'
# MAGIC ```

# COMMAND ----------

# 🔧 設定: 分析対象のJSONファイルパスを指定
JSON_FILE_PATH = 'simple0.json'  # デフォルト: サンプルファイル

# 以下から選択して変更してください:
# JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler_log.json'
# JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'
# JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'

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

for key, value in bottleneck_indicators.items():
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
output_path = 'extracted_metrics.json'
with open(output_path, 'w', encoding='utf-8') as file:
    json.dump(extracted_metrics, file, indent=2, ensure_ascii=False)
print(f"✅ 抽出メトリクスを保存しました: {output_path}")

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

# ノードメトリクスの概要
print(f"\n🏗️ ノードメトリクス概要（上位10件）")
print("=" * 50)
for i, node in enumerate(extracted_metrics['node_metrics'][:10]):
    rows_num = node['key_metrics'].get('rowsNum', 0)
    duration_ms = node['key_metrics'].get('durationMs', 0)
    memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
    
    # 時間とメモリに基づいてアイコンを選択
    time_icon = "🟢" if duration_ms < 1000 else "🟡" if duration_ms < 10000 else "🔴"
    memory_icon = "💚" if memory_mb < 100 else "💛" if memory_mb < 1000 else "❤️"
    
    print(f"{i+1:2d}. {time_icon}{memory_icon} {node['name'][:40]:40} | 行数: {rows_num:>8,} | 時間: {duration_ms:>6,}ms | メモリ: {memory_mb:>6.1f}MB")

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
        print(f"   {i+1}. ノード: {filter_info.get('node_name', '')[:30]}")
        print(f"      📋 フィルター: {filter_info.get('filter_expression', '')[:60]}")
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
# MAGIC 2. **ファイル準備**: SQLプロファイラーJSONファイルをFileStoreまたはDBFSにアップロード
# MAGIC 3. **パス設定**: セル8で `JSON_FILE_PATH` を実際のファイルパスに変更
# MAGIC 4. **実行**: 「Run All」をクリックまたは各セルを順番に実行
# MAGIC 5. **結果確認**: 抽出されたメトリクスとAI分析結果を確認
# MAGIC 
# MAGIC **📧 サポート**: 問題が発生した場合は、エラーメッセージとDatabricks環境情報をお知らせください。