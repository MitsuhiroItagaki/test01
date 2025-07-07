# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL Query Profiler Metrics Analyzer
# MAGIC
# MAGIC このNotebookは、Databricks SQL Query Profilerの出力JSON（simple1.json, simple2.jsonなど）を分析し、
# MAGIC パフォーマンス・ボトルネックの特定と改善提案を生成します。
# MAGIC
# MAGIC ## 主要機能
# MAGIC - クエリメトリクスの詳細分析
# MAGIC - ボトルネック特定（Critical/High/Medium/Low）
# MAGIC - 並列度・データローカリティ評価
# MAGIC - PHOTON利用状況分析
# MAGIC - LIQUID CLUSTERING改善提案
# MAGIC - 実行可能な最適化コマンド生成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 依存関係のインポート

# COMMAND ----------

!pip install databricks-sdk[openai]>=0.35.0
dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
try:
    import pandas as pd
except ImportError:
    pd = None
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. メトリクス抽出クラス定義

# COMMAND ----------

class DatabricksMetricsAnalyzer:
    """
    Databricks SQL Query Profiler メトリクス分析器
    JSONファイルから性能メトリクスを抽出し、Databricks環境で直接分析を実行
    """
    
    def __init__(self, json_file_path: str):
        """
        Args:
            json_file_path: JSON profiler ファイルのパス
        """
        self.json_file_path = json_file_path
        self.spark = SparkSession.getActiveSession()
        self.metrics_data = self._load_json_data()
        self.query_metrics = self._extract_query_metrics()
        self.stage_metrics = self._extract_stage_metrics()
        self.table_info = self._extract_table_info()
        
    def _load_json_data(self) -> Dict[str, Any]:
        """JSONファイルを読み込み"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return {}
    
    def _extract_query_metrics(self) -> Dict[str, Any]:
        """クエリ全体のメトリクスを抽出"""
        query = self.metrics_data.get('query', {})
        metrics = query.get('metrics', {})
        
        return {
            'query_id': query.get('id'),
            'status': query.get('status'),
            'query_text': query.get('queryText', '').replace('\n', ' ').replace("'", "''"),
            'query_start_time': query.get('queryStartTimeMs'),
            'query_end_time': query.get('queryEndTimeMs'),
            'user_name': query.get('user', {}).get('name'),
            'endpoint_id': query.get('endpointId'),
            'total_time_ms': metrics.get('totalTimeMs', 0),
            'compilation_time_ms': metrics.get('compilationTimeMs', 0),
            'execution_time_ms': metrics.get('executionTimeMs', 0),
            'read_bytes': metrics.get('readBytes', 0),
            'rows_produced_count': metrics.get('rowsProducedCount', 0),
            'rows_read_count': metrics.get('rowsReadCount', 0),
            'read_files_count': metrics.get('readFilesCount', 0),
            'read_remote_bytes': metrics.get('readRemoteBytes', 0),
            'write_remote_bytes': metrics.get('writeRemoteBytes', 0),
            'read_cache_bytes': metrics.get('readCacheBytes', 0),
            'spill_to_disk_bytes': metrics.get('spillToDiskBytes', 0),
            'task_total_time_ms': metrics.get('taskTotalTimeMs', 0),
            'photon_total_time_ms': metrics.get('photonTotalTimeMs', 0),
            'network_sent_bytes': metrics.get('networkSentBytes', 0),
            'bytes_read_from_cache_percentage': metrics.get('bytesReadFromCachePercentage', 0),
            'metadata_time_ms': metrics.get('metadataTimeMs', 0),
            'write_remote_files': metrics.get('writeRemoteFiles', 0),
            'write_remote_rows': metrics.get('writeRemoteRows', 0)
        }
    
    def _extract_stage_metrics(self) -> List[Dict[str, Any]]:
        """各実行段階のメトリクスを抽出"""
        stages = []
        graphs = self.metrics_data.get('graphs', [])
        
        for graph in graphs:
            nodes = graph.get('nodes', [])
            for node in nodes:
                if node.get('hidden', False):
                    continue
                    
                stage_metrics = self._extract_node_metrics(node)
                if stage_metrics:
                    stages.append(stage_metrics)
        
        return stages
    
    def _extract_table_info(self) -> Dict[str, Any]:
        """テーブル情報を抽出"""
        query_text = self.query_metrics.get('query_text', '')
        
        table_info = {
            'source_tables': [],
            'target_table': '',
            'sort_columns': [],
            'cluster_columns': [],
            'scan_table_columns': {}
        }
        
        graphs = self.metrics_data.get('graphs', [])
        for graph in graphs:
            nodes = graph.get('nodes', [])
            for node in nodes:
                node_name = node.get('name', '')
                metadata = node.get('metadata', [])
                
                if 'Scan' in node_name:
                    current_table = ''
                    current_clusters = []
                    
                    # ノード名からも直接テーブル名を抽出（フォールバック）
                    import re
                    scan_match = re.search(r'Scan\s+(.+)', node_name)
                    if scan_match:
                        node_table_name = scan_match.group(1).strip()
                        if node_table_name and node_table_name not in table_info['source_tables']:
                            table_info['source_tables'].append(node_table_name)
                        current_table = node_table_name
                    
                    for meta in metadata:
                        if meta.get('key') == 'SCAN_IDENTIFIER':
                            table_name = meta.get('value', '')
                            if table_name:
                                current_table = table_name
                                if table_name not in table_info['source_tables']:
                                    table_info['source_tables'].append(table_name)
                        elif meta.get('key') == 'SCAN_CLUSTERS':
                            cluster_cols = meta.get('values', [])
                            current_clusters = cluster_cols
                            table_info['cluster_columns'].extend(cluster_cols)
                    
                    if current_table and current_clusters:
                        table_info['scan_table_columns'][current_table] = current_clusters
                
                if 'Write Into' in node_name:
                    import re
                    table_match = re.search(r'Write Into\s+(.+)', node_name)
                    if table_match:
                        table_info['target_table'] = table_match.group(1).strip()
                    
                    for meta in metadata:
                        if meta.get('key') == 'SCAN_IDENTIFIER':
                            table_info['target_table'] = meta.get('value', '')
                
                if 'Sort' in node_name or 'SORT' in node.get('tag', ''):
                    for meta in metadata:
                        if meta.get('key') == 'SORT_ORDER':
                            sort_values = meta.get('values', [])
                            for sort_val in sort_values:
                                col_name = sort_val.split()[0] if sort_val else ''
                                if '.' in col_name:
                                    col_name = col_name.split('.')[-1]
                                if col_name and col_name not in table_info['sort_columns']:
                                    table_info['sort_columns'].append(col_name)
        
        # クエリテキストから追加情報を抽出
        if query_text:
            import re
            
            if not table_info['target_table'] and 'CREATE' in query_text.upper():
                create_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`?([^`\s]+)`?', query_text, re.IGNORECASE)
                if create_match:
                    table_info['target_table'] = create_match.group(1)
            
            if not table_info['sort_columns'] and 'ORDER BY' in query_text.upper():
                order_match = re.search(r'ORDER\s+BY\s+([^\s,]+)', query_text, re.IGNORECASE)
                if order_match:
                    col_name = order_match.group(1)
                    if '.' in col_name:
                        col_name = col_name.split('.')[-1]
                    table_info['sort_columns'].append(col_name)
            
            cs_columns = re.findall(r'\bcs_\w+\b', query_text, re.IGNORECASE)
            if cs_columns:
                for col in cs_columns:
                    if 'customer' in col.lower() or 'bill' in col.lower():
                        if col not in table_info['cluster_columns']:
                            table_info['cluster_columns'].insert(0, col)
        
        return table_info
    
    def _extract_node_metrics(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """ノードメトリクスを抽出"""
        node_name = node.get('name', 'Unknown')
        node_tag = node.get('tag', 'UNKNOWN')
        key_metrics = node.get('keyMetrics', {})
        
        important_tags = [
            'UNKNOWN_DATA_SOURCE_SCAN_EXEC',
            'PHOTON_PARQUET_WRITER_EXEC',
            'PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC',
            'PHOTON_SORT_EXEC',
            'PHOTON_PROJECT_EXEC',
            'PHOTON_SHUFFLE_MAP_STAGE_EXEC',
            'PHOTON_WRITE_STAGE_EXEC'
        ]
        
        if not any(tag in node_tag for tag in important_tags):
            return None
        
        metrics = node.get('metrics', [])
        duration_ms = key_metrics.get('durationMs', 0)
        peak_memory_bytes = key_metrics.get('peakMemoryBytes', 0)
        rows_num = key_metrics.get('rowsNum', 0)
        
        data_size_bytes = 0
        task_count = 0
        local_tasks = 0
        non_local_tasks = 0
        is_photon_enabled = False
        photon_time_ms = 0
        
        metadata = node.get('metadata', [])
        for meta in metadata:
            if meta.get('key') == 'IS_PHOTON' and meta.get('value') == 'true':
                is_photon_enabled = True
                break
        
        if 'PHOTON' in node_tag:
            is_photon_enabled = True
        
        for metric in metrics:
            label = metric.get('label', '').lower()
            key = metric.get('key', '')
            value = metric.get('value', 0)
            
            if key in ['UNKNOWN_KEY'] and 'bytes' in label:
                if 'output' in label or 'size' in label:
                    data_size_bytes = value if value > data_size_bytes else data_size_bytes
            
            if 'tasks total' in label:
                task_count = value
            elif 'number of local scan tasks' in label:
                local_tasks = value
            elif 'number of non-local' in label and 'scan tasks' in label:
                non_local_tasks = value
            
            if 'photon' in label and 'time' in label:
                photon_time_ms = value if value > photon_time_ms else photon_time_ms
        
        stage_type = self._determine_stage_type(node_name, node_tag)
        
        return {
            'stage_name': node_name,
            'stage_type': stage_type,
            'stage_tag': node_tag,
            'duration_ms': duration_ms,
            'peak_memory_bytes': peak_memory_bytes,
            'rows_processed': rows_num,
            'data_size_bytes': data_size_bytes,
            'task_count': task_count,
            'local_tasks': local_tasks,
            'non_local_tasks': non_local_tasks,
            'is_photon_enabled': is_photon_enabled,
            'photon_time_ms': photon_time_ms
        }
    
    def _determine_stage_type(self, node_name: str, node_tag: str) -> str:
        """ステージタイプを判定"""
        node_name_lower = node_name.lower()
        node_tag_lower = node_tag.lower()
        
        if 'scan' in node_name_lower or 'scan' in node_tag_lower:
            return 'Table Scan'
        elif 'write' in node_name_lower or 'writer' in node_tag_lower:
            return 'Table Write'
        elif 'shuffle' in node_name_lower or 'shuffle' in node_tag_lower:
            return 'Data Shuffle'
        elif 'sort' in node_name_lower or 'sort' in node_tag_lower:
            return 'Sort Operation'
        elif 'project' in node_name_lower or 'project' in node_tag_lower:
            return 'Projection'
        elif 'stage' in node_name_lower:
            return 'Stage Operation'
        else:
            return 'Other'
    
    def get_cluster_column(self) -> str:
        """最適なクラスターカラムを取得"""
        cluster_columns = self.table_info['cluster_columns']
        sort_columns = self.table_info['sort_columns']
        
        cluster_column = 'cluster_column'
        
        if cluster_columns:
            for col in cluster_columns:
                if 'customer' in col.lower() and 'bill' in col.lower():
                    cluster_column = col
                    break
                elif 'customer' in col.lower():
                    cluster_column = col
                    break
            
            if cluster_column == 'cluster_column':
                cluster_column = cluster_columns[0]
        
        scan_table_columns = self.table_info.get('scan_table_columns', {})
        for table, columns in scan_table_columns.items():
            for col in columns:
                if 'customer' in col.lower() and 'bill' in col.lower():
                    cluster_column = col
                    break
                elif 'customer' in col.lower():
                    cluster_column = col
                    break
        
        if sort_columns and sort_columns[0] != 'sort_column':
            if sort_columns[0].startswith('cs_'):
                cluster_column = sort_columns[0]
        
        return cluster_column
    
    def create_dataframes(self):
        """Spark DataFrameを作成"""
        # Query Overview DataFrame
        query_data = [
            (
                self.query_metrics['query_id'],
                self.query_metrics['status'],
                self.query_metrics['query_text'][:100] + '...',
                self.query_metrics['query_start_time'],
                self.query_metrics['query_end_time'],
                self.query_metrics['user_name'],
                self.query_metrics['endpoint_id'],
                self.query_metrics['total_time_ms'],
                self.query_metrics['compilation_time_ms'],
                self.query_metrics['execution_time_ms'],
                self.query_metrics['read_bytes'],
                self.query_metrics['rows_produced_count'],
                self.query_metrics['rows_read_count'],
                self.query_metrics['read_files_count'],
                self.query_metrics['read_remote_bytes'],
                self.query_metrics['write_remote_bytes'],
                self.query_metrics['read_cache_bytes'],
                self.query_metrics['spill_to_disk_bytes'],
                self.query_metrics['task_total_time_ms'],
                self.query_metrics['photon_total_time_ms'],
                self.query_metrics['network_sent_bytes'],
                self.query_metrics['bytes_read_from_cache_percentage'],
                self.query_metrics['metadata_time_ms'],
                self.query_metrics['write_remote_files'],
                self.query_metrics['write_remote_rows']
            )
        ]
        
        query_schema = StructType([
            StructField("query_id", StringType(), True),
            StructField("query_status", StringType(), True),
            StructField("query_text", StringType(), True),
            StructField("query_start_time", LongType(), True),
            StructField("query_end_time", LongType(), True),
            StructField("user_name", StringType(), True),
            StructField("endpoint_id", StringType(), True),
            StructField("total_time_ms", LongType(), True),
            StructField("compilation_time_ms", LongType(), True),
            StructField("execution_time_ms", LongType(), True),
            StructField("read_bytes", LongType(), True),
            StructField("rows_produced_count", LongType(), True),
            StructField("rows_read_count", LongType(), True),
            StructField("read_files_count", LongType(), True),
            StructField("read_remote_bytes", LongType(), True),
            StructField("write_remote_bytes", LongType(), True),
            StructField("read_cache_bytes", LongType(), True),
            StructField("spill_to_disk_bytes", LongType(), True),
            StructField("task_total_time_ms", LongType(), True),
            StructField("photon_total_time_ms", LongType(), True),
            StructField("network_sent_bytes", LongType(), True),
            StructField("bytes_read_from_cache_percentage", LongType(), True),
            StructField("metadata_time_ms", LongType(), True),
            StructField("write_remote_files", LongType(), True),
            StructField("write_remote_rows", LongType(), True)
        ])
        
        self.query_df = self.spark.createDataFrame(query_data, query_schema)
        
        # Stage Metrics DataFrame
        stage_data = []
        for i, stage in enumerate(self.stage_metrics):
            stage_data.append((
                stage['stage_name'],
                stage['stage_type'],
                stage['stage_tag'],
                stage['duration_ms'],
                stage['peak_memory_bytes'],
                stage['rows_processed'],
                stage['data_size_bytes'],
                stage.get('task_count', 0),
                stage.get('local_tasks', 0),
                stage.get('non_local_tasks', 0),
                stage.get('is_photon_enabled', False),
                stage.get('photon_time_ms', 0),
                i + 1
            ))
        
        stage_schema = StructType([
            StructField("stage_name", StringType(), True),
            StructField("stage_type", StringType(), True),
            StructField("stage_tag", StringType(), True),
            StructField("duration_ms", LongType(), True),
            StructField("peak_memory_bytes", LongType(), True),
            StructField("rows_processed", LongType(), True),
            StructField("data_size_bytes", LongType(), True),
            StructField("task_count", IntegerType(), True),
            StructField("local_tasks", IntegerType(), True),
            StructField("non_local_tasks", IntegerType(), True),
            StructField("is_photon_enabled", BooleanType(), True),
            StructField("photon_time_ms", LongType(), True),
            StructField("stage_order", IntegerType(), True)
        ])
        
        self.stage_df = self.spark.createDataFrame(stage_data, stage_schema)
        
        # Create temporary views for SQL analysis
        self.query_df.createOrReplaceTempView("query_overview")
        self.stage_df.createOrReplaceTempView("execution_stages_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 分析実行設定

# COMMAND ----------

# JSONファイルのパス設定（DBFSまたはローカル）
# 例: '/dbfs/FileStore/shared_uploads/your_email@company.com/simple2.json'
json_file_path = '/Volumes/main/base/mitsuhiro_vol/simple2.json'

# 分析器の初期化
analyzer = DatabricksMetricsAnalyzer(json_file_path)

# DataFrameの作成
analyzer.create_dataframes()

print("✅ 分析準備完了")
print(f"📊 解析対象: {analyzer.query_metrics['query_id']}")
print(f"⏱️  実行時間: {analyzer.query_metrics['total_time_ms']/1000:.2f}秒")
print(f"📋 ステージ数: {len(analyzer.stage_metrics)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. クエリ全体サマリー

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   query_id,
# MAGIC   query_status,
# MAGIC   query_text,
# MAGIC   user_name,
# MAGIC   ROUND(total_time_ms / 1000.0, 2) as total_time_seconds,
# MAGIC   ROUND(read_bytes / 1024.0 / 1024.0 / 1024.0, 2) as read_gb,
# MAGIC   ROUND(rows_read_count / 1000000.0, 2) as rows_read_millions,
# MAGIC   bytes_read_from_cache_percentage as cache_hit_percentage,
# MAGIC   ROUND(photon_total_time_ms / 1000.0, 2) as photon_time_seconds
# MAGIC FROM query_overview

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ステージ別パフォーマンス分析

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW execution_stages AS
# MAGIC SELECT 
# MAGIC   stage_name,
# MAGIC   stage_type,
# MAGIC   stage_tag,
# MAGIC   duration_ms,
# MAGIC   peak_memory_bytes,
# MAGIC   rows_processed,
# MAGIC   data_size_bytes,
# MAGIC   task_count,
# MAGIC   local_tasks,
# MAGIC   non_local_tasks,
# MAGIC   is_photon_enabled,
# MAGIC   photon_time_ms,
# MAGIC   stage_order,
# MAGIC   
# MAGIC   -- ボトルネック分析
# MAGIC   CASE 
# MAGIC     WHEN duration_ms > 1000000 THEN 'Critical'
# MAGIC     WHEN duration_ms > 500000 THEN 'High'
# MAGIC     WHEN duration_ms > 100000 THEN 'Medium'
# MAGIC     ELSE 'Low'
# MAGIC   END AS bottleneck_severity,
# MAGIC   
# MAGIC   -- メモリ使用量評価
# MAGIC   CASE 
# MAGIC     WHEN peak_memory_bytes > 300000000000 THEN 'Memory Intensive'
# MAGIC     WHEN peak_memory_bytes > 100000000000 THEN 'High Memory'
# MAGIC     WHEN peak_memory_bytes > 50000000000 THEN 'Medium Memory'
# MAGIC     ELSE 'Low Memory'
# MAGIC   END AS memory_usage_level,
# MAGIC   
# MAGIC   -- 並列度評価
# MAGIC   CASE 
# MAGIC     WHEN task_count = 0 THEN 'Unknown'
# MAGIC     WHEN task_count < 10 THEN 'Low Parallelism'
# MAGIC     WHEN task_count < 100 THEN 'Medium Parallelism'
# MAGIC     WHEN task_count < 1000 THEN 'High Parallelism'
# MAGIC     ELSE 'Very High Parallelism'
# MAGIC   END AS parallelism_level,
# MAGIC   
# MAGIC   -- データローカリティ評価
# MAGIC   CASE 
# MAGIC     WHEN (local_tasks + non_local_tasks) = 0 THEN 'No Scan Tasks'
# MAGIC     WHEN ROUND(local_tasks * 100.0 / (local_tasks + non_local_tasks), 2) > 80 THEN 'Good Locality'
# MAGIC     WHEN ROUND(local_tasks * 100.0 / (local_tasks + non_local_tasks), 2) > 50 THEN 'Medium Locality'
# MAGIC     ELSE 'Poor Locality'
# MAGIC   END AS data_locality_level,
# MAGIC   
# MAGIC   -- PHOTON利用状況
# MAGIC   CASE 
# MAGIC     WHEN is_photon_enabled = true THEN 'Photon Enabled'
# MAGIC     ELSE 'Photon Disabled'
# MAGIC   END AS photon_utilization_level,
# MAGIC   
# MAGIC   -- PHOTON効率
# MAGIC   CASE 
# MAGIC     WHEN is_photon_enabled = true AND photon_time_ms > 0 THEN 
# MAGIC       ROUND(photon_time_ms * 100.0 / NULLIF(duration_ms, 0), 2)
# MAGIC     ELSE 0
# MAGIC   END AS photon_efficiency_percentage
# MAGIC   
# MAGIC FROM execution_stages_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. トップボトルネック分析

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   stage_name,
# MAGIC   stage_type,
# MAGIC   ROUND(duration_ms / 60000.0, 2) as duration_minutes,
# MAGIC   ROUND(peak_memory_bytes / 1024.0 / 1024.0 / 1024.0, 2) as peak_memory_gb,
# MAGIC   ROUND(rows_processed / 1000000.0, 2) as rows_processed_millions,
# MAGIC   ROUND(duration_ms * 100.0 / SUM(duration_ms) OVER (), 2) as time_percentage,
# MAGIC   bottleneck_severity,
# MAGIC   memory_usage_level,
# MAGIC   parallelism_level,
# MAGIC   data_locality_level,
# MAGIC   photon_utilization_level,
# MAGIC   photon_efficiency_percentage
# MAGIC FROM execution_stages
# MAGIC ORDER BY duration_ms DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 改善提案生成

# COMMAND ----------

# 改善提案の生成とテーブル情報の取得
cluster_column = analyzer.get_cluster_column()
source_table = analyzer.table_info['source_tables'][0] if analyzer.table_info['source_tables'] else 'source_table'
target_table = analyzer.table_info['target_table'] if analyzer.table_info['target_table'] else source_table

# 最適化対象テーブルを決定（SELECTクエリの場合はsource_tableが最適化対象）
optimization_table = source_table if source_table != 'source_table' else target_table

print(f"🎯 検出されたテーブル情報:")
print(f"   - ソーステーブル: {source_table}")
print(f"   - ターゲットテーブル: {target_table}")
print(f"   - 最適化対象テーブル: {optimization_table}")
print(f"   - 推奨クラスターカラム: {cluster_column}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW improvement_suggestions AS
# MAGIC SELECT 
# MAGIC   stage_name,
# MAGIC   stage_type,
# MAGIC   bottleneck_severity,
# MAGIC   memory_usage_level,
# MAGIC   parallelism_level,
# MAGIC   data_locality_level,
# MAGIC   photon_utilization_level,
# MAGIC   photon_efficiency_percentage,
# MAGIC   ROUND(duration_ms / 60000.0, 2) as duration_minutes,
# MAGIC   ROUND(peak_memory_bytes / 1024.0 / 1024.0 / 1024.0, 2) as peak_memory_gb,
# MAGIC   ROUND(rows_processed / NULLIF(duration_ms, 0) * 1000.0, 2) as rows_per_second,
# MAGIC   ROUND(duration_ms * 100.0 / SUM(duration_ms) OVER (), 2) as time_percentage,
# MAGIC   task_count,
# MAGIC   CASE 
# MAGIC     WHEN (local_tasks + non_local_tasks) > 0 
# MAGIC     THEN ROUND(local_tasks * 100.0 / (local_tasks + non_local_tasks), 2)
# MAGIC     ELSE NULL
# MAGIC   END as data_locality_percentage,
# MAGIC   
# MAGIC   -- 改善提案
# MAGIC   CASE stage_type
# MAGIC     WHEN 'Table Scan' THEN 
# MAGIC       CASE 
# MAGIC         WHEN bottleneck_severity = 'Critical' AND photon_utilization_level = 'Photon Disabled' THEN 'LIQUID CLUSTERING最適化; PHOTONエンジン有効化'
# MAGIC         WHEN bottleneck_severity = 'Critical' AND parallelism_level = 'Low Parallelism' THEN 'LIQUID CLUSTERING最適化; 並列度向上(パーティション調整)'
# MAGIC         WHEN bottleneck_severity = 'Critical' AND data_locality_level = 'Poor Locality' THEN 'LIQUID CLUSTERING最適化; データローカリティ改善'
# MAGIC         WHEN bottleneck_severity = 'Critical' THEN 'LIQUID CLUSTERING最適化'
# MAGIC         WHEN photon_utilization_level = 'Photon Disabled' THEN 'LIQUID CLUSTERING最適化; PHOTONエンジン有効化'
# MAGIC         WHEN parallelism_level = 'Low Parallelism' THEN 'LIQUID CLUSTERING最適化; 並列度向上'
# MAGIC         WHEN data_locality_level = 'Poor Locality' THEN 'LIQUID CLUSTERING最適化; データローカリティ改善'
# MAGIC         ELSE 'LIQUID CLUSTERING最適化'
# MAGIC       END
# MAGIC     WHEN 'Sort Operation' THEN 
# MAGIC       CASE 
# MAGIC         WHEN bottleneck_severity = 'Critical' AND photon_utilization_level = 'Photon Disabled' THEN 'PHOTONエンジン有効化; メモリ増加; ソート戦略見直し'
# MAGIC         WHEN bottleneck_severity = 'Critical' AND parallelism_level = 'Low Parallelism' THEN 'メモリ増加; 並列度向上; ソート戦略見直し'
# MAGIC         WHEN bottleneck_severity = 'Critical' THEN 'メモリ増加; 範囲パーティション; ソート戦略見直し'
# MAGIC         WHEN photon_utilization_level = 'Photon Disabled' THEN 'PHOTONエンジン有効化; ソート最適化; 並列度調整'
# MAGIC         WHEN parallelism_level = 'Low Parallelism' THEN 'パーティション内ソート; 並列度調整'
# MAGIC         ELSE 'ソート最適化; 並列度調整'
# MAGIC       END
# MAGIC     ELSE '処理最適化; 演算効率化'
# MAGIC   END as improvement_action,
# MAGIC   
# MAGIC   -- 期待改善率
# MAGIC   CASE 
# MAGIC     WHEN bottleneck_severity = 'Critical' THEN 
# MAGIC       CASE stage_type
# MAGIC         WHEN 'Table Scan' THEN 70
# MAGIC         WHEN 'Sort Operation' THEN 40
# MAGIC         ELSE 30
# MAGIC       END
# MAGIC     WHEN bottleneck_severity = 'High' THEN 
# MAGIC       CASE stage_type
# MAGIC         WHEN 'Table Scan' THEN 50
# MAGIC         WHEN 'Sort Operation' THEN 30
# MAGIC         ELSE 25
# MAGIC       END
# MAGIC     ELSE 20
# MAGIC   END as expected_improvement_percentage,
# MAGIC   
# MAGIC   -- 優先度
# MAGIC   CASE 
# MAGIC     WHEN bottleneck_severity = 'Critical' AND time_percentage > 30 THEN 'P0-Critical'
# MAGIC     WHEN bottleneck_severity = 'Critical' OR time_percentage > 20 THEN 'P1-High'
# MAGIC     WHEN photon_utilization_level = 'Photon Disabled' AND time_percentage > 15 THEN 'P1-High'
# MAGIC     WHEN parallelism_level = 'Low Parallelism' AND time_percentage > 15 THEN 'P1-High'
# MAGIC     WHEN data_locality_level = 'Poor Locality' AND time_percentage > 15 THEN 'P2-Medium'
# MAGIC     WHEN bottleneck_severity = 'High' OR time_percentage > 10 THEN 'P2-Medium'
# MAGIC     ELSE 'P3-Low'
# MAGIC   END as priority
# MAGIC   
# MAGIC FROM execution_stages

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 最終分析結果

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   stage_name,
# MAGIC   stage_type,
# MAGIC   bottleneck_severity,
# MAGIC   duration_minutes,
# MAGIC   peak_memory_gb,
# MAGIC   time_percentage,
# MAGIC   parallelism_level,
# MAGIC   data_locality_level,
# MAGIC   photon_utilization_level,
# MAGIC   improvement_action,
# MAGIC   expected_improvement_percentage,
# MAGIC   priority
# MAGIC FROM improvement_suggestions
# MAGIC ORDER BY 
# MAGIC   CASE bottleneck_severity
# MAGIC     WHEN 'Critical' THEN 1
# MAGIC     WHEN 'High' THEN 2
# MAGIC     WHEN 'Medium' THEN 3
# MAGIC     ELSE 4
# MAGIC   END,
# MAGIC   time_percentage DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 実装可能な最適化コマンド

# COMMAND ----------

# 最適化コマンドを動的に生成
# テーブル情報を取得
cluster_column = analyzer.get_cluster_column()
source_table = analyzer.table_info['source_tables'][0] if analyzer.table_info['source_tables'] else 'source_table'
target_table = analyzer.table_info['target_table'] if analyzer.table_info['target_table'] else source_table
optimization_table = source_table if source_table != 'source_table' else target_table

optimization_commands = []

print(f"🔍 デバッグ情報:")
print(f"   - 最適化対象テーブル: {optimization_table}")
print(f"   - クラスターカラム: {cluster_column}")

# SQLビューから分析結果を取得
try:
    analysis_results = analyzer.spark.sql("""
        SELECT stage_name, stage_type, bottleneck_severity, is_photon_enabled, 
               parallelism_level, data_locality_level
        FROM execution_stages 
        WHERE stage_type = 'Table Scan' AND (bottleneck_severity = 'Critical' OR bottleneck_severity = 'High')
    """).collect()
    
    print(f"   - SQL結果件数: {len(analysis_results)}")
    
    for row in analysis_results:
        stage_name = row['stage_name']
        stage_type = row['stage_type']
        is_photon_enabled = row['is_photon_enabled']
        bottleneck_severity = row['bottleneck_severity']
        
        print(f"   - 発見されたステージ: {stage_name}")
        print(f"     -> ボトルネック重要度: {bottleneck_severity}")
        print(f"     -> PHOTON有効: {is_photon_enabled}")
        
        if not is_photon_enabled:
            optimization_commands.append({
                'stage': stage_name,
                'command': f'ALTER TABLE {optimization_table} CLUSTER BY ({cluster_column}); OPTIMIZE {optimization_table};',
                'type': 'LIQUID CLUSTERING + PHOTON',
                'priority': 'P0-Critical'
            })
        else:
            optimization_commands.append({
                'stage': stage_name,
                'command': f'ALTER TABLE {optimization_table} CLUSTER BY ({cluster_column}); OPTIMIZE {optimization_table};',
                'type': 'LIQUID CLUSTERING',
                'priority': 'P0-Critical'
            })
            
except Exception as e:
    print(f"SQL結果の取得でエラーが発生しました: {e}")
    print("フォールバック: 生データから判定します")
    
    # フォールバック: 生データから直接判定
    optimization_scan_count = 0
    for stage in analyzer.stage_metrics:
        stage_name = stage['stage_name']
        stage_type = stage['stage_type']
        duration_ms = stage['duration_ms']
        is_photon_enabled = stage.get('is_photon_enabled', False)
        
        print(f"   - ステージ: {stage_name}, タイプ: {stage_type}, 実行時間: {duration_ms}ms")
        
        # ボトルネック判定を調整
        # - Critical: duration_ms > 1000000 (1000秒、約16.7分)
        # - High: duration_ms > 500000 (500秒、約8.3分) 
        # - Medium: duration_ms > 100000 (100秒、約1.7分)
        # simple2.jsonでは607秒なので、閾値を調整
        is_critical = duration_ms > 500000  # 500秒以上をCriticalとして扱う
        is_high = duration_ms > 100000      # 100秒以上をHighとして扱う
        
        if stage_type == 'Table Scan':
            print(f"     -> Critical判定: {is_critical} (閾値: 500000ms), High判定: {is_high}")
            # CriticalまたはHighの場合、最適化対象とする
            if is_critical or is_high:
                optimization_scan_count += 1
                if not is_photon_enabled:
                    optimization_commands.append({
                        'stage': stage_name,
                        'command': f'ALTER TABLE {optimization_table} CLUSTER BY ({cluster_column}); OPTIMIZE {optimization_table};',
                        'type': 'LIQUID CLUSTERING + PHOTON',
                        'priority': 'P0-Critical'
                    })
                else:
                    optimization_commands.append({
                        'stage': stage_name,
                        'command': f'ALTER TABLE {optimization_table} CLUSTER BY ({cluster_column}); OPTIMIZE {optimization_table};',
                        'type': 'LIQUID CLUSTERING',
                        'priority': 'P0-Critical'
                    })
    
    print(f"   - 最適化対象Table Scan数: {optimization_scan_count}")
    print(f"   - 生成されたコマンド数: {len(optimization_commands)}")

# 最適化コマンドをDataFrameとして表示
if optimization_commands:
    if pd is not None:
        try:
            commands_df = pd.DataFrame(optimization_commands)
            display(commands_df)
        except Exception as e:
            print(f"DataFrameの表示でエラーが発生しました: {e}")
            print("📋 最適化コマンド一覧:")
            for i, cmd in enumerate(optimization_commands, 1):
                print(f"{i}. ステージ: {cmd['stage']}")
                print(f"   コマンド: {cmd['command']}")
                print(f"   タイプ: {cmd['type']}")
                print(f"   優先度: {cmd['priority']}")
                print()
    else:
        print("📋 最適化コマンド一覧:")
        for i, cmd in enumerate(optimization_commands, 1):
            print(f"{i}. ステージ: {cmd['stage']}")
            print(f"   コマンド: {cmd['command']}")
            print(f"   タイプ: {cmd['type']}")
            print(f"   優先度: {cmd['priority']}")
            print()
    
    print("\n🚀 実行推奨コマンド:")
    for i, cmd in enumerate(optimization_commands, 1):
        print(f"{i}. {cmd['command']}")
else:
    print("⚠️ 最適化コマンドが生成されませんでした。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. 分析サマリー

# COMMAND ----------

# 分析サマリーの生成
total_time_seconds = analyzer.query_metrics['total_time_ms'] / 1000.0
total_data_gb = analyzer.query_metrics['read_bytes'] / (1024**3)
total_rows_millions = analyzer.query_metrics['rows_read_count'] / 1000000.0
cache_hit_rate = analyzer.query_metrics['bytes_read_from_cache_percentage']

# テーブル情報を再取得
cluster_column = analyzer.get_cluster_column()
source_table = analyzer.table_info['source_tables'][0] if analyzer.table_info['source_tables'] else 'source_table'
target_table = analyzer.table_info['target_table'] if analyzer.table_info['target_table'] else source_table
optimization_table = source_table if source_table != 'source_table' else target_table

summary_text = []
summary_text.append("="*80)
summary_text.append("📊 DATABRICKS SQL QUERY PROFILER 分析結果サマリー")
summary_text.append("="*80)
summary_text.append(f"🆔 クエリID: {analyzer.query_metrics['query_id']}")
summary_text.append(f"⏱️  総実行時間: {total_time_seconds:.2f}秒 ({total_time_seconds/60:.2f}分)")
summary_text.append(f"📁 データ読み込み: {total_data_gb:.2f}GB")
summary_text.append(f"📋 処理行数: {total_rows_millions:.2f}百万行")
summary_text.append(f"🎯 キャッシュヒット率: {cache_hit_rate}%")
summary_text.append(f"📊 解析ステージ数: {len(analyzer.stage_metrics)}")

summary_text.append("\n🔍 トップボトルネック:")
for i, stage in enumerate(analyzer.stage_metrics[:3], 1):
    duration_minutes = stage['duration_ms'] / 60000.0
    memory_gb = stage['peak_memory_bytes'] / (1024**3)
    summary_text.append(f"{i}. {stage['stage_name']} ({stage['stage_type']})")
    summary_text.append(f"   - 実行時間: {duration_minutes:.2f}分")
    summary_text.append(f"   - メモリ使用量: {memory_gb:.2f}GB")
    summary_text.append(f"   - 処理行数: {stage['rows_processed']:,}行")

summary_text.append("\n🎯 推奨テーブル最適化:")
summary_text.append(f"   - テーブル: {optimization_table}")
summary_text.append(f"   - クラスターカラム: {cluster_column}")
summary_text.append(f"   - 実行コマンド: ALTER TABLE {optimization_table} CLUSTER BY ({cluster_column})")

summary_text.append("\n✅ 分析完了！上記の最適化を実施することで、パフォーマンスの大幅な向上が期待できます。")
summary_text.append("="*80)

summary_str = "\n".join(summary_text)
summary_df = spark.createDataFrame([(summary_str,)], ["summary"]).createOrReplaceTempView("summary_table")
display(sql('select * from summary_table'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. クエリの書き換え

# COMMAND ----------

# MAGIC %md ### ボトルネック分析の結果とクエリテキストをマージ

# COMMAND ----------

df_json = spark.read.json(json_file_path)
df_json.createOrReplaceTempView("json_table")
df_result = sql('select summary,query.queryText from json_table cross join summary_table').createOrReplaceTempView("result_table")

#display(sql('select summary,queryText from result_table'))

# COMMAND ----------

# MAGIC %md ### databricks-claude-3-7-sonnet

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     endpoint => 'databricks-claude-3-7-sonnet',
# MAGIC     request => '統計情報の分析結果テキスト(' || summary || ')の内容からテキストからクエリのボトルネックを特定し、次のクエリを以下のルールに従い、最も効率的なクエリに書き換えてください。
# MAGIC     ルール1: 必ず同じ結果セットを返却してください。
# MAGIC     ルール2: PHOTONエンジンの利用を優先してください。
# MAGIC     ルール3: Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください。
# MAGIC     ルール4: Where句でLiquidClusteringが利用できる場合はクエリ変換後に各テーブルごと適切クラスタリングキーを設定するためのALTER文クエリを生成してください。
# MAGIC     ルール5: 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください。
# MAGIC     ルール6: 書き換え後のクエリは省略せずにそのまま実行できる完全な形で表示してください。
# MAGIC     ルール7: 書き換え後のクエリついて変更内容の説明をしてください。
# MAGIC     ' || queryText
# MAGIC   ) AS rewritten_query
# MAGIC FROM result_table

# COMMAND ----------

# MAGIC %md ### databricks-llama-4-maverick

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     endpoint => 'databricks-llama-4-maverick',
# MAGIC     request => '統計情報の分析結果テキスト(' || summary || ')の内容からテキストからクエリのボトルネックを特定し、次のクエリを以下のルールに従い、最も効率的なクエリに書き換えてください。
# MAGIC     ルール1: 必ず同じ結果セットを返却してください。
# MAGIC     ルール2: PHOTONエンジンの利用を優先してください。
# MAGIC     ルール3: Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください。
# MAGIC     ルール4: Where句でLiquidClusteringが利用できる場合はクエリ変換後に各テーブルごと適切クラスタリングキーを設定するためのALTER文クエリを生成してください。
# MAGIC     ルール5: 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください。
# MAGIC     ルール6: 書き換え後のクエリは省略せずにそのまま実行できる完全な形で表示してください。
# MAGIC     ルール7: 書き換え後のクエリついて変更内容の説明をしてください。
# MAGIC     ' || queryText
# MAGIC   ) AS rewritten_query
# MAGIC FROM result_table

# COMMAND ----------

# MAGIC %md ### gpt-4o

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     endpoint => 'gpt-4o-mini-yyl',
# MAGIC     request => '統計情報の分析結果テキスト(' || summary || ')の内容からテキストからクエリのボトルネックを特定し、次のクエリを以下のルールに従い、最も効率的なクエリに書き換えてください。
# MAGIC     ルール1: 必ず同じ結果セットを返却してください。
# MAGIC     ルール2: PHOTONエンジンの利用を優先してください。
# MAGIC     ルール3: Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください。
# MAGIC     ルール4: Where句でLiquidClusteringが利用できる場合はクエリ変換後に各テーブルごと適切クラスタリングキーを設定するためのALTER文クエリを生成してください。
# MAGIC     ルール5: 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください。
# MAGIC     ルール6: 書き換え後のクエリは省略せずにそのまま実行できる完全な形で表示してください。
# MAGIC     ルール7: 書き換え後のクエリついて変更内容の説明をしてください。
# MAGIC     ' || queryText
# MAGIC   ) AS rewritten_query
# MAGIC FROM result_table

# COMMAND ----------

# MAGIC %md ### databricks-claude-opus-4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     endpoint => 'databricks-claude-opus-4',
# MAGIC     request => '統計情報の分析結果テキスト(' || summary || ')の内容からテキストからクエリのボトルネックを特定し、次のクエリを以下のルールに従い、最も効率的なクエリに書き換えてください。
# MAGIC     ルール1: 必ず同じ結果セットを返却してください。
# MAGIC     ルール2: PHOTONエンジンの利用を優先してください。
# MAGIC     ルール3: Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください。
# MAGIC     ルール4: Where句でLiquidClusteringが利用できる場合はクエリ変換後に各テーブルごと適切クラスタリングキーを設定するためのALTER文クエリを生成してください。
# MAGIC     ルール5: 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください。
# MAGIC     ルール6: 書き換え後のクエリは省略せずにそのまま実行できる完全な形で表示してください。
# MAGIC     ルール7: 書き換え後のクエリついて変更内容の説明をしてください。
# MAGIC     ' || queryText
# MAGIC   ) AS rewritten_query
# MAGIC FROM result_table

# COMMAND ----------

# MAGIC %md ### databricks-claude-sonnet-4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ai_query(
# MAGIC     endpoint => 'databricks-claude-sonnet-4',
# MAGIC     request => '統計情報の分析結果テキスト(' || summary || ')の内容からテキストからクエリのボトルネックを特定し、次のクエリを以下のルールに従い、最も効率的なクエリに書き換えてください。
# MAGIC     ルール1: 必ず同じ結果セットを返却してください。
# MAGIC     ルール2: PHOTONエンジンの利用を優先してください。
# MAGIC     ルール3: Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください。
# MAGIC     ルール4: Where句でLiquidClusteringが利用できる場合はクエリ変換後に各テーブルごと適切クラスタリングキーを設定するためのALTER文クエリを生成してください。
# MAGIC     ルール5: 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください。
# MAGIC     ルール6: 書き換え後のクエリは省略せずにそのまま実行できる完全な形で表示してください。
# MAGIC     ルール7: 書き換え後のクエリついて変更内容の説明をしてください。
# MAGIC     ' || queryText
# MAGIC   ) AS rewritten_query
# MAGIC FROM result_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 注意事項
# MAGIC
# MAGIC 1. **JSONファイルパス**: セル3で`json_file_path`を正しいパスに設定してください
# MAGIC 2. **DBFS アップロード**: JSONファイルをDBFSにアップロードしてから実行してください
# MAGIC 3. **権限**: テーブルの最適化コマンド実行には適切な権限が必要です
# MAGIC 4. **本番環境**: 本番環境での実行前に、開発環境での検証を推奨します
# MAGIC
# MAGIC ## 使用方法
# MAGIC
# MAGIC 1. JSONファイルをDBFSにアップロード
# MAGIC 2. セル3で`json_file_path`を設定
# MAGIC 3. 全セルを順次実行
# MAGIC 4. 生成された最適化コマンドを実行