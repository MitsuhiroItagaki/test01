# Databricks notebook source
# MAGIC %md
# MAGIC ## 📁 セル1: 分析対象ファイル設定
# MAGIC 
# MAGIC **最初に、分析対象のSQLプロファイラーJSONファイルを指定してください。**
# MAGIC 
# MAGIC このセルでは以下の設定を行います：
# MAGIC - 📂 SQLプロファイラーJSONファイルのパス設定
# MAGIC - 📋 対応するファイルパス形式の例
# MAGIC - ⚙️ 基本的な環境設定

# COMMAND ----------

# 📁 SQLプロファイラーJSONファイルのパス設定
# 
# 以下のJSON_FILE_PATHを実際のファイルパスに変更してください：

JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'  # デフォルト: サンプルファイル

# 📋 対応するファイルパス形式の例:
# Unity Catalog Volumes:
# JSON_FILE_PATH = '/Volumes/catalog/schema/volume/profiler.json'
# 
# FileStore (推奨):
# JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler_log.json'
# 
# DBFS:
# JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'
# 
# DBFS URI:
# JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'

print("📁 【分析対象ファイル設定完了】")
print("=" * 50)
print(f"📄 対象ファイル: {JSON_FILE_PATH}")
print("=" * 50)

# ⚙️ 基本的な環境設定
import json
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime

print("✅ 基本ライブラリインポート完了")
print("🚀 次のセルに進んでください")

# COMMAND ----------

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
# MAGIC    - 設定可能なLLMエンドポイント (Databricks, OpenAI, Azure OpenAI, Anthropic)
# MAGIC    - 抽出メトリクスからボトルネック特定
# MAGIC    - 具体的な改善案の提示
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **事前準備:**
# MAGIC - LLMエンドポイントの設定（Databricks Model Serving または 外部API）
# MAGIC - 必要なAPIキーの設定
# MAGIC - SQLプロファイラーJSONファイルの準備（DBFS または FileStore）
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 セル目次
# MAGIC 
# MAGIC ### 🔧 設定・準備セル
# MAGIC - **セル1**: 分析対象ファイル設定（実行前に必ず設定）
# MAGIC - **セル2**: LLMエンドポイント設定
# MAGIC - **セル3**: SQLプロファイラーJSONファイル読み込み関数
# MAGIC 
# MAGIC ### 📊 分析関数定義セル
# MAGIC - **セル4**: パフォーマンスメトリクス抽出関数
# MAGIC - **セル5**: ノード名解析・改善関数
# MAGIC - **セル6**: ボトルネック指標計算関数
# MAGIC - **セル7**: Liquid Clustering分析関数
# MAGIC - **セル8**: LLMによるボトルネック分析関数
# MAGIC - **セル9**: 個別LLMプロバイダー接続関数
# MAGIC 
# MAGIC ### 🚀 メイン処理実行セル
# MAGIC - **セル10**: SQLプロファイラーJSONファイル読み込み実行
# MAGIC - **セル11**: パフォーマンスメトリクス抽出と概要表示
# MAGIC - **セル12**: ボトルネック指標詳細表示と時間消費TOP10
# MAGIC - **セル13**: LLMボトルネック分析実行の準備
# MAGIC - **セル14**: LLMボトルネック分析結果の表示
# MAGIC - **セル15**: 分析結果の保存と完了サマリー
# MAGIC 
# MAGIC ### 🔧 SQL最適化機能セル
# MAGIC - **セル16**: SQL最適化関連関数定義
# MAGIC - **セル17**: SQLクエリ最適化の実行（ステップ1: クエリ抽出）
# MAGIC - **セル18**: LLMによるSQL最適化（ステップ2: 最適化実行）
# MAGIC - **セル19**: 最適化結果の保存（ステップ3: ファイル生成）
# MAGIC - **セル20**: テスト実行の準備（ステップ4: 実行ガイド）
# MAGIC - **セル21**: 最終処理完了サマリー
# MAGIC 
# MAGIC ### 📚 リファレンスセル
# MAGIC - **セル22**: 追加の使用方法とカスタマイズ
# MAGIC - **セル23**: このNotebookの使用方法

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 セル2: LLMエンドポイント設定
# MAGIC 
# MAGIC このセルでは以下の設定を行います：
# MAGIC - LLMプロバイダーの選択（Databricks/OpenAI/Azure/Anthropic）
# MAGIC - 各プロバイダーの接続設定
# MAGIC - 必要なライブラリのインポート

# COMMAND ----------

# 🤖 LLMエンドポイント設定
LLM_CONFIG = {
    # エンドポイントタイプ: 'databricks', 'openai', 'azure_openai', 'anthropic'
    "provider": "databricks",
    
    # Databricks Model Serving設定
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",  # Model Servingエンドポイント名
        "max_tokens": 2000,
        "temperature": 0.1,
        "thinking_enabled": True  # 拡張思考モード（thinking: {"type": "enabled"}）
    },
    
    # OpenAI設定
    "openai": {
        "api_key": "",  # OpenAI APIキー (環境変数OPENAI_API_KEYでも可)
        "model": "gpt-4o",  # gpt-4o, gpt-4-turbo, gpt-3.5-turbo
        "max_tokens": 2000,
        "temperature": 0.1
    },
    
    # Azure OpenAI設定
    "azure_openai": {
        "api_key": "",  # Azure OpenAI APIキー (環境変数AZURE_OPENAI_API_KEYでも可)
        "endpoint": "",  # https://your-resource.openai.azure.com/
        "deployment_name": "",  # デプロイメント名
        "api_version": "2024-02-01",
        "max_tokens": 2000,
        "temperature": 0.1
    },
    
    # Anthropic設定
    "anthropic": {
        "api_key": "",  # Anthropic APIキー (環境変数ANTHROPIC_API_KEYでも可)
        "model": "claude-3-5-sonnet-20241022",  # claude-3-5-sonnet-20241022, claude-3-opus-20240229
        "max_tokens": 2000,
        "temperature": 0.1
    }
}

print("🤖 LLMエンドポイント設定完了")
print(f"🤖 LLMプロバイダー: {LLM_CONFIG['provider']}")

if LLM_CONFIG['provider'] == 'databricks':
    print(f"🔗 Databricksエンドポイント: {LLM_CONFIG['databricks']['endpoint_name']}")
    thinking_status = "有効" if LLM_CONFIG['databricks'].get('thinking_enabled', True) else "無効"
    print(f"🧠 拡張思考モード: {thinking_status}")
elif LLM_CONFIG['provider'] == 'openai':
    print(f"🔗 OpenAIモデル: {LLM_CONFIG['openai']['model']}")
elif LLM_CONFIG['provider'] == 'azure_openai':
    print(f"🔗 Azure OpenAIデプロイメント: {LLM_CONFIG['azure_openai']['deployment_name']}")
elif LLM_CONFIG['provider'] == 'anthropic':
    print(f"🔗 Anthropicモデル: {LLM_CONFIG['anthropic']['model']}")

print()
print("💡 LLMプロバイダー切り替え例:")
print('   LLM_CONFIG["provider"] = "openai"      # OpenAI GPT-4に切り替え')
print('   LLM_CONFIG["provider"] = "anthropic"   # Anthropic Claudeに切り替え')
print('   LLM_CONFIG["provider"] = "azure_openai" # Azure OpenAIに切り替え')
print()
print("🧠 Databricks拡張思考モード設定例:")
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = True   # 拡張思考モード有効')
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = False  # 拡張思考モード無効')
print()

# 必要なライブラリのインポート
import requests
import os
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

# MAGIC %md
# MAGIC ## 📂 セル3: SQLプロファイラーJSONファイル読み込み関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - SQLプロファイラーJSONファイルの読み込み
# MAGIC - DBFS/FileStore/ローカルパスの自動判別
# MAGIC - ファイルサイズとデータ情報の表示

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

# MAGIC %md
# MAGIC ## 📊 セル4: パフォーマンスメトリクス抽出関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - SQLプロファイラーデータからのメトリクス抽出
# MAGIC - クエリ基本情報の取得
# MAGIC - 全体/ステージ/ノード別パフォーマンス指標の計算
# MAGIC - スピル検出とボトルネック指標の分析

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
                # Photon利用状況の分析（Photon実行時間/タスク合計時間）
                "photon_enabled": query_metrics.get('photonTotalTimeMs', 0) > 0,
                "photon_utilization_ratio": min(query_metrics.get('photonTotalTimeMs', 0) / max(query_metrics.get('taskTotalTimeMs', 1), 1), 1.0)
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
                    
                    # 重要なメトリクスのみ詳細抽出（スピル関連キーワード追加・label対応）
                    detailed_metrics = {}
                    for metric in node.get('metrics', []):
                        metric_key = metric.get('key', '')
                        metric_label = metric.get('label', '')
                        
                        # キーワードをkeyとlabelの両方で確認
                        key_keywords = ['TIME', 'MEMORY', 'ROWS', 'BYTES', 'DURATION', 'PEAK', 'CUMULATIVE', 'EXCLUSIVE', 
                                       'SPILL', 'DISK', 'PRESSURE', 'SINK']
                        
                        # metric_keyまたはmetric_labelに重要なキーワードが含まれる場合に抽出
                        is_important_metric = (
                            any(keyword in metric_key.upper() for keyword in key_keywords) or
                            any(keyword in metric_label.upper() for keyword in key_keywords)
                        )
                        
                        if is_important_metric:
                            # メトリクス名として、labelが有効な場合はlabelを使用、そうでなければkeyを使用
                            metric_name = metric_label if metric_label and metric_label != 'UNKNOWN_KEY' else metric_key
                            detailed_metrics[metric_name] = {
                                'value': metric.get('value', 0),
                                'label': metric_label,
                                'type': metric.get('metricType', ''),
                                'original_key': metric_key,  # 元のキー名を保存
                                'display_name': metric_name  # 表示用の名前
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

# MAGIC %md
# MAGIC ## 🏷️ セル5: ノード名解析・改善関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 汎用的なノード名（Whole Stage Codegen等）の具体化
# MAGIC - 関連ノードの検索と最適な処理名の選択
# MAGIC - Photon情報やテーブル情報の付加
# MAGIC - 処理名の意味的な改善

# COMMAND ----------

def get_meaningful_node_name(node: Dict[str, Any], extracted_metrics: Dict[str, Any]) -> str:
    """
    より意味のあるノード名を取得する関数
    汎用的な名前（Whole Stage Codegenなど）を具体的な処理名に変換
    """
    original_name = node.get('name', '')
    node_id = node.get('node_id', node.get('id', ''))
    node_tag = node.get('tag', '')
    
    # メタデータから詳細情報を取得
    metadata = node.get('metadata', [])
    metadata_info = {}
    for meta in metadata:
        key = meta.get('key', '')
        value = meta.get('value', '')
        label = meta.get('label', '')
        if value:
            metadata_info[key] = value
    
    # 1. 汎用的な名前を具体的な名前に置き換え
    if 'whole stage codegen' in original_name.lower():
        # より具体的な処理名を推測するためのヒューリスティック
        
        # ノードIDベースでの関連性を推測（隣接ID）
        node_id_num = None
        try:
            node_id_num = int(node_id) if node_id else None
        except:
            pass
        
        if node_id_num:
            # 同じファイル内の近いIDの具体的な処理を探す
            all_nodes = extracted_metrics.get('node_metrics', [])
            nearby_specific_nodes = []
            
            for other_node in all_nodes:
                other_id = other_node.get('node_id', '')
                other_name = other_node.get('name', '')
                
                try:
                    other_id_num = int(other_id) if other_id else None
                    if other_id_num and abs(other_id_num - node_id_num) <= 10:  # 近隣10個以内
                        if is_specific_process_name(other_name):
                            nearby_specific_nodes.append(other_name)
                except:
                    continue
            
            # 最も具体的な処理名を選択
            if nearby_specific_nodes:
                specific_name = get_most_specific_process_name_from_list(nearby_specific_nodes)
                if specific_name and specific_name != original_name:
                    return f"{specific_name} (Whole Stage Codegen)"
        
        # フォールバック: tagからより具体的な情報を抽出
        if 'CODEGEN' in node_tag:
            # メタデータから子タグ情報を確認
            child_tag = metadata_info.get('CHILD_TAG', '')
            if child_tag and child_tag != 'Child':
                return f"Whole Stage Codegen ({child_tag})"
    
    # 2. より具体的なタグ情報をノード名に反映
    tag_to_name_mapping = {
        'PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC': 'Photon Shuffle Exchange',
        'PHOTON_GROUPING_AGG_EXEC': 'Photon Grouping Aggregate', 
        'UNKNOWN_DATA_SOURCE_SCAN_EXEC': 'Data Source Scan',
        'HASH_AGGREGATE_EXEC': 'Hash Aggregate',
        'WHOLE_STAGE_CODEGEN_EXEC': 'Whole Stage Codegen'
    }
    
    if node_tag in tag_to_name_mapping:
        mapped_name = tag_to_name_mapping[node_tag]
        if mapped_name != original_name and mapped_name != 'Whole Stage Codegen':
            # タグの方がより具体的な場合は使用
            enhanced_name = mapped_name
        else:
            enhanced_name = original_name
    else:
        enhanced_name = original_name
    
    # 3. メタデータから処理の詳細を追加
    
    # データベース・テーブル情報を追加
    if 'SCAN_TABLE' in metadata_info:
        table_name = metadata_info['SCAN_TABLE']
        if 'scan' in enhanced_name.lower():
            enhanced_name = f"Scan {table_name}"
    
    # Photon情報を追加
    if 'IS_PHOTON' in metadata_info and metadata_info['IS_PHOTON'] == 'true':
        if not enhanced_name.startswith('Photon'):
            enhanced_name = f"Photon {enhanced_name}"
    
    return enhanced_name

def find_related_specific_nodes(target_node_id: str, nodes: list, edges: list) -> list:
    """指定ノードに関連する具体的な処理ノードを検索"""
    
    # エッジから関連ノードを特定
    related_node_ids = set()
    
    # 直接接続されているノード
    for edge in edges:
        from_id = edge.get('fromId', '')
        to_id = edge.get('toId', '')
        
        if from_id == target_node_id:
            related_node_ids.add(to_id)
        elif to_id == target_node_id:
            related_node_ids.add(from_id)
    
    # 関連ノードの詳細を取得
    related_nodes = []
    for node in nodes:
        node_id = node.get('id', '')
        if node_id in related_node_ids:
            node_name = node.get('name', '')
            # 具体的な処理名を持つノードのみ選択
            if is_specific_process_name(node_name):
                related_nodes.append(node)
    
    return related_nodes

def is_specific_process_name(name: str) -> bool:
    """具体的な処理名かどうかを判定"""
    specific_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project', 'join',
        'aggregate', 'sort', 'exchange', 'broadcast', 'scan', 'union'
    ]
    
    generic_keywords = [
        'whole stage codegen', 'stage', 'query', 'result'
    ]
    
    name_lower = name.lower()
    
    # 具体的なキーワードを含む場合
    for keyword in specific_keywords:
        if keyword in name_lower:
            return True
    
    # 汎用的なキーワードのみの場合は除外
    for keyword in generic_keywords:
        if keyword in name_lower and len(name_lower.split()) <= 3:
            return False
    
    return True

def get_most_specific_process_name(nodes: list) -> str:
    """最も具体的な処理名を選択"""
    if not nodes:
        return ""
    
    # 優先順位: より具体的で意味のある処理名
    priority_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project',
        'hash join', 'broadcast join', 'sort merge join',
        'hash aggregate', 'sort aggregate', 'grouping aggregate'
    ]
    
    for keyword in priority_keywords:
        for node in nodes:
            node_name = node.get('name', '').lower()
            if keyword in node_name:
                return node.get('name', '')
    
    # フォールバック: 最初の具体的なノード名
    for node in nodes:
        node_name = node.get('name', '')
        if is_specific_process_name(node_name):
            return node_name
    
    return ""

def get_most_specific_process_name_from_list(node_names: list) -> str:
    """ノード名のリストから最も具体的な処理名を選択"""
    if not node_names:
        return ""
    
    # 優先順位: より具体的で意味のある処理名
    priority_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project',
        'hash join', 'broadcast join', 'sort merge join',
        'hash aggregate', 'sort aggregate', 'grouping aggregate'
    ]
    
    for keyword in priority_keywords:
        for name in node_names:
            if keyword in name.lower():
                return name
    
    # フォールバック: 最初の具体的なノード名
    for name in node_names:
        if is_specific_process_name(name):
            return name
    
    return ""

print("✅ 関数定義完了: get_meaningful_node_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 セル6: ボトルネック指標計算関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 実行時間とコンパイル時間の比率分析
# MAGIC - キャッシュ効率とデータ処理効率の計算
# MAGIC - Photon利用率の分析
# MAGIC - スピル検出とシャッフル/並列度の問題特定

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
    
    # Photon使用率（タスク実行時間に対する割合）
    task_time = overall.get('task_total_time_ms', 0)
    photon_time = overall.get('photon_total_time_ms', 0)
    if task_time > 0:
        indicators['photon_ratio'] = min(photon_time / task_time, 1.0)  # 最大100%に制限
    else:
        indicators['photon_ratio'] = 0.0
    
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

# MAGIC %md
# MAGIC ## 🧬 セル7: Liquid Clustering分析関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - プロファイラーデータからのカラム情報抽出
# MAGIC - フィルター、JOIN、GROUP BY条件の分析
# MAGIC - データスキューとパフォーマンス影響の評価
# MAGIC - クラスタリング推奨カラムの特定

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

# MAGIC %md
# MAGIC ## 🤖 セル8: LLMによるボトルネック分析関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 抽出されたメトリクスのLLM分析用フォーマット
# MAGIC - 複数LLMプロバイダーの対応（Databricks/OpenAI/Azure/Anthropic）
# MAGIC - 日本語での詳細な分析レポート生成
# MAGIC - エラーハンドリングとフォールバック分析

# COMMAND ----------

def analyze_bottlenecks_with_llm(metrics: Dict[str, Any]) -> str:
    """
    設定されたLLMエンドポイントを使用してボトルネック分析を行う
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
    photon_utilization_ratio = metrics['overall_metrics'].get('photon_utilization_ratio', 0)
    photon_utilization = min(photon_utilization_ratio * 100, 100.0)  # 最大100%に制限
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
        # 設定されたLLMプロバイダーに基づいて分析実行
        provider = LLM_CONFIG["provider"]
        print(f"🤖 {provider}エンドポイントに分析リクエストを送信中...")
        print("⏳ 大きなデータのため処理に時間がかかる場合があります...")
        
        # プロバイダー別の処理
        if provider == "databricks":
            return _call_databricks_llm(analysis_prompt)
        elif provider == "openai":
            return _call_openai_llm(analysis_prompt)
        elif provider == "azure_openai":
            return _call_azure_openai_llm(analysis_prompt)
        elif provider == "anthropic":
            return _call_anthropic_llm(analysis_prompt)
        else:
            return f"❌ サポートされていないLLMプロバイダー: {provider}"
            
    except Exception as e:
        error_msg = f"分析エラー: {str(e)}"
        print(f"❌ {error_msg}")
        
        # フォールバック: 基本的な分析結果を提供
        fallback_analysis = f"""
🔧 **基本的なボトルネック分析結果** ({provider} LLM利用不可のため簡易版)

## 📊 パフォーマンス概要
- **実行時間**: {total_time_sec:.1f}秒
- **読み込みデータ量**: {read_gb:.1f}GB  
- **キャッシュ効率**: {cache_ratio:.1f}%
- **データ選択性**: {data_selectivity:.1f}%

## ⚡ Photonエンジン分析
- **Photon有効**: {'はい' if photon_enabled else 'いいえ'}
- **Photon利用率**: {min(photon_utilization, 100.0):.1f}%
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

**注意**: {provider} LLMエンドポイントの接続に問題があります。詳細な分析は手動で実施してください。
        """
        return fallback_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 セル9: 個別LLMプロバイダー接続関数
# MAGIC 
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - Databricks Model Serving エンドポイント接続
# MAGIC - OpenAI API 接続
# MAGIC - Azure OpenAI API 接続
# MAGIC - Anthropic API 接続
# MAGIC - 各プロバイダーのエラーハンドリング

# COMMAND ----------

def _call_databricks_llm(prompt: str) -> str:
    """Databricks Model Serving APIを呼び出す"""
    try:
        # Databricksトークンの取得
        try:
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception:
            token = os.environ.get('DATABRICKS_TOKEN')
            if not token:
                return "❌ Databricksトークンの取得に失敗しました。環境変数DATABRICKS_TOKENを設定してください。"
        
        # ワークスペースURLの取得
        try:
            workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
        
        config = LLM_CONFIG["databricks"]
        endpoint_url = f"https://{workspace_url}/serving-endpoints/{config['endpoint_name']}/invocations"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        # 拡張思考モードが有効な場合は追加
        if config.get("thinking_enabled", True):
            payload["thinking"] = {"type": "enabled"}
        
        # リトライ機能
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
                    if attempt == max_retries - 1:
                        print(f"❌ {error_msg}\nレスポンス: {response.text}")
                        return error_msg
                    else:
                        print(f"⚠️ {error_msg} - リトライします...")
                        continue
                        
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    timeout_msg = "⏰ タイムアウトエラー: Databricksエンドポイントの応答が180秒以内に完了しませんでした。"
                    print(f"❌ {timeout_msg}")
                    return timeout_msg
                else:
                    print(f"⏰ タイムアウト発生 - リトライします... (試行 {attempt + 1}/{max_retries})")
                    continue
                    
    except Exception as e:
        return f"Databricks API呼び出しエラー: {str(e)}"

def _call_openai_llm(prompt: str) -> str:
    """OpenAI APIを呼び出す"""
    try:
        config = LLM_CONFIG["openai"]
        api_key = config["api_key"] or os.environ.get('OPENAI_API_KEY')
        
        if not api_key:
            return "❌ OpenAI APIキーが設定されていません。LLM_CONFIG['openai']['api_key']または環境変数OPENAI_API_KEYを設定してください。"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": config["model"],
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        response = requests.post("https://api.openai.com/v1/chat/completions", 
                               headers=headers, json=payload, timeout=180)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['choices'][0]['message']['content']
            print("✅ OpenAI分析が完了しました")
            return analysis_text
        else:
            return f"OpenAI APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"OpenAI API呼び出しエラー: {str(e)}"

def _call_azure_openai_llm(prompt: str) -> str:
    """Azure OpenAI APIを呼び出す"""
    try:
        config = LLM_CONFIG["azure_openai"]
        api_key = config["api_key"] or os.environ.get('AZURE_OPENAI_API_KEY')
        
        if not api_key or not config["endpoint"] or not config["deployment_name"]:
            return "❌ Azure OpenAI設定が不完全です。api_key、endpoint、deployment_nameを設定してください。"
        
        endpoint_url = f"{config['endpoint']}/openai/deployments/{config['deployment_name']}/chat/completions?api-version={config['api_version']}"
        
        headers = {
            "api-key": api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=180)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['choices'][0]['message']['content']
            print("✅ Azure OpenAI分析が完了しました")
            return analysis_text
        else:
            return f"Azure OpenAI APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"Azure OpenAI API呼び出しエラー: {str(e)}"

def _call_anthropic_llm(prompt: str) -> str:
    """Anthropic APIを呼び出す"""
    try:
        config = LLM_CONFIG["anthropic"]
        api_key = config["api_key"] or os.environ.get('ANTHROPIC_API_KEY')
        
        if not api_key:
            return "❌ Anthropic APIキーが設定されていません。LLM_CONFIG['anthropic']['api_key']または環境変数ANTHROPIC_API_KEYを設定してください。"
        
        headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        }
        
        payload = {
            "model": config["model"],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"],
            "messages": [{"role": "user", "content": prompt}]
        }
        
        response = requests.post("https://api.anthropic.com/v1/messages", 
                               headers=headers, json=payload, timeout=180)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['content'][0]['text']
            print("✅ Anthropic分析が完了しました")
            return analysis_text
        else:
            return f"Anthropic APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"Anthropic API呼び出しエラー: {str(e)}"

print("✅ 関数定義完了: analyze_bottlenecks_with_llm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 セル13: LLMボトルネック分析実行の準備
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたLLMプロバイダーの確認と表示
# MAGIC - 分析開始の準備とメッセージ表示
# MAGIC - プロンプト最適化による安定性向上

# COMMAND ----------

# LLMボトルネック分析実行の準備
provider = LLM_CONFIG["provider"]

print(f"\n🤖 【{provider.upper()} LLM による SQLボトルネック分析を開始します】")
print("=" * 80)

if provider == "databricks":
    endpoint = LLM_CONFIG["databricks"]["endpoint_name"]
    print(f"🔗 Databricks Model Serving エンドポイント: {endpoint}")
    print("⚠️  Model Servingエンドポイントが稼働中である必要があります")
elif provider == "openai":
    model = LLM_CONFIG["openai"]["model"]
    print(f"🔗 OpenAI モデル: {model}")
    print("⚠️  OpenAI APIキーが必要です")
elif provider == "azure_openai":
    deployment = LLM_CONFIG["azure_openai"]["deployment_name"]
    print(f"🤖 Azure OpenAI ({deployment}) によるボトルネック分析を開始します...")
    print("⚠️  Azure OpenAI APIキーとエンドポイントが必要です")
elif provider == "anthropic":
    model = LLM_CONFIG["anthropic"]["model"]
    print(f"🤖 Anthropic ({model}) によるボトルネック分析を開始します...")
    print("⚠️  Anthropic APIキーが必要です")

print("📝 分析プロンプトを簡潔化してタイムアウトリスクを軽減しています...")
print()

# extracted_metrics変数が定義されているかチェック
try:
    extracted_metrics
    print("✅ extracted_metrics変数が確認されました")
    analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)
except NameError:
    print("❌ extracted_metrics変数が定義されていません")
    print("⚠️ セル11 (パフォーマンスメトリクス抽出) を先に実行してください")
    print("📋 正しい実行順序: セル10 → セル11 → セル13")
    print("🔄 デフォルトの分析結果を設定します")
    analysis_result = """
🤖 LLMボトルネック分析結果

❌ 分析に必要なメトリクスデータが見つかりませんでした。

📋 解決方法:
1. セル10でJSONファイルを読み込む
2. セル11でメトリクスを抽出する  
3. このセル（セル13）を再実行する

⚠️ 先にメトリクス抽出を完了してから分析を実行してください。
"""
except Exception as e:
    print(f"❌ LLM分析中にエラーが発生しました: {str(e)}")
    analysis_result = f"LLM分析エラー: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 メイン処理の実行
# MAGIC 
# MAGIC 以下のセルを順番に実行して、SQLプロファイラー分析を実行します。
# MAGIC 
# MAGIC ### 設定について
# MAGIC 
# MAGIC ファイルパスの設定は**セル1**で行います：
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

# MAGIC %md
# MAGIC ## 🚀 セル10: SQLプロファイラーJSONファイル読み込み実行
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたファイルパスからJSONファイルの読み込み
# MAGIC - ファイルサイズと基本情報の表示
# MAGIC - エラーハンドリングと処理停止制御

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

# MAGIC %md
# MAGIC ## 📊 セル11: パフォーマンスメトリクス抽出と概要表示
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - プロファイラーデータからメトリクスの抽出
# MAGIC - クエリ基本情報の表示
# MAGIC - 全体パフォーマンス指標の計算と表示
# MAGIC - Liquid Clusteringの分析結果表示

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

# MAGIC %md
# MAGIC ## 🔍 セル12: ボトルネック指標詳細表示と時間消費TOP10
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - Photon エンジンの利用状況とパフォーマンス分析
# MAGIC - シャッフル操作と並列度の問題検出
# MAGIC - 各種パフォーマンス指標の詳細表示
# MAGIC - 最も時間がかかっている処理TOP10の分析

# COMMAND ----------

# 📋 ボトルネック指標の詳細表示
print("\n" + "=" * 50)
print("🔍 ボトルネック指標詳細")
print("=" * 50)

# Photon関連指標
photon_enabled = overall_metrics.get('photon_enabled', False)
photon_utilization_ratio = overall_metrics.get('photon_utilization_ratio', 0)
photon_utilization = min(photon_utilization_ratio * 100, 100.0)  # 最大100%に制限
photon_emoji = "✅" if photon_enabled and photon_utilization > 80 else "⚠️" if photon_enabled else "❌"

# 利用率に関する詳細情報
if photon_enabled:
    photon_total_ms = overall_metrics.get('photon_total_time_ms', 0)
    task_total_ms = overall_metrics.get('task_total_time_ms', 0)
    print(f"{photon_emoji} Photonエンジン: 有効 (利用率: {photon_utilization:.1f}%)")
    print(f"   📊 Photon実行時間: {photon_total_ms:,} ms | タスク合計時間: {task_total_ms:,} ms")
else:
    print(f"{photon_emoji} Photonエンジン: 無効")

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

# MAGIC %md
# MAGIC ## 💾 セル13: メトリクス保存と時間消費分析
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 抽出したメトリクスのJSON形式での保存
# MAGIC - set型からlist型への変換処理
# MAGIC - 最も時間がかかっている処理TOP10の詳細分析
# MAGIC - スピル検出とデータスキュー分析

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

from datetime import datetime
metrics_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
output_path = f'extracted_metrics_{metrics_timestamp}.json'
try:
    # set型をlist型に変換してからJSONに保存
    serializable_metrics = convert_sets_to_lists(extracted_metrics)
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(serializable_metrics, file, indent=2, ensure_ascii=False)
    print(f"✅ 抽出メトリクスを保存しました: {output_path}")
except Exception as e:
    print(f"⚠️ メトリクス保存でエラーが発生しましたがスキップします: {e}")
    print("✅ 分析は正常に継続されます")

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
        
        # より意味のあるノード名を取得
        raw_node_name = node['name']
        node_name = get_meaningful_node_name(node, extracted_metrics)
        short_name = node_name[:100] + "..." if len(node_name) > 100 else node_name
        
        # 並列度情報の取得
        num_tasks = 0
        for stage in extracted_metrics.get('stage_metrics', []):
            if duration_ms > 0:  # このノードに関連するステージを推定
                num_tasks = max(num_tasks, stage.get('num_tasks', 0))
        
        # ディスクスピルアウトの検出（強化版）
        detailed_metrics = node.get('detailed_metrics', {})
        spill_detected = False
        spill_bytes = 0
        spill_details = []
        

        
        # 強化されたスピル検出ロジック（detailed_metricsと生メトリクスの両方をチェック）
        
        def check_spill_metric(metric_key, metric_label, metric_value):
            """スピルメトリクスかどうかを判定する共通関数"""
            # より具体的なスピル関連メトリクスの検出パターン（改良版）
            spill_patterns = ['SPILL', 'DISK', 'PRESSURE']
            
            # パターンマッチング（より柔軟に）
            is_spill_metric = False
            metric_key_clean = metric_key.upper().replace(' ', '').replace('-', '').replace('_', '')
            metric_label_clean = metric_label.upper().replace(' ', '').replace('-', '').replace('_', '')
            
            # 時間関連のメトリクスを除外（スピル容量ではない）
            time_exclusions = ['TIME', 'DURATION', 'ELAPSED', 'LATENCY', 'DELAY']
            for exclusion in time_exclusions:
                if exclusion in metric_key_clean or exclusion in metric_label_clean:
                    return False
            
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
            
            return is_spill_metric
        
        # 1. detailed_metricsからのスピル検出
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            # スピルメトリクスが検出され、値が0より大きい場合
            if check_spill_metric(metric_key, metric_label, metric_value) and metric_value > 0:
                spill_detected = True
                spill_bytes += metric_value
                spill_details.append({
                    'metric_name': metric_key,
                    'value': metric_value,
                    'label': metric_label,
                    'source': 'detailed_metrics'
                })
        
        # 2. フォールバック: 生メトリクスからの直接検出（detailed_metricsが不完全な場合）
        raw_metrics = node.get('metrics', [])
        for metric in raw_metrics:
            metric_key = metric.get('key', '')
            metric_label = metric.get('label', '')
            metric_value = metric.get('value', 0)
            
            # 既にdetailed_metricsで処理済みかチェック
            already_processed = any(detail['metric_name'] == metric_key for detail in spill_details)
            
            if not already_processed and check_spill_metric(metric_key, metric_label, metric_value) and metric_value > 0:
                spill_detected = True
                spill_bytes += metric_value
                spill_details.append({
                    'metric_name': metric_key,
                    'value': metric_value,
                    'label': metric_label,
                    'source': 'raw_metrics'
                })
        
        # 3. key_metricsからもスピル情報を確認
        key_metrics = node.get('key_metrics', {})
        for key_metric_name, key_metric_value in key_metrics.items():
            if ('spill' in key_metric_name.lower() or 'disk' in key_metric_name.lower()) and key_metric_value > 0:
                spill_detected = True
                spill_bytes += key_metric_value
                spill_details.append({
                    'metric_name': f"key_metrics.{key_metric_name}",
                    'value': key_metric_value,
                    'label': f"Key metric: {key_metric_name}",
                    'source': 'key_metrics'
                })
        
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
        
        # スピル詳細情報（強化版）
        if spill_detected:
            if spill_bytes > 0:
                print(f"    💿 スピル詳細: {spill_bytes/1024/1024:.1f} MB")
            


        
        # ノードIDも表示
        print(f"    🆔 ノードID: {node.get('node_id', node.get('id', 'N/A'))}")
        print()
        
else:
    print("⚠️ ノードメトリクスが見つかりませんでした")

print()

# 🔥 Sparkステージ実行分析
if extracted_metrics['stage_metrics']:
    print("\n🔥 Sparkステージ実行分析")
    print("=" * 60)
    
    stage_metrics = extracted_metrics['stage_metrics']
    total_stages = len(stage_metrics)
    completed_stages = len([s for s in stage_metrics if s.get('status') == 'COMPLETE'])
    failed_stages = len([s for s in stage_metrics if s.get('num_failed_tasks', 0) > 0])
    
    print(f"📊 ステージ概要: 全{total_stages}ステージ (完了:{completed_stages}, 失敗タスクあり:{failed_stages})")
    print()
    
    # ステージを実行時間でソート
    sorted_stages = sorted(stage_metrics, key=lambda x: x.get('duration_ms', 0), reverse=True)
    
    print("⏱️ ステージ実行時間ランキング:")
    print("-" * 60)
    
    for i, stage in enumerate(sorted_stages[:5]):  # TOP5ステージのみ表示
        stage_id = stage.get('stage_id', 'N/A')
        status = stage.get('status', 'UNKNOWN')
        duration_ms = stage.get('duration_ms', 0)
        num_tasks = stage.get('num_tasks', 0)
        failed_tasks = stage.get('num_failed_tasks', 0)
        complete_tasks = stage.get('num_complete_tasks', 0)
        
        # ステータスに応じたアイコン
        if status == 'COMPLETE' and failed_tasks == 0:
            status_icon = "✅"
        elif failed_tasks > 0:
            status_icon = "⚠️"
        else:
            status_icon = "❓"
        
        # 並列度アイコン
        parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
        
        # 実行時間の重要度
        if duration_ms >= 10000:
            time_icon = "🔴"
            severity = "CRITICAL"
        elif duration_ms >= 5000:
            time_icon = "🟠"
            severity = "HIGH"
        elif duration_ms >= 1000:
            time_icon = "🟡"
            severity = "MEDIUM"
        else:
            time_icon = "🟢"
            severity = "LOW"
        
        print(f"{i+1}. {status_icon}{parallelism_icon}{time_icon} ステージ {stage_id} [{severity:8}]")
        print(f"   ⏱️ 実行時間: {duration_ms:,} ms ({duration_ms/1000:.1f} sec)")
        print(f"   🔧 タスク: {complete_tasks}/{num_tasks} 完了 (失敗: {failed_tasks})")
        
        # タスクあたりの平均時間
        if num_tasks > 0:
            avg_task_time = duration_ms / num_tasks
            print(f"   📊 平均タスク時間: {avg_task_time:.1f} ms")
        
        # 効率性評価
        if num_tasks > 0:
            task_efficiency = "高効率" if num_tasks >= 10 and failed_tasks == 0 else "要改善" if failed_tasks > 0 else "標準"
            print(f"   🎯 効率性: {task_efficiency}")
        
        print()
    
    if len(sorted_stages) > 5:
        print(f"... 他 {len(sorted_stages) - 5} ステージ")
    
    # 問題のあるステージのハイライト
    problematic_stages = [s for s in stage_metrics if s.get('num_failed_tasks', 0) > 0 or s.get('duration_ms', 0) > 30000]
    if problematic_stages:
        print("\n🚨 注意が必要なステージ:")
        print("-" * 40)
        for stage in problematic_stages[:3]:
            stage_id = stage.get('stage_id', 'N/A')
            duration_sec = stage.get('duration_ms', 0) / 1000
            failed_tasks = stage.get('num_failed_tasks', 0)
            
            issues = []
            if failed_tasks > 0:
                issues.append(f"失敗タスク{failed_tasks}個")
            if duration_sec > 30:
                issues.append(f"長時間実行({duration_sec:.1f}sec)")
            
            print(f"   ⚠️ ステージ {stage_id}: {', '.join(issues)}")
    
    # DataFrame形式での詳細データ（オプション）
    print(f"\n📋 詳細データ (DataFrame形式):")
    print("-" * 40)
    try:
        import pandas as pd
        # データを整理してわかりやすく表示
        display_data = []
        for stage in stage_metrics:
            display_data.append({
                'ステージID': stage.get('stage_id', 'N/A'),
                'ステータス': stage.get('status', 'UNKNOWN'),
                '実行時間(秒)': round(stage.get('duration_ms', 0) / 1000, 1),
                'タスク数': stage.get('num_tasks', 0),
                '完了タスク': stage.get('num_complete_tasks', 0),
                '失敗タスク': stage.get('num_failed_tasks', 0),
                '平均タスク時間(ms)': round(stage.get('duration_ms', 0) / max(stage.get('num_tasks', 1), 1), 1)
            })
        
        df = pd.DataFrame(display_data)
        df = df.sort_values('実行時間(秒)', ascending=False)
        print(df.to_string(index=False))
        
    except Exception as e:
        print(f"⚠️ DataFrame表示をスキップ: {e}")
        # シンプルな表形式で表示
        print(f"{'ステージID':<10} {'実行時間':<10} {'タスク':<8} {'失敗':<6} {'ステータス'}")
        print("-" * 50)
        for stage in sorted_stages:
            stage_id = str(stage.get('stage_id', 'N/A'))[:8]
            duration_sec = stage.get('duration_ms', 0) / 1000
            num_tasks = stage.get('num_tasks', 0)
            failed = stage.get('num_failed_tasks', 0)
            status = stage.get('status', 'UNKNOWN')[:8]
            print(f"{stage_id:<10} {duration_sec:<10.1f} {num_tasks:<8} {failed:<6} {status}")
    
    print()
else:
    print("\n🔥 Sparkステージ実行分析")
    print("=" * 60)
    print("⚠️ ステージメトリクスが見つかりませんでした")
    print()

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

# 🤖 設定されたLLMエンドポイントを使用してボトルネック分析
provider = LLM_CONFIG["provider"]
if provider == "databricks":
    endpoint_name = LLM_CONFIG["databricks"]["endpoint_name"]
    print(f"🤖 Databricks Model Serving ({endpoint_name}) によるボトルネック分析を開始します...")
    print(f"⚠️  Model Servingエンドポイント '{endpoint_name}' が必要です")
elif provider == "openai":
    model = LLM_CONFIG["openai"]["model"]
    print(f"🤖 OpenAI ({model}) によるボトルネック分析を開始します...")
    print("⚠️  OpenAI APIキーが必要です")
elif provider == "azure_openai":
    deployment = LLM_CONFIG["azure_openai"]["deployment_name"]
    print(f"🤖 Azure OpenAI ({deployment}) によるボトルネック分析を開始します...")
    print("⚠️  Azure OpenAI APIキーとエンドポイントが必要です")
elif provider == "anthropic":
    model = LLM_CONFIG["anthropic"]["model"]
    print(f"🤖 Anthropic ({model}) によるボトルネック分析を開始します...")
    print("⚠️  Anthropic APIキーが必要です")

print("📝 分析プロンプトを簡潔化してタイムアウトリスクを軽減しています...")
print()

analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 セル14: LLMボトルネック分析結果の表示
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたLLMプロバイダーによる詳細分析結果の表示
# MAGIC - ボトルネック特定と改善提案の可視化
# MAGIC - 分析結果の整形と読みやすい表示

# COMMAND ----------

# 📊 分析結果の表示
print("\n" + "=" * 80)
print(f"🎯 【{provider.upper()} LLM による SQLボトルネック分析結果】")
print("=" * 80)
print()
print(analysis_result)
print()
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 セル15: 分析結果の保存と完了サマリー
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - LLM分析結果のテキストファイルへの保存
# MAGIC - 分析対象の基本情報の記録
# MAGIC - 全体処理の完了サマリー表示
# MAGIC - 出力ファイルの一覧表示

# COMMAND ----------

# 💾 分析結果の保存と完了サマリー
from datetime import datetime
result_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
result_output_path = f'bottleneck_analysis_result_{result_timestamp}.txt'
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
print(f"✅ パフォーマンスメトリクス抽出完了 ({output_path})")
print("✅ Databricks Claude 3.7 Sonnetによるボトルネック分析完了")
print(f"✅ 分析結果保存完了 ({result_output_path})")
print()
print("📁 出力ファイル:")
print(f"   📄 {output_path}")
print(f"   📄 {result_output_path}")
print()
print("🚀 分析完了！結果を確認してクエリ最適化にお役立てください。")
print("🎉" * 20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 セル16: SQL最適化関連関数定義
# MAGIC 
# MAGIC このセルでは以下の関数を定義します：
# MAGIC - `extract_original_query_from_profiler_data`: プロファイラーデータからオリジナルクエリを抽出
# MAGIC - `generate_optimized_query_with_llm`: LLM分析結果に基づくクエリ最適化
# MAGIC - `save_optimized_sql_files`: 最適化結果の各種ファイル保存

# COMMAND ----------

def extract_original_query_from_profiler_data(profiler_data: Dict[str, Any]) -> str:
    """
    プロファイラーデータからオリジナルクエリを抽出
    """
    
    # 複数の場所からSQLクエリを探す
    query_candidates = []
    
    # 1. query.queryText から抽出
    if 'query' in profiler_data and 'queryText' in profiler_data['query']:
        query_text = profiler_data['query']['queryText']
        if query_text and query_text.strip():
            query_candidates.append(query_text.strip())
    
    # 2. metadata から抽出
    if 'metadata' in profiler_data:
        metadata = profiler_data['metadata']
        for key, value in metadata.items():
            if 'sql' in key.lower() or 'query' in key.lower():
                if isinstance(value, str) and value.strip():
                    query_candidates.append(value.strip())
    
    # 3. graphs の metadata から抽出
    if 'graphs' in profiler_data:
        for graph in profiler_data['graphs']:
            nodes = graph.get('nodes', [])
            for node in nodes:
                node_metadata = node.get('metadata', [])
                for meta in node_metadata:
                    if meta.get('key', '').upper() in ['SQL', 'QUERY', 'SQL_TEXT']:
                        value = meta.get('value', '')
                        if value and value.strip():
                            query_candidates.append(value.strip())
    
    # 最も長いクエリを選択（通常、最も完全なクエリ）
    if query_candidates:
        original_query = max(query_candidates, key=len)
        return original_query
    
    return ""

def generate_optimized_query_with_llm(original_query: str, analysis_result: str, metrics: Dict[str, Any]) -> str:
    """
    LLM分析結果に基づいてSQLクエリを最適化
    """
    
    # 最適化のためのコンテキスト情報を準備
    optimization_context = []
    
    # ボトルネック情報の抽出
    bottlenecks = metrics.get('bottleneck_indicators', {})
    
    if bottlenecks.get('has_spill', False):
        spill_gb = bottlenecks.get('spill_bytes', 0) / 1024 / 1024 / 1024
        optimization_context.append(f"スピル発生: {spill_gb:.1f}GB - メモリ効率の改善が必要")
    
    if bottlenecks.get('has_shuffle_bottleneck', False):
        optimization_context.append("シャッフルボトルネック - JOINとGROUP BYの最適化が必要")
    
    if bottlenecks.get('cache_hit_ratio', 0) < 0.5:
        optimization_context.append("キャッシュ効率低下 - データアクセスパターンの最適化が必要")
    
    # Liquid Clustering推奨情報
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    recommended_tables = liquid_analysis.get('recommended_tables', {})
    
    clustering_recommendations = []
    for table, info in recommended_tables.items():
        cols = info.get('clustering_columns', [])
        if cols:
            clustering_recommendations.append(f"テーブル {table}: {', '.join(cols)} でクラスタリング推奨")
    
    # 最適化プロンプトの作成
    optimization_prompt = f"""
あなたはDatabricksのSQLパフォーマンス最適化の専門家です。以下の情報を基にSQLクエリを最適化してください。

【元のSQLクエリ】
```sql
{original_query}
```

【パフォーマンス分析結果】
{analysis_result}

【特定されたボトルネック】
{chr(10).join(optimization_context) if optimization_context else "主要なボトルネックは検出されませんでした"}

【Liquid Clustering推奨】
{chr(10).join(clustering_recommendations) if clustering_recommendations else "特別な推奨事項はありません"}

【最適化要求】
1. 上記の分析結果に基づいて、元のSQLクエリを最適化してください
2. 最適化のポイントを具体的に説明してください
3. パフォーマンス向上の見込みを定量的に示してください
4. 実行可能なSQLコードとして出力してください

【出力形式】
## 🚀 最適化されたSQLクエリ

```sql
-- 最適化されたクエリをここに記述
[最適化されたSQL]
```

## 📊 最適化のポイント

1. **[最適化項目1]**: [説明]
2. **[最適化項目2]**: [説明]
3. **[最適化項目3]**: [説明]

## 📈 期待される効果

- **実行時間**: [現在] → [最適化後] (改善率: [XX%])
- **メモリ使用量**: [改善内容]
- **スピル削減**: [改善内容]

注意：実際の環境で実行前に、テストデータでの動作確認を推奨します。
"""

    # 設定されたLLMプロバイダーを使用
    provider = LLM_CONFIG["provider"]
    
    try:
        if provider == "databricks":
            optimized_result = _call_databricks_llm(optimization_prompt)
        elif provider == "openai":
            optimized_result = _call_openai_llm(optimization_prompt)
        elif provider == "azure_openai":
            optimized_result = _call_azure_openai_llm(optimization_prompt)
        elif provider == "anthropic":
            optimized_result = _call_anthropic_llm(optimization_prompt)
        else:
            return "⚠️ 設定されたLLMプロバイダーが認識できません"
        
        return optimized_result
        
    except Exception as e:
        return f"⚠️ SQL最適化の生成中にエラーが発生しました: {str(e)}"

def save_optimized_sql_files(original_query: str, optimized_result: str, metrics: Dict[str, Any]) -> Dict[str, str]:
    """
    最適化されたSQLクエリを実行可能な形でファイルに保存
    """
    
    import re
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    query_id = metrics.get('query_info', {}).get('query_id', 'unknown')
    
    # オリジナルクエリファイルの保存
    original_filename = f"original_query_{timestamp}.sql"
    with open(original_filename, 'w', encoding='utf-8') as f:
        f.write(f"-- オリジナルSQLクエリ\n")
        f.write(f"-- クエリID: {query_id}\n")
        f.write(f"-- 抽出日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- ファイル: {original_filename}\n\n")
        f.write(original_query)
    
    # 最適化されたクエリの抽出と保存
    optimized_filename = f"optimized_query_{timestamp}.sql"
    
    # 最適化結果からSQLコードを抽出
    sql_pattern = r'```sql\s*(.*?)\s*```'
    sql_matches = re.findall(sql_pattern, optimized_result, re.DOTALL | re.IGNORECASE)
    
    optimized_sql = ""
    if sql_matches:
        # 最初に見つかったSQLブロックを使用
        optimized_sql = sql_matches[0].strip()
    else:
        # SQLブロックが見つからない場合は、SQL関連の行を抽出
        lines = optimized_result.split('\n')
        sql_lines = []
        in_sql_section = False
        
        for line in lines:
            if any(keyword in line.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'WITH', 'CREATE']):
                in_sql_section = True
            
            if in_sql_section:
                if line.strip().startswith('#') or line.strip().startswith('*'):
                    in_sql_section = False
                else:
                    sql_lines.append(line)
        
        optimized_sql = '\n'.join(sql_lines).strip()
    
    # 最適化されたクエリファイルの保存
    with open(optimized_filename, 'w', encoding='utf-8') as f:
        f.write(f"-- 最適化されたSQLクエリ\n")
        f.write(f"-- 元クエリID: {query_id}\n")
        f.write(f"-- 最適化日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- ベースクエリ: {original_filename}\n")
        f.write(f"-- ファイル: {optimized_filename}\n\n")
        
        if optimized_sql:
            f.write(optimized_sql)
        else:
            f.write("-- ⚠️ SQLコードの自動抽出に失敗しました\n")
            f.write("-- 以下は最適化分析の全結果です:\n\n")
            f.write(f"/*\n{optimized_result}\n*/")
    
    # 分析レポートファイルの保存
    report_filename = f"optimization_report_{timestamp}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(f"# SQL最適化レポート\n\n")
        f.write(f"**クエリID**: {query_id}\n")
        f.write(f"**最適化日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**オリジナルファイル**: {original_filename}\n")
        f.write(f"**最適化ファイル**: {optimized_filename}\n\n")
        f.write(f"## 最適化分析結果\n\n")
        f.write(optimized_result)
        f.write(f"\n\n## パフォーマンスメトリクス参考情報\n\n")
        
        # 主要メトリクスの追加
        overall_metrics = metrics.get('overall_metrics', {})
        f.write(f"- **実行時間**: {overall_metrics.get('total_time_ms', 0):,} ms\n")
        f.write(f"- **読み込みデータ**: {overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024:.2f} GB\n")
        f.write(f"- **スピル**: {metrics.get('bottleneck_indicators', {}).get('spill_bytes', 0) / 1024 / 1024 / 1024:.2f} GB\n")
    
    # テスト実行用スクリプトの作成
    test_script_filename = f"test_optimized_query_{timestamp}.py"
    with open(test_script_filename, 'w', encoding='utf-8') as f:
        # f-stringの中で三重引用符を含む場合のエスケープ処理
        escaped_original_query = original_query.replace('"""', '\\"""')
        escaped_optimized_sql = optimized_sql.replace('"""', '\\"""') if optimized_sql else '-- SQLコードが抽出できませんでした'
        
        test_script_content = f"""#!/usr/bin/env python3
\"\"\"
最適化されたSQLクエリのテスト実行スクリプト
生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
クエリID: {query_id}
タイムスタンプ: {timestamp}
\"\"\"

# Databricks環境での実行例
def test_optimized_query():
    
    # 必要なライブラリのインポート
    try:
        from pyspark.sql import SparkSession
        import time
    except ImportError as e:
        print(f"⚠️ 必要なライブラリがインストールされていません: {{e}}")
        return
    
    # Sparkセッションの取得
    spark = SparkSession.builder.appName("OptimizedQueryTest").getOrCreate()
    
    print("🚀 最適化されたSQLクエリのテスト実行")
    print("=" * 60)
    
    # オリジナルクエリの実行（オプション）
    print("\\n📊 オリジナルクエリの実行...")
    original_sql = \"\"\"
{escaped_original_query}
    \"\"\"
    
    start_time = time.time()
    try:
        # original_result = spark.sql(original_sql)
        # original_count = original_result.count()
        print("⚠️ オリジナルクエリの実行はコメントアウトされています")
        print("   必要に応じてコメントを解除してください")
        original_execution_time = 0
    except Exception as e:
        print(f"❌ オリジナルクエリの実行エラー: {{e}}")
        original_execution_time = 0
    
    original_execution_time = time.time() - start_time
    
    # 最適化されたクエリの実行
    print("\\n🚀 最適化されたクエリの実行...")
    optimized_sql = \"\"\"
{escaped_optimized_sql}
    \"\"\"
    
    start_time = time.time()
    try:
        # optimized_result = spark.sql(optimized_sql)
        # optimized_count = optimized_result.count()
        print("⚠️ 最適化クエリの実行はコメントアウトされています")
        print("   動作確認後、コメントを解除してください")
        optimized_execution_time = 0
    except Exception as e:
        print(f"❌ 最適化クエリの実行エラー: {{e}}")
        optimized_execution_time = 0
    
    optimized_execution_time = time.time() - start_time
    
    # 結果の比較
    print("\\n📊 実行結果の比較:")
    print(f"   オリジナル実行時間: {{original_execution_time:.2f}} 秒")
    print(f"   最適化実行時間: {{optimized_execution_time:.2f}} 秒")
    
    if original_execution_time > 0 and optimized_execution_time > 0:
        improvement = ((original_execution_time - optimized_execution_time) / original_execution_time) * 100
        print(f"   パフォーマンス改善: {{improvement:.1f}}%")
    
    print("\\n✅ テスト完了")

if __name__ == "__main__":
    test_optimized_query()
"""
        f.write(test_script_content)
    
    return {
        'original_file': original_filename,
        'optimized_file': optimized_filename,
        'report_file': report_filename,
        'test_script': test_script_filename
    }

print("✅ 関数定義完了: SQL最適化関連関数")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 セル17: SQLクエリ最適化の実行（ステップ1: クエリ抽出）
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - プロファイラーデータからオリジナルクエリの抽出
# MAGIC - 抽出されたクエリの詳細表示（64KBまで）
# MAGIC - フォールバック処理（サンプルクエリの設定）

# COMMAND ----------

# 🚀 SQLクエリ最適化の実行
print("\n" + "🚀" * 20)
print("🔧 【SQLクエリ最適化の実行】")
print("🚀" * 20)

# 1. オリジナルクエリの抽出
print("\n📋 ステップ1: オリジナルクエリの抽出")
print("-" * 40)

original_query = extract_original_query_from_profiler_data(profiler_data)

if original_query:
    print(f"✅ オリジナルクエリを抽出しました ({len(original_query)} 文字)")
    print(f"🔍 クエリプレビュー:")
    # 64KB (65536文字) まで表示
    max_display_chars = 65536
    if len(original_query) > max_display_chars:
        preview = original_query[:max_display_chars] + f"\n... (残り {len(original_query) - max_display_chars} 文字は省略)"
    else:
        preview = original_query
    print(f"   {preview}")
else:
    print("⚠️ オリジナルクエリが見つかりませんでした")
    print("   手動でクエリを設定してください")
    
    # フォールバック: サンプルクエリを設定
    original_query = """
    -- サンプルクエリ（実際のクエリに置き換えてください）
    SELECT 
        customer_id,
        SUM(order_amount) as total_amount,
        COUNT(*) as order_count
    FROM orders 
    WHERE order_date >= '2023-01-01'
    GROUP BY customer_id
    ORDER BY total_amount DESC
    LIMIT 100
    """
    print(f"📝 サンプルクエリを設定しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 セル18: LLMによるSQL最適化（ステップ2: 最適化実行）
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - LLMを使用した抽出クエリの最適化
# MAGIC - 最適化結果の詳細表示（1000行まで）
# MAGIC - エラーハンドリングと代替処理

# COMMAND ----------

# 🤖 ステップ2: LLMによるSQL最適化
print("\n🤖 ステップ2: LLMによるSQL最適化")
print("-" * 40)

if original_query.strip():
    print(f"🔄 {provider.upper()} を使用してクエリを最適化中...")
    
    optimized_result = generate_optimized_query_with_llm(
        original_query, 
        analysis_result, 
        extracted_metrics
    )
    
    if optimized_result and not optimized_result.startswith("⚠️"):
        print("✅ SQL最適化が完了しました")
        print(f"📄 最適化結果の詳細:")
        
        # 最適化結果の詳細を表示（1000行まで）
        lines = optimized_result.split('\n')
        max_display_lines = 1000
        
        if len(lines) <= max_display_lines:
            # 全行表示
            for line in lines:
                print(f"   {line}")
        else:
            # 1000行まで表示
            for line in lines[:max_display_lines]:
                print(f"   {line}")
            print(f"   ... (残り {len(lines) - max_display_lines} 行は省略、詳細は保存ファイルを確認)")
        
    else:
        print(f"❌ SQL最適化に失敗しました")
        print(f"   エラー: {optimized_result}")
        optimized_result = "最適化の生成に失敗しました。手動での最適化を検討してください。"
else:
    print("⚠️ オリジナルクエリが空のため、最適化をスキップします")
    optimized_result = "オリジナルクエリが見つからないため、最適化できませんでした。"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 セル19: 最適化結果の保存（ステップ3: ファイル生成）
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 最適化されたSQLクエリのファイル保存
# MAGIC - オリジナルクエリ、最適化クエリ、レポート、テストスクリプトの生成
# MAGIC - 生成ファイルの詳細情報表示

# COMMAND ----------

# 💾 ステップ3: 最適化結果の保存
print("\n💾 ステップ3: 最適化結果の保存")
print("-" * 40)

# 必要な変数が定義されているかチェックし、デフォルト値を設定
missing_variables = []

# original_query のチェック
try:
    original_query
except NameError:
    missing_variables.append("original_query (セル17を実行してください)")
    original_query = ""

# optimized_result のチェック  
try:
    optimized_result
except NameError:
    missing_variables.append("optimized_result (セル18を実行してください)")
    optimized_result = ""

# extracted_metrics のチェック
try:
    extracted_metrics
except NameError:
    missing_variables.append("extracted_metrics (セル11を実行してください)")
    # デフォルト値として最小限の構造を設定
    extracted_metrics = {
        'query_info': {'query_id': 'unknown'},
        'overall_metrics': {},
        'bottleneck_indicators': {}
    }

if missing_variables:
    print("❌ 必要な変数が定義されていません:")
    for var in missing_variables:
        print(f"   • {var}")
    print("\n⚠️ 上記のセルを先に実行してから、このセルを再実行してください。")
    print("📋 正しい実行順序: セル10 → セル11 → ... → セル17 → セル18 → セル19")
    print("\n🔄 デフォルト値を使用して処理を継続します。")

# 変数が存在する（またはデフォルト値が設定された）場合の処理
if original_query.strip() and optimized_result.strip():
    print("📁 ファイル生成中...")
    
    try:
        saved_files = save_optimized_sql_files(
            original_query,
            optimized_result,
            extracted_metrics
        )
        
        print("✅ 以下のファイルを生成しました:")
        for file_type, filename in saved_files.items():
            file_type_jp = {
                'original_file': 'オリジナルSQLクエリ',
                'optimized_file': '最適化SQLクエリ',
                'report_file': '最適化レポート',
                'test_script': 'テスト実行スクリプト'
            }
            print(f"   📄 {file_type_jp.get(file_type, file_type)}: {filename}")
        
        # ファイルサイズの確認
        import os
        print(f"\n📊 生成ファイルの詳細:")
        for file_type, filename in saved_files.items():
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                print(f"   {filename}: {file_size:,} bytes")
            else:
                print(f"   ⚠️ {filename}: ファイルが見つかりません")
        
    except Exception as e:
        print(f"❌ ファイル生成中にエラーが発生しました: {str(e)}")
        print("⚠️ 空のファイルリストを設定します。")
        saved_files = {}
        
else:
    print("⚠️ クエリまたは最適化結果が不完全なため、ファイル保存をスキップしました")
    saved_files = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 セル20: テスト実行の準備（ステップ4: 実行ガイド）
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 生成されたファイルの使用方法説明
# MAGIC - テスト実行の手順ガイド
# MAGIC - 自動テストスクリプトの実行方法
# MAGIC - 重要な注意事項の表示

# COMMAND ----------

# 🧪 ステップ4: テスト実行の準備
print("\n🧪 ステップ4: テスト実行の準備")
print("-" * 40)

# saved_files変数が定義されているかチェック
try:
    saved_files
except NameError:
    print("❌ saved_files変数が定義されていません。")
    print("⚠️ セル19 (最適化結果の保存) を先に実行してください。")
    saved_files = {}

if saved_files:
    test_script = saved_files.get('test_script', '')
    optimized_file = saved_files.get('optimized_file', '')
    
    print("🚀 テスト実行の手順:")
    print("1. 生成されたSQLファイルの内容を確認")
    print("2. 必要に応じてクエリを手動調整")
    print("3. テストデータセットでの実行")
    print("4. パフォーマンス測定と比較")
    
    if test_script:
        print(f"\n🔧 自動テストスクリプトの実行:")
        print(f"   python {test_script}")
    
    if optimized_file:
        print(f"\n📝 最適化されたSQLの手動実行:")
        print(f"   # Databricks SQLエディタで {optimized_file} を実行")
        print(f"   # または以下のPythonコードを使用:")
        print(f"   spark.sql(open('{optimized_file}').read()).show()")
    
    print(f"\n⚠️ 重要な注意事項:")
    print(f"   • 本番環境での実行前に、必ずテスト環境で検証してください")
    print(f"   • データベースの構造やサイズによって結果は変わる可能性があります")
    print(f"   • クエリプランの確認: EXPLAIN 文を使用してください")

else:
    print("⚠️ テスト実行用ファイルが生成されていません")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 セル21: 最終処理完了サマリー
# MAGIC 
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 全処理の完了状況確認
# MAGIC - 生成された全ファイルの一覧表示
# MAGIC - 次のステップの提示
# MAGIC - 処理完了の総合報告

# COMMAND ----------

# 📊 最終サマリーの更新
print("\n" + "🎉" * 25)
print("🏁 【SQL最適化処理完了サマリー】")
print("🎉" * 25)

print("✅ SQLプロファイラーJSONファイル読み込み完了")
print("✅ パフォーマンスメトリクス抽出完了")
print("✅ Databricks Claude 3.7 Sonnetによるボトルネック分析完了")
print("✅ 分析結果保存完了")
print("✅ オリジナルクエリ抽出完了")
print("✅ LLMによるSQL最適化完了")
print("✅ 最適化結果ファイル生成完了")
print("✅ テスト実行スクリプト生成完了")

# 必要な変数が定義されているかチェック
missing_summary_vars = []

try:
    output_path
except NameError:
    missing_summary_vars.append("output_path (セル13を実行してください)")

try:
    result_output_path
except NameError:
    missing_summary_vars.append("result_output_path (セル15を実行してください)")

try:
    saved_files
except NameError:
    missing_summary_vars.append("saved_files (セル19を実行してください)")
    saved_files = {}

print(f"\n📁 出力ファイル一覧:")

if 'output_path' in globals():
    print(f"   📄 パフォーマンス分析: {output_path}")
else:
    print("   📄 パフォーマンス分析: (セル13を実行してください)")

if 'result_output_path' in globals():
    print(f"   📄 ボトルネック分析レポート: {result_output_path}")
else:
    print("   📄 ボトルネック分析レポート: (セル15を実行してください)")

if saved_files:
    for file_type, filename in saved_files.items():
        file_type_jp = {
            'original_file': '📄 オリジナルSQL',
            'optimized_file': '🚀 最適化SQL',
            'report_file': '📊 最適化レポート',
            'test_script': '🧪 テストスクリプト'
        }
        icon_name = file_type_jp.get(file_type, f"📄 {file_type}")
        print(f"   {icon_name}: {filename}")

print(f"\n🚀 次のステップ:")
print(f"   1. 生成されたSQLファイルを確認")
print(f"   2. テスト環境での動作確認")
print(f"   3. パフォーマンス測定")
print(f"   4. 本番環境への適用検討")

print("🎉" * 25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 セル22: 追加の使用方法とカスタマイズ
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
# MAGIC - **LLMプロバイダー**: `LLM_CONFIG` でプロバイダーとAPIキーを切り替え
# MAGIC - **メトリクス抽出**: `extract_performance_metrics` 関数内の重要キーワードリスト
# MAGIC - **分析プロンプト**: `analyze_bottlenecks_with_llm` 関数内の分析指示
# MAGIC - **表示形式**: emoji と出力フォーマットの調整
# MAGIC 
# MAGIC ### 🔍 エラー対処方法
# MAGIC 
# MAGIC 1. **LLMエンドポイントエラー**: 
# MAGIC    - Databricks: Model Servingエンドポイントの状態確認
# MAGIC    - OpenAI/Azure/Anthropic: APIキーとクォータ確認
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
# MAGIC ## 🎯 セル23: このNotebookの使用方法
# MAGIC 
# MAGIC 1. **LLM設定**: セル2で `LLM_CONFIG` のプロバイダーとAPIキーを設定
# MAGIC 2. **ファイル準備**: SQLプロファイラーJSONファイルをVolumes、FileStore、またはDBFSにアップロード
# MAGIC 3. **パス設定**: セル1で `JSON_FILE_PATH` を実際のファイルパスに変更
# MAGIC 4. **実行**: 「Run All」をクリックまたは各セルを順番に実行
# MAGIC 5. **結果確認**: 抽出されたメトリクスとAI分析結果を確認
# MAGIC 
# MAGIC **📧 サポート**: 問題が発生した場合は、エラーメッセージとDatabricks環境情報をお知らせください。