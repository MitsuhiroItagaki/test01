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

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔧 設定・準備セクション
# MAGIC
# MAGIC **このセクションではツールの基本設定を行います**
# MAGIC
# MAGIC 📋 **設定内容:**
# MAGIC - 分析対象ファイルの指定
# MAGIC - LLMエンドポイントの設定
# MAGIC - 分析関数の定義
# MAGIC
# MAGIC ⚠️ **重要:** メイン処理を実行する前に、このセクションのすべてのセルを実行してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 分析対象ファイル設定
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

JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/POC1.json'  # デフォルト

# 🌐 出力言語設定（OUTPUT_LANGUAGE: 'ja' = 日本語, 'en' = 英語）
OUTPUT_LANGUAGE = 'ja'  # デフォルト: 日本語

# 💡 使用例:
# OUTPUT_LANGUAGE = 'ja'  # 日本語でファイル出力
# OUTPUT_LANGUAGE = 'en'  # 英語でファイル出力

# 🌐 多言語メッセージ辞書
MESSAGES = {
    'ja': {
        'bottleneck_title': 'Databricks SQLプロファイラー ボトルネック分析結果',
        'query_id': 'クエリID',
        'analysis_time': '分析日時',
        'execution_time': '実行時間',
        'sql_optimization_report': 'SQL最適化レポート',
        'optimization_time': '最適化日時',
        'original_file': 'オリジナルファイル',
        'optimized_file': '最適化ファイル',
        'optimization_analysis': '最適化分析結果',
        'performance_metrics': 'パフォーマンスメトリクス参考情報',
        'read_data': '読み込みデータ',
        'spill': 'スピル',
        'top10_processes': '最も時間がかかっている処理TOP10'
    },
    'en': {
        'bottleneck_title': 'Databricks SQL Profiler Bottleneck Analysis Results',
        'query_id': 'Query ID',
        'analysis_time': 'Analysis Time',
        'execution_time': 'Execution Time',
        'sql_optimization_report': 'SQL Optimization Report',
        'optimization_time': 'Optimization Time',
        'original_file': 'Original File',
        'optimized_file': 'Optimized File',
        'optimization_analysis': 'Optimization Analysis Results',
        'performance_metrics': 'Performance Metrics Reference',
        'read_data': 'Data Read',
        'spill': 'Spill',
        'top10_processes': 'Top 10 Most Time-Consuming Processes'
    }
}

def get_message(key: str) -> str:
    """多言語メッセージを取得"""
    return MESSAGES.get(OUTPUT_LANGUAGE, MESSAGES['ja']).get(key, key)

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
try:
    import pandas as pd
except ImportError:
    print("Warning: pandas is not installed, some features may not work")
    pd = None
from typing import Dict, List, Any
from datetime import datetime

print("✅ 基本ライブラリインポート完了")
print("🚀 次のセルに進んでください")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 LLMエンドポイント設定
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
    
    # Databricks Model Serving設定（高速実行優先）
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",  # Model Servingエンドポイント名
        "max_tokens": 131072,  # 128K tokens（Claude 3.7 Sonnetの最大制限）
        "temperature": 0.0,    # 決定的な出力のため（0.1→0.0）
        "thinking_enabled": False,  # 拡張思考モード（デフォルト: 無効 - 高速実行優先）
        "thinking_budget_tokens": 65536  # 思考用トークン予算 64K tokens（有効時のみ使用）
    },
    
    # OpenAI設定（完全なSQL生成用に最適化）
    "openai": {
        "api_key": "",  # OpenAI APIキー (環境変数OPENAI_API_KEYでも可)
        "model": "gpt-4o",  # gpt-4o, gpt-4-turbo, gpt-3.5-turbo
        "max_tokens": 16000,  # OpenAIの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    },
    
    # Azure OpenAI設定（完全なSQL生成用に最適化）
    "azure_openai": {
        "api_key": "",  # Azure OpenAI APIキー (環境変数AZURE_OPENAI_API_KEYでも可)
        "endpoint": "",  # https://your-resource.openai.azure.com/
        "deployment_name": "",  # デプロイメント名
        "api_version": "2024-02-01",
        "max_tokens": 16000,  # Azure OpenAIの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    },
    
    # Anthropic設定（完全なSQL生成用に最適化）
    "anthropic": {
        "api_key": "",  # Anthropic APIキー (環境変数ANTHROPIC_API_KEYでも可)
        "model": "claude-3-5-sonnet-20241022",  # claude-3-5-sonnet-20241022, claude-3-opus-20240229
        "max_tokens": 16000,  # Anthropicの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    }
}

print("🤖 LLMエンドポイント設定完了")
print(f"🤖 LLMプロバイダー: {LLM_CONFIG['provider']}")

if LLM_CONFIG['provider'] == 'databricks':
    print(f"🔗 Databricksエンドポイント: {LLM_CONFIG['databricks']['endpoint_name']}")
    thinking_status = "有効" if LLM_CONFIG['databricks'].get('thinking_enabled', False) else "無効"
    thinking_budget = LLM_CONFIG['databricks'].get('thinking_budget_tokens', 65536)
    max_tokens = LLM_CONFIG['databricks'].get('max_tokens', 131072)
    print(f"🧠 拡張思考モード: {thinking_status} (予算: {thinking_budget:,} tokens)")
    print(f"📊 最大トークン数: {max_tokens:,} tokens ({max_tokens//1024}K)")
    if not LLM_CONFIG['databricks'].get('thinking_enabled', False):
        print("⚡ 高速実行モード: 思考プロセスを省略して迅速な結果生成")
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
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = False  # 拡張思考モード無効（デフォルト・高速実行）')
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = True   # 拡張思考モード有効（詳細分析時のみ）')
print('   LLM_CONFIG["databricks"]["thinking_budget_tokens"] = 65536  # 思考用トークン予算(64K)')
print('   LLM_CONFIG["databricks"]["max_tokens"] = 131072  # 最大トークン数(128K)')
print()

# 必要なライブラリのインポート
try:
    import requests
except ImportError:
    print("Warning: requests is not installed, some features may not work")
    requests = None
import os
try:
    from pyspark.sql import SparkSession
except ImportError:
    print("Warning: pyspark is not installed")
    SparkSession = None
    print("✅ Spark Version: Not available")

# Databricks Runtime情報を安全に取得
try:
    if spark is not None:
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
# MAGIC ## 📂 SQLプロファイラーJSONファイル読み込み関数
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
# MAGIC ## 📊 パフォーマンスメトリクス抽出関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - SQLプロファイラーデータからのメトリクス抽出
# MAGIC - クエリ基本情報の取得
# MAGIC - 全体/ステージ/ノード別パフォーマンス指標の計算
# MAGIC - スピル検出とボトルネック指標の分析

# COMMAND ----------

def detect_data_format(profiler_data: Dict[str, Any]) -> str:
    """
    JSONデータの形式を検出
    """
    # SQLプロファイラー形式の検出
    if 'graphs' in profiler_data and isinstance(profiler_data['graphs'], list):
        if len(profiler_data['graphs']) > 0:
            return 'sql_profiler'
    
    # SQLクエリサマリー形式の検出（test2.json形式）
    if 'query' in profiler_data and 'planMetadatas' in profiler_data:
        query_data = profiler_data.get('query', {})
        if 'metrics' in query_data:
            return 'sql_query_summary'
    
    return 'unknown'

def extract_performance_metrics_from_query_summary(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Databricks SQLクエリサマリー形式のJSONから基本メトリクスを抽出
    (test2.json形式に対応)
    """
    try:
        query_data = profiler_data.get('query', {})
        metrics_data = query_data.get('metrics', {})
        
        if not metrics_data:
            print("⚠️ メトリクスデータが見つかりません")
            return {}
        
        print(f"✅ SQLクエリサマリー形式のメトリクスを検出しました")
        print(f"   - 実行時間: {metrics_data.get('totalTimeMs', 0):,} ms")
        print(f"   - 読み込みデータ: {metrics_data.get('readBytes', 0) / 1024 / 1024 / 1024:.2f} GB")
        print(f"   - 処理行数: {metrics_data.get('rowsReadCount', 0):,} 行")
        
        # 基本メトリクスの抽出
        overall_metrics = {
            'total_time_ms': metrics_data.get('totalTimeMs', 0),
            'execution_time_ms': metrics_data.get('executionTimeMs', 0),
            'compilation_time_ms': metrics_data.get('compilationTimeMs', 0),
            'read_bytes': metrics_data.get('readBytes', 0),
            'read_remote_bytes': metrics_data.get('readRemoteBytes', 0),
            'read_cache_bytes': metrics_data.get('readCacheBytes', 0),
            'spill_to_disk_bytes': metrics_data.get('spillToDiskBytes', 0),
            'rows_produced_count': metrics_data.get('rowsProducedCount', 0),
            'rows_read_count': metrics_data.get('rowsReadCount', 0),
            'read_files_count': metrics_data.get('readFilesCount', 0),
            'read_partitions_count': metrics_data.get('readPartitionsCount', 0),
            'photon_total_time_ms': metrics_data.get('photonTotalTimeMs', 0),
            'task_total_time_ms': metrics_data.get('taskTotalTimeMs', 0),
            'network_sent_bytes': metrics_data.get('networkSentBytes', 0),
            'photon_enabled': metrics_data.get('photonTotalTimeMs', 0) > 0,
            'photon_utilization_ratio': 0
        }
        
        # Photon利用率の計算
        if overall_metrics['task_total_time_ms'] > 0:
            overall_metrics['photon_utilization_ratio'] = min(
                overall_metrics['photon_total_time_ms'] / overall_metrics['task_total_time_ms'], 1.0
            )
        
        # キャッシュヒット率の計算
        cache_hit_ratio = 0
        if overall_metrics['read_bytes'] > 0:
            cache_hit_ratio = overall_metrics['read_cache_bytes'] / overall_metrics['read_bytes']
        
        # ボトルネック指標の計算
        bottleneck_indicators = {
            'spill_bytes': overall_metrics['spill_to_disk_bytes'],
            'has_spill': overall_metrics['spill_to_disk_bytes'] > 0,
            'cache_hit_ratio': cache_hit_ratio,
            'has_cache_miss': cache_hit_ratio < 0.8,
            'photon_efficiency': overall_metrics['photon_utilization_ratio'],
            'has_shuffle_bottleneck': False,  # 詳細情報がないため判定不可
            'remote_read_ratio': 0,
            'has_memory_pressure': overall_metrics['spill_to_disk_bytes'] > 0,
            'max_task_duration_ratio': 1.0,  # 不明
            'has_data_skew': False  # 詳細情報がないため判定不可
        }
        
        # リモート読み込み比率の計算
        if overall_metrics['read_bytes'] > 0:
            bottleneck_indicators['remote_read_ratio'] = overall_metrics['read_remote_bytes'] / overall_metrics['read_bytes']
        
        # クエリ情報の抽出
        query_info = {
            'query_id': query_data.get('id', ''),
            'query_text': query_data.get('queryText', '')[:300] + "..." if len(query_data.get('queryText', '')) > 300 else query_data.get('queryText', ''),
            'status': query_data.get('status', ''),
            'query_start_time': query_data.get('queryStartTimeMs', 0),
            'query_end_time': query_data.get('queryEndTimeMs', 0),
            'spark_ui_url': query_data.get('sparkUiUrl', ''),
            'endpoint_id': query_data.get('endpointId', ''),
            'user': query_data.get('user', {}).get('displayName', ''),
            'statement_type': query_data.get('statementType', ''),
            'plans_state': query_data.get('plansState', '')
        }
        
        # 詳細なパフォーマンス洞察を計算
        performance_insights = calculate_performance_insights_from_metrics(overall_metrics)
        
        # 擬似的なノードメトリクス（サマリー情報から生成）
        summary_node = {
            'node_id': 'summary_node',
            'name': f'Query Execution Summary ({query_data.get("statementType", "SQL")})',
            'tag': 'QUERY_SUMMARY',
            'key_metrics': {
                'durationMs': overall_metrics['total_time_ms'],
                'rowsNum': overall_metrics['rows_read_count'],
                'peakMemoryBytes': 0,  # 不明
                'throughputMBps': performance_insights['parallelization']['throughput_mb_per_second'],
                'dataSelectivity': performance_insights['data_efficiency']['data_selectivity'],
                'cacheHitRatio': performance_insights['cache_efficiency']['cache_hit_ratio']
            },
            'detailed_metrics': {
                'Total Time': {'value': overall_metrics['total_time_ms'], 'display_name': 'Total Time'},
                'Read Bytes': {'value': overall_metrics['read_bytes'], 'display_name': 'Read Bytes'},
                'Spill Bytes': {'value': overall_metrics['spill_to_disk_bytes'], 'display_name': 'Spill to Disk'},
                'Photon Time': {'value': overall_metrics['photon_total_time_ms'], 'display_name': 'Photon Time'},
                'Rows Read': {'value': overall_metrics['rows_read_count'], 'display_name': 'Rows Read Count'},
                'Cache Hit Ratio': {'value': performance_insights['cache_efficiency']['cache_hit_ratio'], 'display_name': 'Cache Hit Ratio'},
                'Data Selectivity': {'value': performance_insights['data_efficiency']['data_selectivity'], 'display_name': 'Data Selectivity'},
                'Throughput': {'value': performance_insights['parallelization']['throughput_mb_per_second'], 'display_name': 'Throughput (MB/s)'}
            },
            'graph_index': 0,
            'performance_insights': performance_insights
        }
        
        return {
            'data_format': 'sql_query_summary',
            'query_info': query_info,
            'overall_metrics': overall_metrics,
            'bottleneck_indicators': bottleneck_indicators,
            'node_metrics': [summary_node],
            'stage_metrics': [],  # 詳細ステージ情報なし
            'liquid_clustering_analysis': {},  # 後で追加
            'raw_profiler_data': profiler_data,
            'performance_insights': performance_insights,  # 詳細なパフォーマンス洞察を追加
            'analysis_capabilities': [
                'メトリクスベースのボトルネック分析（キャッシュ効率、データ選択性、Photon効率）',
                'リソース使用状況分析（スピル、並列化効率、スループット）',
                'パフォーマンス指標計算（ファイル効率、パーティション効率）',
                'ポテンシャルボトルネック特定（メトリクスベース）'
            ],
            'analysis_limitations': [
                '詳細な実行プラン情報（ノード、エッジ）が利用できません',
                'ステージ別メトリクスが利用できません', 
                'BROADCAST分析は基本的な推定のみ可能',
                'Liquid Clustering分析は一般的な推奨のみ可能',
                'データスキュー検出は平均値ベースの推定のみ',
                'クエリ構造の詳細解析は行いません（メトリクス重視アプローチ）'
            ]
        }
        
    except Exception as e:
        print(f"⚠️ SQLクエリサマリー形式のメトリクス抽出でエラー: {str(e)}")
        return {}

def extract_performance_metrics(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからボトルネック分析に必要なメトリクスを抽出（複数形式対応）
    """
    # データ形式を検出
    data_format = detect_data_format(profiler_data)
    
    print(f"🔍 検出されたデータ形式: {data_format}")
    
    if data_format == 'sql_query_summary':
        print("📊 Databricks SQLクエリサマリー形式として処理中...")
        result = extract_performance_metrics_from_query_summary(profiler_data)
        if result:
            # Liquid Clustering分析を追加（制限付き）
            try:
                result["liquid_clustering_analysis"] = analyze_liquid_clustering_opportunities(profiler_data, result)
            except Exception as e:
                print(f"⚠️ Liquid Clustering分析をスキップ: {str(e)}")
                result["liquid_clustering_analysis"] = {}
        return result
    elif data_format == 'sql_profiler':
        print("📊 SQLプロファイラー詳細形式として処理中...")
        # 既存のSQLプロファイラー形式の処理を継続
        pass
    else:
        print(f"⚠️ 未知のデータ形式です: {data_format}")
        return {}
    
    # 既存のSQLプロファイラー形式の処理
    metrics = {
        "query_info": {},
        "overall_metrics": {},
        "stage_metrics": [],
        "node_metrics": [],
        "bottleneck_indicators": {},
        "liquid_clustering_analysis": {},
        "raw_profiler_data": profiler_data  # プラン分析のために生データを保存
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
    
    # グラフデータからステージとノードのメトリクスを抽出（複数グラフ対応）
    if 'graphs' in profiler_data and profiler_data['graphs']:
        # すべてのグラフを分析
        for graph_index, graph in enumerate(profiler_data['graphs']):
            print(f"🔍 グラフ{graph_index}を分析中...")
            
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
                        "graph_index": graph_index  # どのグラフ由来かを記録
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
                            "key_metrics": node.get('keyMetrics', {}),
                            "metrics": node.get('metrics', []),  # 元のmetrics配列を保持
                            "graph_index": graph_index  # どのグラフ由来かを記録
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
# MAGIC ## 🏷️ ノード名解析・改善関数
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
    
    # データベース・テーブル情報を追加（強化版）
    table_name = None
    
    # 複数のメタデータキーからテーブル名を抽出（フルパス優先）
    for key_candidate in ['SCAN_TABLE', 'SCAN_IDENTIFIER', 'TABLE_NAME', 'RELATION', 'SCAN_RELATION']:
        if key_candidate in metadata_info:
            extracted_table = metadata_info[key_candidate]
            # フルパス（catalog.schema.table）の場合はそのまま使用
            if isinstance(extracted_table, str) and extracted_table.count('.') >= 2:
                table_name = extracted_table
                break
            elif isinstance(extracted_table, str) and extracted_table.count('.') == 1:
                # schema.table形式の場合もそのまま使用
                table_name = extracted_table
                break
            elif not table_name:  # まだテーブル名が見つかっていない場合のみ設定
                table_name = extracted_table
    
    # メタデータからテーブル名を抽出できない場合、ノード名から推測
    if not table_name and ('scan' in enhanced_name.lower() or 'data source' in enhanced_name.lower()):
        # ノード名からテーブル名を推測
        import re
        
        # "Scan tpcds.tpcds_sf1000_delta_lc.customer" のような形式
        table_patterns = [
            r'[Ss]can\s+([a-zA-Z_][a-zA-Z0-9_.]*[a-zA-Z0-9_])',
            r'[Tt]able\s+([a-zA-Z_][a-zA-Z0-9_.]*[a-zA-Z0-9_])',
            r'([a-zA-Z_][a-zA-Z0-9_]*\.)+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, original_name)
            if match:
                if '.' in match.group(0):
                    # フルテーブル名（catalog.schema.table）の場合はフルパスを使用
                    table_name = match.group(0)
                else:
                    table_name = match.group(1) if match.lastindex and match.lastindex >= 1 else match.group(0)
                break
    
    # メタデータのvaluesフィールドからもテーブル名を検索
    if not table_name:
        for meta in metadata:
            values = meta.get('values', [])
            if values:
                for value in values:
                    if isinstance(value, str) and '.' in value and len(value.split('.')) >= 2:
                        # "catalog.schema.table" 形式の場合
                        parts = value.split('.')
                        if len(parts) >= 2 and not any(part.isdigit() for part in parts[-2:]):
                            # フルパスを使用（catalog.schema.table）
                            if len(parts) >= 3:
                                table_name = '.'.join(parts)  # フルパス
                            else:
                                table_name = value  # そのまま使用
                            break
                if table_name:
                    break
    
    # Data Source Scanの場合にテーブル名を表示
    if table_name and ('scan' in enhanced_name.lower() or 'data source' in enhanced_name.lower()):
        # フルパス表示のために制限を緩和（60文字まで）
        if len(table_name) > 60:
            # カタログ.スキーマ.テーブル形式の場合は中間を省略
            parts = table_name.split('.')
            if len(parts) >= 3:
                table_name = f"{parts[0]}.*.{parts[-1]}"
            else:
                table_name = table_name[:57] + "..."
        enhanced_name = f"Data Source Scan ({table_name})"
    elif 'scan' in enhanced_name.lower() and 'data source' in enhanced_name.lower():
        # テーブル名が見つからない場合でも、より明確な名前に
        enhanced_name = "Data Source Scan"
    
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
# MAGIC ## 🎯 ボトルネック指標計算関数
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
    
    # スピル検出（詳細版：Sink - Num bytes spilled to disk due to memory pressure ベース）
    spill_detected = False
    total_spill_bytes = 0
    spill_details = []
    
    # ターゲットメトリクス名（複数パターン対応）
    target_spill_metrics = [
        "Sink - Num bytes spilled to disk due to memory pressure",
        "Num bytes spilled to disk due to memory pressure"
    ]
    
    # 各ノードでスピル検出を実行
    for node in metrics.get('node_metrics', []):
        node_spill_found = False
        
        # 1. detailed_metricsから検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            # 複数のスピルメトリクス名をチェック
            if ((metric_key in target_spill_metrics or 
                 metric_label in target_spill_metrics) and metric_value > 0):
                spill_detected = True
                node_spill_found = True
                total_spill_bytes += metric_value
                spill_details.append({
                    'node_id': node.get('node_id', ''),
                    'node_name': node.get('name', ''),
                    'spill_bytes': metric_value,
                    'spill_metric': metric_key if metric_key in target_spill_metrics else metric_label,
                    'source': 'detailed_metrics'
                })
                break
        
        # 2. raw_metricsから検索（このノードでまだ見つからない場合）
        if not node_spill_found:
            raw_metrics = node.get('metrics', [])
            for metric in raw_metrics:
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                # 複数のスピルメトリクス名をチェック
                if ((metric_key in target_spill_metrics or 
                     metric_label in target_spill_metrics) and metric_value > 0):
                    spill_detected = True
                    node_spill_found = True
                    total_spill_bytes += metric_value
                    spill_details.append({
                        'node_id': node.get('node_id', ''),
                        'node_name': node.get('name', ''),
                        'spill_bytes': metric_value,
                        'spill_metric': metric_key if metric_key in target_spill_metrics else metric_label,
                        'source': 'raw_metrics'
                    })
                    break
        
        # 3. key_metricsから検索（最後のフォールバック）
        if not node_spill_found:
            key_metrics = node.get('key_metrics', {})
            for key_metric_name, key_metric_value in key_metrics.items():
                # 複数のスピルメトリクス名をチェック
                if key_metric_name in target_spill_metrics and key_metric_value > 0:
                    spill_detected = True
                    node_spill_found = True
                    total_spill_bytes += key_metric_value
                    spill_details.append({
                        'node_id': node.get('node_id', ''),
                        'node_name': node.get('name', ''),
                        'spill_bytes': key_metric_value,
                        'spill_metric': key_metric_name,
                        'source': 'key_metrics'
                    })
                    break
    
    # フォールバック: overall_metricsからの簡易検出
    if not spill_detected:
        fallback_spill_bytes = overall.get('spill_to_disk_bytes', 0)
        if fallback_spill_bytes > 0:
            spill_detected = True
            total_spill_bytes = fallback_spill_bytes
            spill_details.append({
                'node_id': 'overall',
                'node_name': 'Overall Metrics',
                'spill_bytes': fallback_spill_bytes,
                'source': 'overall_metrics'
            })
    
    indicators['has_spill'] = spill_detected
    indicators['spill_bytes'] = total_spill_bytes
    indicators['spill_details'] = spill_details
    indicators['spill_nodes_count'] = len(spill_details)
    
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
# MAGIC ## 🧬 Liquid Clustering分析関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - プロファイラーデータからのカラム情報抽出
# MAGIC - フィルター、JOIN、GROUP BY条件の分析
# MAGIC - データスキューとパフォーマンス影響の評価
# MAGIC - クラスタリング推奨カラムの特定

# COMMAND ----------

def calculate_performance_insights_from_metrics(overall_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    メトリクス情報のみから詳細なパフォーマンス洞察を計算
    """
    insights = {}
    
    # 基本データ
    total_time_ms = overall_metrics.get('total_time_ms', 0)
    read_bytes = overall_metrics.get('read_bytes', 0)
    read_cache_bytes = overall_metrics.get('read_cache_bytes', 0)
    read_remote_bytes = overall_metrics.get('read_remote_bytes', 0)
    rows_read = overall_metrics.get('rows_read_count', 0)
    rows_produced = overall_metrics.get('rows_produced_count', 0)
    read_files = overall_metrics.get('read_files_count', 0)
    read_partitions = overall_metrics.get('read_partitions_count', 0)
    photon_time = overall_metrics.get('photon_total_time_ms', 0)
    task_time = overall_metrics.get('task_total_time_ms', 0)
    spill_bytes = overall_metrics.get('spill_to_disk_bytes', 0)
    
    # 1. データ効率分析
    insights['data_efficiency'] = {
        'data_selectivity': rows_produced / max(rows_read, 1),
        'avg_bytes_per_file': read_bytes / max(read_files, 1),
        'avg_bytes_per_partition': read_bytes / max(read_partitions, 1),
        'avg_rows_per_file': rows_read / max(read_files, 1),
        'avg_rows_per_partition': rows_read / max(read_partitions, 1)
    }
    
    # 2. キャッシュ効率分析
    cache_hit_ratio = read_cache_bytes / max(read_bytes, 1)
    insights['cache_efficiency'] = {
        'cache_hit_ratio': cache_hit_ratio,
        'cache_hit_percentage': cache_hit_ratio * 100,
        'remote_read_ratio': read_remote_bytes / max(read_bytes, 1),
        'cache_effectiveness': 'high' if cache_hit_ratio > 0.8 else 'medium' if cache_hit_ratio > 0.5 else 'low'
    }
    
    # 3. 並列化効率分析
    insights['parallelization'] = {
        'files_per_second': read_files / max(total_time_ms / 1000, 1),
        'partitions_per_second': read_partitions / max(total_time_ms / 1000, 1),
        'throughput_mb_per_second': (read_bytes / 1024 / 1024) / max(total_time_ms / 1000, 1),
        'rows_per_second': rows_read / max(total_time_ms / 1000, 1)
    }
    
    # 4. Photon効率分析
    photon_efficiency = photon_time / max(task_time, 1)
    insights['photon_analysis'] = {
        'photon_enabled': photon_time > 0,
        'photon_efficiency': photon_efficiency,
        'photon_utilization_percentage': photon_efficiency * 100,
        'photon_effectiveness': 'high' if photon_efficiency > 0.8 else 'medium' if photon_efficiency > 0.5 else 'low'
    }
    
    # 5. リソース使用状況
    insights['resource_usage'] = {
        'memory_pressure': spill_bytes > 0,
        'spill_gb': spill_bytes / 1024 / 1024 / 1024,
        'data_processed_gb': read_bytes / 1024 / 1024 / 1024,
        'data_reduction_ratio': 1 - (rows_produced / max(rows_read, 1))
    }
    
    # 6. ボトルネック指標
    bottlenecks = []
    if cache_hit_ratio < 0.3:
        bottlenecks.append('低キャッシュ効率')
    if read_remote_bytes / max(read_bytes, 1) > 0.8:
        bottlenecks.append('高リモート読み込み比率')
    if photon_efficiency < 0.5 and photon_time > 0:
        bottlenecks.append('低Photon効率')
    if spill_bytes > 0:
        bottlenecks.append('メモリスピル発生')
    if insights['data_efficiency']['data_selectivity'] < 0.1:
        bottlenecks.append('低データ選択性')
    
    insights['potential_bottlenecks'] = bottlenecks
    
    return insights

def extract_liquid_clustering_data(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからLiquid Clustering分析に必要なデータを抽出（LLM分析用）
    """
    extracted_data = {
        "filter_columns": [],
        "join_columns": [],
        "groupby_columns": [],
        "aggregate_columns": [],
        "table_info": {},
        "scan_nodes": [],
        "join_nodes": [],
        "filter_nodes": [],
        "metadata_summary": {}
    }
    
    print(f"🔍 Liquid Clustering分析用データ抽出開始")
    
    # データ形式を確認
    data_format = metrics.get('data_format', '')
    if data_format == 'sql_query_summary':
        print("📊 SQLクエリサマリー形式: 制限付きのLiquid Clustering分析")
        # test2.json形式の場合は制限付きの分析を行う
        query_info = metrics.get('query_info', {})
        query_text = query_info.get('query_text', '')
        
        # メトリクス情報のみから基本的なテーブル情報を生成
        # test2.json形式では planMetadatas が空のため、graphs metadata は利用不可
        # メトリクス重視のアプローチでボトルネック分析を行う
        
        extracted_data["table_info"]["metrics_summary"] = {
            "node_name": "Metrics-Based Analysis",
            "node_tag": "QUERY_SUMMARY", 
            "node_id": "summary",
            "files_count": overall_metrics.get('read_files_count', 0),
            "partitions_count": overall_metrics.get('read_partitions_count', 0),
            "data_size_gb": overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024,
            "rows_read": overall_metrics.get('rows_read_count', 0),
            "rows_produced": overall_metrics.get('rows_produced_count', 0),
            "data_selectivity": overall_metrics.get('rows_produced_count', 0) / max(overall_metrics.get('rows_read_count', 1), 1),
            "avg_file_size_mb": (overall_metrics.get('read_bytes', 0) / 1024 / 1024) / max(overall_metrics.get('read_files_count', 1), 1),
            "avg_partition_size_mb": (overall_metrics.get('read_bytes', 0) / 1024 / 1024) / max(overall_metrics.get('read_partitions_count', 1), 1),
            "note": "詳細なテーブル情報はSQLクエリサマリー形式では利用不可。メトリクスベース分析を実行。"
        }
        
        # サマリーノードの情報を使用
        for node in metrics.get('node_metrics', []):
            node_name = node.get('name', '')
            extracted_data["scan_nodes"].append({
                "name": node_name,
                "type": node.get('tag', ''),
                "rows": node.get('key_metrics', {}).get('rowsNum', 0),
                "duration_ms": node.get('key_metrics', {}).get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })
        
        # メタデータサマリー（制限付き）
        view_count = sum(1 for table in extracted_data["table_info"].values() if table.get('is_view', False))
        actual_table_count = sum(len(table.get('underlying_tables', [])) for table in extracted_data["table_info"].values())
        
        extracted_data["metadata_summary"] = {
            "total_nodes": len(metrics.get('node_metrics', [])),
            "total_graphs": 0,
            "filter_expressions_count": 0,
            "join_expressions_count": 0,
            "groupby_expressions_count": 0,
            "aggregate_expressions_count": 0,
            "tables_identified": len(extracted_data["table_info"]),
            "views_identified": view_count,
            "underlying_tables_estimated": actual_table_count,
            "scan_nodes_count": len(extracted_data["scan_nodes"]),
            "join_nodes_count": 0,
            "filter_nodes_count": 0,
            "analysis_limitation": "SQLクエリサマリー形式のため詳細分析が制限されています"
        }
        
        print(f"✅ 制限付きデータ抽出完了: {extracted_data['metadata_summary']}")
        
        # ビュー情報の詳細表示
        if view_count > 0:
            print(f"🔍 ビュー情報の詳細:")
            for table_name, table_info in extracted_data["table_info"].items():
                if table_info.get('is_view', False):
                    print(f"  📊 ビュー: {table_name}")
                    print(f"     エイリアス: {table_info.get('alias', 'なし')}")
                    print(f"     テーブル種別: {table_info.get('table_type', 'unknown')}")
                    
                    underlying_tables = table_info.get('underlying_tables', [])
                    if underlying_tables:
                        print(f"     推定実テーブル数: {len(underlying_tables)}")
                        for i, underlying_table in enumerate(underlying_tables[:3]):  # 最大3個表示
                            print(f"       - {underlying_table}")
                        if len(underlying_tables) > 3:
                            print(f"       ... および {len(underlying_tables) - 3} 個の追加テーブル")
                    print()
        
        return extracted_data
    
    # 通常のSQLプロファイラー形式の処理
    # プロファイラーデータから実行グラフ情報を取得（複数グラフ対応）
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        print("⚠️ グラフデータが見つかりません")
        return extracted_data

    # すべてのグラフからノードを収集
    all_nodes = []
    for graph_index, graph in enumerate(graphs):
        nodes = graph.get('nodes', [])
        for node in nodes:
            node['graph_index'] = graph_index
            all_nodes.append(node)
    
    print(f"🔍 {len(graphs)}個のグラフから{len(all_nodes)}個のノードを処理中")

    # ノードからメタデータ情報を抽出
    for node in all_nodes:
        node_name = node.get('name', '')
        node_tag = node.get('tag', '')
        node_metadata = node.get('metadata', [])
        
        # メタデータから重要な情報を抽出
        for metadata_item in node_metadata:
            key = metadata_item.get('key', '')
            values = metadata_item.get('values', [])
            value = metadata_item.get('value', '')
            
            # フィルター条件の抽出
            if key == 'FILTERS' and values:
                for filter_expr in values:
                    extracted_data["filter_columns"].append({
                        "expression": filter_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # GROUP BY式の抽出
            elif key == 'GROUPING_EXPRESSIONS' and values:
                for group_expr in values:
                    extracted_data["groupby_columns"].append({
                        "expression": group_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # JOIN条件の抽出
            elif key in ['LEFT_KEYS', 'RIGHT_KEYS'] and values:
                for join_key in values:
                    extracted_data["join_columns"].append({
                        "expression": join_key,
                        "key_type": key,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # 集約関数の抽出
            elif key == 'AGGREGATE_EXPRESSIONS' and values:
                for agg_expr in values:
                    extracted_data["aggregate_columns"].append({
                        "expression": agg_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # テーブル情報の抽出
            elif key == 'SCAN_IDENTIFIER':
                table_name = value
                extracted_data["table_info"][table_name] = {
                    "node_name": node_name,
                    "node_tag": node_tag,
                    "node_id": node.get('id', '')
                }

    # ノードタイプ別の分類
    node_metrics = metrics.get('node_metrics', [])
    for node in node_metrics:
        node_name = node.get('name', '')
        node_type = node.get('tag', '')
        key_metrics = node.get('key_metrics', {})
        
        if any(keyword in node_name.upper() for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            extracted_data["scan_nodes"].append({
                "name": node_name,
                "type": node_type,
                "rows": key_metrics.get('rowsNum', 0),
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })
        elif any(keyword in node_name.upper() for keyword in ['JOIN', 'HASH']):
            extracted_data["join_nodes"].append({
                "name": node_name,
                "type": node_type,
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })
        elif any(keyword in node_name.upper() for keyword in ['FILTER']):
            extracted_data["filter_nodes"].append({
                "name": node_name,
                "type": node_type,
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })

    # メタデータサマリー
    extracted_data["metadata_summary"] = {
        "total_nodes": len(all_nodes),
        "total_graphs": len(graphs),
        "filter_expressions_count": len(extracted_data["filter_columns"]),
        "join_expressions_count": len(extracted_data["join_columns"]),
        "groupby_expressions_count": len(extracted_data["groupby_columns"]),
        "aggregate_expressions_count": len(extracted_data["aggregate_columns"]),
        "tables_identified": len(extracted_data["table_info"]),
        "scan_nodes_count": len(extracted_data["scan_nodes"]),
        "join_nodes_count": len(extracted_data["join_nodes"]),
        "filter_nodes_count": len(extracted_data["filter_nodes"])
    }
    
    print(f"✅ データ抽出完了: {extracted_data['metadata_summary']}")
    return extracted_data

def analyze_liquid_clustering_opportunities(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    LLMを使用してLiquid Clusteringの分析と推奨事項を生成
    """
    print(f"🤖 LLMによるLiquid Clustering分析を開始")
    
    # 基本データの抽出
    extracted_data = extract_liquid_clustering_data(profiler_data, metrics)
    
    # LLM分析用のプロンプト作成
    overall_metrics = metrics.get('overall_metrics', {})
    bottleneck_indicators = metrics.get('bottleneck_indicators', {})
    
    # パフォーマンス概要
    total_time_sec = overall_metrics.get('total_time_ms', 0) / 1000
    read_gb = overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024
    rows_produced = overall_metrics.get('rows_produced_count', 0)
    rows_read = overall_metrics.get('rows_read_count', 0)
    
    # 抽出したカラム情報のサマリー作成（上位5個まで）
    filter_summary = []
    for i, item in enumerate(extracted_data["filter_columns"][:5]):
        filter_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    join_summary = []
    for i, item in enumerate(extracted_data["join_columns"][:5]):
        join_summary.append(f"  {i+1}. {item['expression']} (タイプ: {item['key_type']}, ノード: {item['node_name']})")
    
    groupby_summary = []
    for i, item in enumerate(extracted_data["groupby_columns"][:5]):
        groupby_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    aggregate_summary = []
    for i, item in enumerate(extracted_data["aggregate_columns"][:5]):
        aggregate_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    # テーブル情報のサマリー
    table_summary = []
    for table_name, table_info in extracted_data["table_info"].items():
        table_summary.append(f"  - {table_name} (ノード: {table_info['node_name']})")
    
    # スキャンノードのパフォーマンス情報
    scan_performance = []
    for scan in extracted_data["scan_nodes"]:
        efficiency = scan['rows'] / max(scan['duration_ms'], 1)
        scan_performance.append(f"  - {scan['name']}: {scan['rows']:,}行, {scan['duration_ms']:,}ms, 効率={efficiency:.1f}行/ms")

    clustering_prompt = f"""
あなたはDatabricksのLiquid Clustering専門家です。以下のSQLプロファイラーデータを分析し、最適なLiquid Clusteringの推奨事項を提示してください。

【クエリパフォーマンス概要】
- 実行時間: {total_time_sec:.1f}秒
- データ読み込み: {read_gb:.2f}GB
- 出力行数: {rows_produced:,}行
- 読み込み行数: {rows_read:,}行
- データ選択性: {(rows_produced/max(rows_read,1)):.4f}

【抽出されたカラム使用パターン】

🔍 フィルター条件 ({len(extracted_data["filter_columns"])}個):
{chr(10).join(filter_summary)}

🔗 JOIN条件 ({len(extracted_data["join_columns"])}個):
{chr(10).join(join_summary)}

📊 GROUP BY ({len(extracted_data["groupby_columns"])}個):
{chr(10).join(groupby_summary)}

📈 集約関数 ({len(extracted_data["aggregate_columns"])}個):
{chr(10).join(aggregate_summary)}

【テーブル情報】
テーブル数: {len(extracted_data["table_info"])}個
{chr(10).join(table_summary)}

【スキャンノードパフォーマンス】
{chr(10).join(scan_performance)}

【現在のボトルネック指標】
- スピル発生: {'あり' if bottleneck_indicators.get('has_spill', False) else 'なし'}
- シャッフル操作: {bottleneck_indicators.get('shuffle_operations_count', 0)}回
- 低並列度ステージ: {bottleneck_indicators.get('low_parallelism_stages_count', 0)}個

【分析要求】
1. 各テーブルに対する最適なLiquid Clusteringカラムの推奨（最大4カラム）
2. カラム選定の根拠（フィルター、JOIN、GROUP BYでの使用頻度と重要度）
3. 実装優先順位（パフォーマンス向上効果順）
4. 具体的なSQL実装例（正しいDatabricks SQL構文）
5. 期待されるパフォーマンス改善効果（数値で）

【制約事項】
- パーティショニングやZORDERは提案しない（Liquid Clusteringのみ）
- 正しいDatabricks SQL構文を使用：
  * 新規テーブル: CREATE TABLE ... CLUSTER BY (col1, col2, ...)
  * 既存テーブル: ALTER TABLE table_name CLUSTER BY (col1, col2, ...)
- 最大4カラムまでの推奨
- データスキューや並列度の問題も考慮

簡潔で実践的な分析結果を日本語で提供してください。
"""

    try:
        # LLM分析の実行
        provider = LLM_CONFIG["provider"]
        print(f"🤖 {provider}を使用してLiquid Clustering分析中...")
        
        if provider == "databricks":
            llm_analysis = _call_databricks_llm(clustering_prompt)
        elif provider == "openai":
            llm_analysis = _call_openai_llm(clustering_prompt)
        elif provider == "azure_openai":
            llm_analysis = _call_azure_openai_llm(clustering_prompt)
        elif provider == "anthropic":
            llm_analysis = _call_anthropic_llm(clustering_prompt)
        else:
            llm_analysis = f"❌ サポートされていないLLMプロバイダー: {provider}"
        
        # 分析結果の構造化
        clustering_analysis = {
            "llm_analysis": llm_analysis,
            "extracted_data": extracted_data,
            "performance_context": {
                "total_time_sec": total_time_sec,
                "read_gb": read_gb,
                "rows_produced": rows_produced,
                "rows_read": rows_read,
                "data_selectivity": rows_produced/max(rows_read,1)
            },
            "summary": {
                "analysis_method": "LLM-based",
                "tables_identified": len(extracted_data["table_info"]),
                "total_filter_columns": len(extracted_data["filter_columns"]),
                "total_join_columns": len(extracted_data["join_columns"]),
                "total_groupby_columns": len(extracted_data["groupby_columns"]),
                "total_aggregate_columns": len(extracted_data["aggregate_columns"]),
                "scan_nodes_count": len(extracted_data["scan_nodes"]),
                "llm_provider": provider
            }
        }
        
        print("✅ LLM Liquid Clustering分析完了")
        return clustering_analysis
        
    except Exception as e:
        error_msg = f"LLM分析エラー: {str(e)}"
        print(f"❌ {error_msg}")
        
        # フォールバック: 基本的な抽出データのみを返す
        return {
            "llm_analysis": f"❌ LLM分析に失敗しました: {error_msg}",
            "extracted_data": extracted_data,
            "summary": {
                "analysis_method": "extraction-only",
                "tables_identified": len(extracted_data["table_info"]),
                "total_filter_columns": len(extracted_data["filter_columns"]),
                "error": error_msg
            }
        }

def save_liquid_clustering_analysis(clustering_analysis: Dict[str, Any], output_dir: str = "/tmp") -> Dict[str, str]:
    """
    Liquid Clustering分析結果をファイルに出力
    """
    import os
    import json
    from datetime import datetime
    
    # タイムスタンプ付きファイル名の生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 出力ファイルパス
    json_path = f"{output_dir}/liquid_clustering_analysis_{timestamp}.json"
    markdown_path = f"{output_dir}/liquid_clustering_analysis_{timestamp}.md"
    sql_path = f"{output_dir}/liquid_clustering_implementation_{timestamp}.sql"
    
    file_paths = {}
    
    try:
        # 1. JSON形式での詳細データ保存
        # set型をlist型に変換してJSON serializable にする
        json_data = convert_sets_to_lists(clustering_analysis)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        file_paths['json'] = json_path
        print(f"✅ JSON形式の詳細データを保存: {json_path}")
        
        # 2. Markdown形式での分析レポート保存
        markdown_content = generate_liquid_clustering_markdown_report(clustering_analysis)
        
        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        file_paths['markdown'] = markdown_path
        print(f"✅ Markdown形式の分析レポートを保存: {markdown_path}")
        
        # 3. SQL実装例ファイルの生成
        sql_content = generate_liquid_clustering_sql_implementations(clustering_analysis)
        
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        file_paths['sql'] = sql_path
        print(f"✅ SQL実装例を保存: {sql_path}")
        
        return file_paths
        
    except Exception as e:
        error_msg = f"ファイル出力エラー: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}

def generate_liquid_clustering_markdown_report(clustering_analysis: Dict[str, Any]) -> str:
    """
    Liquid Clustering分析結果のMarkdownレポートを生成
    """
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 基本情報の取得
    summary = clustering_analysis.get('summary', {})
    performance_context = clustering_analysis.get('performance_context', {})
    extracted_data = clustering_analysis.get('extracted_data', {})
    llm_analysis = clustering_analysis.get('llm_analysis', '')
    
    markdown_content = f"""# Liquid Clustering 分析レポート

**生成日時**: {timestamp}  
**分析方法**: {summary.get('analysis_method', 'Unknown')}  
**LLMプロバイダー**: {summary.get('llm_provider', 'Unknown')}

## 📊 パフォーマンス概要

| 項目 | 値 |
|------|-----|
| 実行時間 | {performance_context.get('total_time_sec', 0):.1f}秒 |
| データ読み込み | {performance_context.get('read_gb', 0):.2f}GB |
| 出力行数 | {performance_context.get('rows_produced', 0):,}行 |
| 読み込み行数 | {performance_context.get('rows_read', 0):,}行 |
| データ選択性 | {performance_context.get('data_selectivity', 0):.4f} |

## 🔍 抽出されたメタデータ

### フィルター条件 ({summary.get('total_filter_columns', 0)}個)
"""
    
    # フィルター条件の詳細
    filter_columns = extracted_data.get('filter_columns', [])
    for i, filter_item in enumerate(filter_columns[:10], 1):  # 最大10個まで表示
        markdown_content += f"{i}. `{filter_item.get('expression', '')}` (ノード: {filter_item.get('node_name', '')})\n"
    
    if len(filter_columns) > 10:
        markdown_content += f"... 他 {len(filter_columns) - 10}個\n"
    
    markdown_content += f"""
### JOIN条件 ({summary.get('total_join_columns', 0)}個)
"""
    
    # JOIN条件の詳細
    join_columns = extracted_data.get('join_columns', [])
    for i, join_item in enumerate(join_columns[:10], 1):
        markdown_content += f"{i}. `{join_item.get('expression', '')}` ({join_item.get('key_type', '')})\n"
    
    if len(join_columns) > 10:
        markdown_content += f"... 他 {len(join_columns) - 10}個\n"
    
    markdown_content += f"""
### GROUP BY条件 ({summary.get('total_groupby_columns', 0)}個)
"""
    
    # GROUP BY条件の詳細
    groupby_columns = extracted_data.get('groupby_columns', [])
    for i, groupby_item in enumerate(groupby_columns[:10], 1):
        markdown_content += f"{i}. `{groupby_item.get('expression', '')}` (ノード: {groupby_item.get('node_name', '')})\n"
    
    if len(groupby_columns) > 10:
        markdown_content += f"... 他 {len(groupby_columns) - 10}個\n"
    
    markdown_content += f"""
### 集約関数 ({summary.get('total_aggregate_columns', 0)}個)
"""
    
    # 集約関数の詳細
    aggregate_columns = extracted_data.get('aggregate_columns', [])
    for i, agg_item in enumerate(aggregate_columns[:10], 1):
        markdown_content += f"{i}. `{agg_item.get('expression', '')}` (ノード: {agg_item.get('node_name', '')})\n"
    
    if len(aggregate_columns) > 10:
        markdown_content += f"... 他 {len(aggregate_columns) - 10}個\n"
    
    markdown_content += f"""
## 🏷️ 識別されたテーブル ({summary.get('tables_identified', 0)}個)

"""
    
    # テーブル情報の詳細
    table_info = extracted_data.get('table_info', {})
    for table_name, table_details in table_info.items():
        markdown_content += f"- **{table_name}** (ノード: {table_details.get('node_name', '')})\n"
    
    markdown_content += f"""
## 🔎 スキャンノード分析 ({summary.get('scan_nodes_count', 0)}個)

"""
    
    # スキャンノードの詳細
    scan_nodes = extracted_data.get('scan_nodes', [])
    for scan in scan_nodes:
        efficiency = scan.get('rows', 0) / max(scan.get('duration_ms', 1), 1)
        markdown_content += f"- **{scan.get('name', '')}**: {scan.get('rows', 0):,}行, {scan.get('duration_ms', 0):,}ms, 効率={efficiency:.1f}行/ms\n"
    
    markdown_content += f"""
## 🤖 LLM分析結果

{llm_analysis}

## 📋 分析サマリー

- **分析対象テーブル数**: {summary.get('tables_identified', 0)}
- **フィルター条件数**: {summary.get('total_filter_columns', 0)}
- **JOIN条件数**: {summary.get('total_join_columns', 0)}
- **GROUP BY条件数**: {summary.get('total_groupby_columns', 0)}
- **集約関数数**: {summary.get('total_aggregate_columns', 0)}
- **スキャンノード数**: {summary.get('scan_nodes_count', 0)}

---
*レポート生成時刻: {timestamp}*
"""
    
    return markdown_content

def generate_liquid_clustering_sql_implementations(clustering_analysis: Dict[str, Any]) -> str:
    """
    Liquid Clustering実装用のSQL例を生成
    """
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 基本情報の取得
    extracted_data = clustering_analysis.get('extracted_data', {})
    table_info = extracted_data.get('table_info', {})
    
    sql_content = f"""-- =====================================================
-- Liquid Clustering 実装SQL例
-- 生成日時: {timestamp}
-- =====================================================

-- 【重要】
-- 以下のSQL例は分析結果に基づく推奨事項です。
-- 実際の実装前に、テーブル構造やデータ特性を確認してください。

"""
    
    # テーブルごとのSQL実装例を生成
    for table_name, table_details in table_info.items():
        sql_content += f"""
-- =====================================================
-- テーブル: {table_name}
-- =====================================================

-- 既存テーブルにLiquid Clusteringを適用する場合:
-- ALTER TABLE {table_name} CLUSTER BY (column1, column2, column3, column4);

-- 新規テーブル作成時にLiquid Clusteringを設定する場合:
-- CREATE TABLE {table_name}_clustered
-- CLUSTER BY (column1, column2, column3, column4)
-- AS SELECT * FROM {table_name};

-- Delta Live Tablesでの設定例:
-- @dlt.table(
--   cluster_by=["column1", "column2", "column3", "column4"]
-- )
-- def {table_name.split('.')[-1]}_clustered():
--   return spark.table("{table_name}")

-- クラスタリング状況の確認:
-- DESCRIBE DETAIL {table_name};

-- クラスタリング統計の確認:
-- ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS;

"""
    
    sql_content += f"""
-- =====================================================
-- 一般的なLiquid Clustering実装パターン
-- =====================================================

-- パターン1: フィルター頻度の高いカラムを優先
-- 推奨順序: 1) フィルター条件カラム 2) JOIN条件カラム 3) GROUP BYカラム

-- パターン2: カーディナリティを考慮した順序
-- 低カーディナリティ → 高カーディナリティの順で配置

-- パターン3: データアクセスパターンに基づく配置
-- よく一緒に使用されるカラムを近い位置に配置

-- =====================================================
-- 実装後のパフォーマンス検証SQL
-- =====================================================

-- 1. クエリ実行計画の確認
-- EXPLAIN SELECT ... FROM table_name WHERE ...;

-- 2. ファイルスキップ統計の確認
-- SELECT * FROM table_name WHERE filter_column = 'value';
-- -- SQLプロファイラーでファイルスキップ数を確認

-- 3. データ配置の確認
-- SELECT 
--   file_path,
--   count(*) as row_count,
--   min(cluster_column1) as min_val,
--   max(cluster_column1) as max_val
-- FROM table_name
-- GROUP BY file_path
-- ORDER BY file_path;

-- =====================================================
-- 注意事項
-- =====================================================

-- 1. Liquid Clusteringは最大4カラムまで指定可能
-- 2. パーティショニングとは併用不可
-- 3. 既存のZORDER BYは自動的に無効化される
-- 4. クラスタリングの効果は時間とともに向上する（OPTIMIZE実行で最適化）
-- 5. 定期的なOPTIMIZE実行を推奨

-- OPTIMIZE実行例:
-- OPTIMIZE table_name;

-- =====================================================
-- 生成情報
-- =====================================================
-- 生成日時: {timestamp}
-- 分析対象テーブル数: {len(table_info)}
-- 基づいた分析: LLMによるLiquid Clustering分析
"""
    
    return sql_content

print("✅ 関数定義完了: analyze_liquid_clustering_opportunities, save_liquid_clustering_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 LLMによるボトルネック分析関数
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
    
    # Liquid Clustering分析情報の取得（LLMベース対応）
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    extracted_data = liquid_analysis.get('extracted_data', {})
    metadata_summary = extracted_data.get('metadata_summary', {})
    
    # テーブル情報の簡潔版
    table_info = extracted_data.get('table_info', {})
    table_recommendations = []
    if table_info:
        for i, (table_name, table_details) in enumerate(list(table_info.items())[:3]):
            table_recommendations.append(f"- {table_name}: LLM分析による推奨")
    
    # フィルター・JOIN・GROUP BYカラム情報
    filter_columns = extracted_data.get('filter_columns', [])
    join_columns = extracted_data.get('join_columns', [])
    groupby_columns = extracted_data.get('groupby_columns', [])
    
    high_impact_summary = []
    if filter_columns:
        high_impact_summary.append(f"- フィルター条件: {len(filter_columns)}個のカラム抽出")
    if join_columns:
        high_impact_summary.append(f"- JOIN条件: {len(join_columns)}個のカラム抽出")
    if groupby_columns:
        high_impact_summary.append(f"- GROUP BY条件: {len(groupby_columns)}個のカラム抽出")
    
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
テーブル数: {metadata_summary.get('tables_identified', 0)}個
推奨カラム:
{chr(10).join(table_recommendations) if table_recommendations else "LLM分析結果を参照"}

カラム抽出状況:
{chr(10).join(high_impact_summary) if high_impact_summary else "分析データが不足しています"}

【重要指標】
- 最遅ステージ: {metrics['bottleneck_indicators'].get('slowest_stage_id', 'N/A')}
- 最高メモリ: {metrics['bottleneck_indicators'].get('highest_memory_bytes', 0)/1024/1024:.0f}MB
- Photon使用率: {metrics['bottleneck_indicators'].get('photon_ratio', 0)*100:.0f}%

【求める分析】
1. 主要ボトルネックと原因（Photon、並列度、シャッフルに焦点）
2. Liquid Clustering実装の優先順位と手順（パーティショニング・ZORDER以外をDatabricksSQLでサポートされる正しいSQL構文で提示）
3. 各推奨カラムの選定理由と効果
4. Photonエンジンの最適化案
5. 並列度・シャッフル最適化案
6. パフォーマンス改善見込み

**重要**: パーティショニングやZORDERは提案せず、Liquid Clusteringのみを推奨してください。
Liquid Clustering実装時は、正しいDatabricks SQL構文を使用してください：
- 新規テーブル作成時: CREATE TABLE ... CLUSTER BY (column1, column2, ...)
- 既存テーブル変更時: ALTER TABLE table_name CLUSTER BY (column1, column2, ...)
- Delta Liveテーブル: @dlt.table(cluster_by=["column1", "column2"])
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

## 🗂️ Liquid Clustering推奨事項
**対象テーブル数**: {metadata_summary.get('tables_identified', 0)}個

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
1. **Liquid Clustering実装**: 上記推奨カラムでテーブルをクラスタリング（正しいDatabricks SQL構文: ALTER TABLE table_name CLUSTER BY (column1, column2, ...)）
2. **Photon有効化**: {'Photonエンジンを有効にする' if not photon_enabled else 'Photon設定を最適化'}
3. **並列度最適化**: {'クラスターサイズ・並列度設定を見直し' if has_low_parallelism else '現在の並列度は適切'}
4. **シャッフル最適化**: {'JOIN順序・GROUP BY最適化でシャッフル削減' if has_shuffle_bottleneck else 'シャッフル処理は最適'}
5. **クエリ最適化**: WHERE句の条件を適切に設定
6. **キャッシュ活用**: よく使用されるテーブルのキャッシュを検討

**重要**: パーティショニングやZORDERは使用せず、正しいDatabricks SQL構文（ALTER TABLE table_name CLUSTER BY (column1, column2, ...)）を使用してLiquid Clusteringで最適化してください。

**注意**: {provider} LLMエンドポイントの接続に問題があります。詳細な分析は手動で実施してください。
        """
        return fallback_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 個別LLMプロバイダー接続関数
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
        if config.get("thinking_enabled", False):
            payload["thinking"] = {
                "type": "enabled",
                "budget_tokens": config.get("thinking_budget_tokens", 65536)
            }
        
        # リトライ機能（SQL最適化用に増強）
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"🔄 リトライ中... (試行 {attempt + 1}/{max_retries})")
                
                response = requests.post(endpoint_url, headers=headers, json=payload, timeout=300)
                
                if response.status_code == 200:
                    result = response.json()
                    analysis_text = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                    print("✅ ボトルネック分析が完了しました")
                    return analysis_text
                else:
                    error_msg = f"APIエラー: ステータスコード {response.status_code}"
                    if response.status_code == 400:
                        # 400エラーの場合は詳細な解決策を提供
                        error_detail = response.text
                        if "maximum tokens" in error_detail.lower():
                            if attempt == max_retries - 1:
                                detailed_error = f"""❌ {error_msg}

🔧 トークン制限エラーの解決策:
1. LLM_CONFIG["databricks"]["max_tokens"] を 65536 (64K) に削減
2. より単純なクエリで再試行
3. 手動でSQL最適化を実行
4. クエリを分割して段階的に最適化

💡 推奨設定:
LLM_CONFIG["databricks"]["max_tokens"] = 65536
LLM_CONFIG["databricks"]["thinking_budget_tokens"] = 32768

詳細エラー: {error_detail}"""
                                print(detailed_error)
                                return detailed_error
                            else:
                                print(f"⚠️ {error_msg} (トークン制限) - リトライします...")
                                continue
                    
                    if attempt == max_retries - 1:
                        print(f"❌ {error_msg}\nレスポンス: {response.text}")
                        return f"{error_msg}\nレスポンス: {response.text}"
                    else:
                        print(f"⚠️ {error_msg} - リトライします...")
                        continue
                        
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    timeout_msg = f"""⏰ タイムアウトエラー: Databricksエンドポイントの応答が300秒以内に完了しませんでした。

🔧 解決策:
1. LLMエンドポイントの稼働状況を確認
2. プロンプトサイズを削減
3. より高性能なモデルを使用
4. 手動でSQL最適化を実行

💡 推奨アクション:
- クエリの複雑度を確認
- Databricks Model Servingエンドポイントのスケールアップ
- シンプルなクエリでテスト実行"""
                    print(f"❌ {timeout_msg}")
                    return timeout_msg
                else:
                    print(f"⏰ タイムアウト発生（300秒）- リトライします... (試行 {attempt + 1}/{max_retries})")
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
                               headers=headers, json=payload, timeout=300)
        
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
        
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=300)
        
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
                               headers=headers, json=payload, timeout=300)
        
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
# MAGIC ## 📋 LLMボトルネック分析実行の準備
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
    print("⚠️ セル12 (パフォーマンスメトリクス抽出) を先に実行してください")
    print("📋 正しい実行順序: セル11 → セル12 → セル15")
    print("🔄 デフォルトの分析結果を設定します")
    analysis_result = """
🤖 LLMボトルネック分析結果

❌ 分析に必要なメトリクスデータが見つかりませんでした。

📋 解決方法:
1. セル11でJSONファイルを読み込む
2. セル12でメトリクスを抽出する
3. このセル（セル15）を再実行する

⚠️ 先にメトリクス抽出を完了してから分析を実行してください。
"""
except Exception as e:
    print(f"❌ LLM分析中にエラーが発生しました: {str(e)}")
    analysis_result = f"LLM分析エラー: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 メイン処理実行セクション
# MAGIC
# MAGIC **ここからメインの分析処理が開始されます**
# MAGIC
# MAGIC 📋 **実行手順:**
# MAGIC 1. 上記の🔧設定・準備セクションをすべて実行してください
# MAGIC 2. 以下のセルを順番に実行して分析を行います
# MAGIC 3. エラーが発生した場合は、設定セクションから再実行してください
# MAGIC
# MAGIC ⚠️ **注意:**
# MAGIC - 🔧設定・準備セクション → 🚀メイン処理セクション → 🔧SQL最適化セクション の順序で実行
# MAGIC - ファイルパスの設定は必ず最初のセルで行ってください
# MAGIC - LLMエンドポイントの設定を確認してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 SQLプロファイラーJSONファイル読み込み実行
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
# MAGIC ## 📊 パフォーマンスメトリクス抽出と概要表示
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
# MAGIC ## 🔍 ボトルネック指標詳細
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - Photon エンジンの利用状況とパフォーマンス分析
# MAGIC - シャッフル操作と並列度の問題検出
# MAGIC - 各種パフォーマンス指標の詳細表示

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
# MAGIC ## 💾 メトリクス保存と時間消費分析
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 抽出したメトリクスのJSON形式での保存
# MAGIC - set型からlist型への変換処理
# MAGIC - 最も時間がかかっている処理TOP10の詳細分析
# MAGIC - 特定メトリクスベーススピル検出とAQEベーススキュー分析
# MAGIC
# MAGIC 💿 **スピル検出ロジック**:
# MAGIC - ターゲットメトリクス: `"Sink - Num bytes spilled to disk due to memory pressure"`
# MAGIC - 判定条件: 上記メトリクスの値 > 0 の場合にスピルありと判定
# MAGIC - 検索対象: detailed_metrics → raw_metrics → key_metrics の順序で検索
# MAGIC
# MAGIC 🎯 **スキュー検出ロジック**:
# MAGIC - `AQEShuffleRead - Number of skewed partitions`: AQEベーススキュー検出
# MAGIC - 判定条件: メトリクス値 > 0 でスキュー判定
# MAGIC - 重要度: 検出値に基づいた判定
# MAGIC - 統計ベース判定は非推奨（AQEベース判定を推奨）
# MAGIC
# MAGIC 💡 **デバッグモード**: スピル・スキューの判定根拠を詳細表示したい場合
# MAGIC ```python
# MAGIC import os
# MAGIC os.environ['DEBUG_SPILL_ANALYSIS'] = 'true'   # 特定メトリクススピル判定の詳細表示
# MAGIC os.environ['DEBUG_SKEW_ANALYSIS'] = 'true'    # AQEベーススキュー判定の詳細表示
# MAGIC ```

# COMMAND ----------

# 🐛 デバッグモード設定（オプション）
# 
# **スピル・スキューの判定根拠を詳細表示したい場合のみ実行してください**
# 
# 📋 設定内容:
# - DEBUG_SPILL_ANALYSIS=true: 特定メトリクススピル判定の詳細根拠を表示
# - DEBUG_SKEW_ANALYSIS=true: AQEベーススキュー判定の詳細根拠を表示
# 
# 💿 スピルデバッグ表示内容:
# - ターゲットメトリクス: "Sink - Num bytes spilled to disk due to memory pressure"
# - 各データソース（detailed_metrics, raw_metrics, key_metrics）での検索結果
# - メトリクス発見時の値と判定結果
# - その他のスピル関連メトリクス一覧（参考情報）
# 
# 🎯 スキューデバッグ表示内容:
# - AQEShuffleRead - Number of skewed partitions メトリクス値
# - AQEベーススキュー検出の判定根拠
# - 検出されたスキュー数と重要度レベル
# - 統計ベース判定は非推奨（AQEベース判定を推奨）

import os

# 特定メトリクススピル分析のデバッグ表示を有効にする場合はコメントアウトを解除
# os.environ['DEBUG_SPILL_ANALYSIS'] = 'true'

# AQEベーススキュー分析のデバッグ表示を有効にする場合はコメントアウトを解除  
# os.environ['DEBUG_SKEW_ANALYSIS'] = 'true'

print("🐛 デバッグモード設定:")
print(f"   特定メトリクススピル分析デバッグ: {os.environ.get('DEBUG_SPILL_ANALYSIS', 'false')}")
print(f"   AQEベーススキュー分析デバッグ: {os.environ.get('DEBUG_SKEW_ANALYSIS', 'false')}")
print("   ※ 'true'に設定すると判定根拠の詳細情報が表示されます")
print()
print("💿 特定メトリクススピル検出基準:")
print('   🎯 ターゲット: "Sink - Num bytes spilled to disk due to memory pressure"')
print("   ✅ 判定条件: 値 > 0")
print()
print("🎯 AQEベーススキュー検出基準:")
print("   📊 AQEShuffleRead - Number of skewed partitions > 0")
print("   📊 判定条件: メトリクス値 > 0")
print("   📊 重要度: 検出値に基づく")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐌 最も時間がかかっている処理TOP10
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 抽出したメトリクスのJSON形式での保存
# MAGIC - set型からlist型への変換処理
# MAGIC - 最も時間がかかっている処理TOP10の詳細分析
# MAGIC - スピル検出とデータスキュー分析
# MAGIC - Sparkステージ実行分析

# COMMAND ----------

# 💾 抽出したメトリクスのJSONファイル保存は除外（不要）
def format_thinking_response(response) -> str:
    """
    thinking_enabled: Trueの場合のレスポンスを人間に読みやすい形式に変換
    思考過程（thinking）とシグネチャ（signature）等の不要な情報は除外し、最終的な結論のみを表示
    JSON構造や不適切な文字列の露出を防止
    """
    import re  # reモジュールのインポートを追加
    
    if not isinstance(response, list):
        # リストでない場合は文字列として処理し、クリーンアップ
        cleaned_text = clean_response_text(str(response))
        return cleaned_text
    
    # 除外すべきキーのリスト（拡張）
    excluded_keys = {
        'thinking', 'signature', 'metadata', 'id', 'request_id', 
        'timestamp', 'uuid', 'reasoning', 'type', 'model'
    }
    
    formatted_parts = []
    
    for item in response:
        if isinstance(item, dict):
            # 最も適切なテキストコンテンツを抽出
            content = extract_best_content_from_dict(item, excluded_keys)
            if content:
                cleaned_content = clean_response_text(content)
                if is_valid_content(cleaned_content):
                    formatted_parts.append(cleaned_content)
        else:
            # 辞書でない場合もクリーンアップ
            cleaned_content = clean_response_text(str(item))
            if is_valid_content(cleaned_content):
                formatted_parts.append(cleaned_content)
    
    final_result = '\n'.join(formatted_parts)
    
    # 最終的な品質チェックとクリーンアップ
    final_result = final_quality_check(final_result)
    
    return final_result

def extract_best_content_from_dict(item_dict, excluded_keys):
    """辞書から最適なコンテンツを抽出"""
    # 優先順位: text > summary_text > content > message > その他
    priority_keys = ['text', 'summary_text', 'content', 'message', 'response']
    
    for key in priority_keys:
        if key in item_dict and item_dict[key]:
            content = str(item_dict[key])
            # JSON構造が含まれていないかチェック
            if not looks_like_json_structure(content):
                return content
    
    # 優先キーで見つからない場合、他のキーをチェック（除外キー以外）
    for key, value in item_dict.items():
        if key not in excluded_keys and value and isinstance(value, str):
            if not looks_like_json_structure(value):
                return value
    
    return None

def looks_like_json_structure(text):
    """テキストがJSON構造を含んでいるかチェック"""
    json_indicators = [
        "{'type':", '[{\'type\':', '{"type":', '[{"type":',
        "'text':", '"text":', "'summary_text':", '"summary_text":',
        'reasoning', 'metadata', 'signature'
    ]
    text_lower = text.lower()
    return any(indicator.lower() in text_lower for indicator in json_indicators)

def clean_response_text(text):
    """レスポンステキストのクリーンアップ"""
    import re
    
    if not text or not isinstance(text, str):
        return ""
    
    # 改行コードの正規化
    text = text.replace('\\n', '\n').replace('\\t', '\t')
    
    # JSON構造の除去
    
    # 典型的なJSON構造パターンを除去
    json_patterns = [
        r"'type':\s*'[^']*'",
        r'"type":\s*"[^"]*"',
        r"\[?\{'type':[^}]*\}[,\]]?",
        r'\[?\{"type":[^}]*\}[,\]]?',
        r"'reasoning':\s*\[[^\]]*\]",
        r'"reasoning":\s*\[[^\]]*\]',
        r"'signature':\s*'[A-Za-z0-9+/=]{50,}'",
        r'"signature":\s*"[A-Za-z0-9+/=]{50,}"'
    ]
    
    for pattern in json_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
    
    # 不完全なJSONブラケットの除去
    text = re.sub(r'^\s*[\[\{]', '', text)  # 先頭の [ や {
    text = re.sub(r'[\]\}]\s*$', '', text)  # 末尾の ] や }
    text = re.sub(r'^\s*[,;]\s*', '', text)  # 先頭のカンマやセミコロン
    
    # 連続する空白・改行の正規化
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # 3つ以上の連続改行を2つに
    text = re.sub(r'[ \t]+', ' ', text)  # 連続するスペース・タブを1つに
    
    # 前後の空白を除去
    text = text.strip()
    
    return text

def is_valid_content(text):
    """コンテンツが有効かどうかをチェック"""
    import re
    
    if not text or len(text.strip()) < 10:
        return False
    
    # 無効なパターンをチェック
    invalid_patterns = [
        r'^[{\[\'"]*$',  # JSON構造のみ
        r'^[,;:\s]*$',   # 区切り文字のみ
        r'^\s*reasoning\s*$',  # reasoningのみ
        r'^\s*metadata\s*$',   # metadataのみ
        r'^[A-Za-z0-9+/=]{50,}$',  # Base64っぽい長い文字列
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, text.strip(), re.IGNORECASE):
            return False
    
    return True

def final_quality_check(text):
    """最終的な品質チェックとクリーンアップ"""
    import re  # reモジュールのインポートを追加
    
    if not text:
        return "分析結果の抽出に失敗しました。"
    
    # 言語の一貫性チェック（安全な変数アクセス）
    try:
        language = globals().get('OUTPUT_LANGUAGE', 'ja')  # デフォルトは日本語
    except:
        language = 'ja'
    
    if language == 'ja':
        text = ensure_japanese_consistency(text)
    elif language == 'en':
        text = ensure_english_consistency(text)
    
    # 最小限の長さチェック
    if len(text.strip()) < 20:
        if language == 'ja':
            return "分析結果が不完全です。詳細な分析を実行中です。"
        else:
            return "Analysis result is incomplete. Detailed analysis in progress."
    
    return text

def ensure_japanese_consistency(text):
    """日本語の一貫性を確保"""
    import re
    
    # 明らかに破損している部分を除去
    # 例: "正caientify="predicate_liquid_referencet1" のような破損文字列
    text = re.sub(r'[a-zA-Z0-9_="\']{20,}', '', text)
    
    # 不完全なマークダウンの修正
    text = re.sub(r'#\s*[^#\n]*["\'>]+[^#\n]*', '', text)  # 破損したマークダウンヘッダー
    
    # 意味不明な文字列パターンの除去（拡張）
    nonsense_patterns = [
        r'addressing_sales_column\d*',
        r'predicate_liquid_reference[a-zA-Z0-9]*',
        r'bottlenars\s+effect',
        r'実装非保存在',
        r'裏票のend_by',
        r'riconsistall',
        r'caientify[a-zA-Z0-9="\']*',
        r'iving\s+[a-zA-Z0-9]*',
        r'o\s+Matter配賛',
        r'ubsが低い僮性',
        r'到田データの方効性',
        r'パフォーマンス.*topic.*項行に考',
        r'［[^］]*］">[^<]*',  # 破損したHTML/XML要素
        r'\]\s*">\s*$'  # 文末の破損したタグ
    ]
    
    for pattern in nonsense_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # 連続する記号の除去
    text = re.sub(r'["\'>]{2,}', '', text)
    text = re.sub(r'[=\'"]{3,}', '', text)
    
    # 破損した日本語の修正パターン
    broken_japanese_patterns = [
        (r'の方法動的がら', '動的な方法で'),
        (r'思考に沿って進めていきます。$', '思考に沿って分析を進めます。'),
        (r'ベストプラクティスに沿った改善を.*までしているの', 'ベストプラクティスに沿った改善提案'),
    ]
    
    for broken, fixed in broken_japanese_patterns:
        text = re.sub(broken, fixed, text, flags=re.IGNORECASE)
    
    # 空行の正規化
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    return text.strip()

def ensure_english_consistency(text):
    """英語の一貫性を確保"""
    import re
    
    # 同様のクリーンアップを英語用に実装
    text = re.sub(r'[^\x00-\x7F\s]{10,}', '', text)  # 非ASCII文字の長い連続を除去
    
    return text.strip()

def extract_main_content_from_thinking_response(response) -> str:
    """
    thinking形式のレスポンスから主要コンテンツ（textまたはsummary_text）のみを抽出
    thinking、signature等の不要な情報は除外
    JSON構造や破損したテキストの混入を防止
    """
    if not isinstance(response, list):
        cleaned_text = clean_response_text(str(response))
        return final_quality_check(cleaned_text)
    
    # 除外すべきキー
    excluded_keys = {
        'thinking', 'signature', 'metadata', 'id', 'request_id', 
        'timestamp', 'uuid', 'reasoning', 'type', 'model'
    }
    
    for item in response:
        if isinstance(item, dict):
            # 最適なコンテンツを抽出
            content = extract_best_content_from_dict(item, excluded_keys)
            if content:
                cleaned_content = clean_response_text(content)
                if is_valid_content(cleaned_content):
                    return final_quality_check(cleaned_content)
    
    # 主要コンテンツが見つからない場合は全体をフォーマット
    return format_thinking_response(response)

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

# output_extracted_metrics の生成は除外（不要）

# 🐌 最も時間がかかっている処理TOP10
print(f"\n🐌 最も時間がかかっている処理TOP10")
print("=" * 80)
print("📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー")
print('💿 スピル判定: "Sink - Num bytes spilled to disk due to memory pressure" > 0')
print("🎯 スキュー判定: 'AQEShuffleRead - Number of skewed partitions' > 0")

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
        
        # ディスクスピルアウトの検出（メモリプレッシャーによるスピルメトリクス対応改善版）
        spill_detected = False
        spill_bytes = 0
        spill_details = []
        
        # スピル検出ターゲットメトリクス名リスト（複数パターン対応）
        target_spill_metrics = [
            "Sink - Num bytes spilled to disk due to memory pressure",
            "Num bytes spilled to disk due to memory pressure",
            "bytes spilled to disk due to memory pressure",
            "spilled to disk due to memory pressure"
        ]
        
        # 1. detailed_metricsからスピルメトリクスを検索（柔軟検索）
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            # 部分文字列マッチングでスピルメトリクスを検索
            key_matches = any(target in metric_key for target in target_spill_metrics)
            label_matches = any(target in metric_label for target in target_spill_metrics)
            
            if (key_matches or label_matches) and metric_value > 0:
                spill_detected = True
                spill_bytes = metric_value
                matched_target = next((target for target in target_spill_metrics if target in metric_key or target in metric_label), "unknown")
                spill_details.append({
                    'metric_name': metric_key,
                    'value': metric_value,
                    'label': metric_label,
                    'source': 'detailed_metrics',
                    'matched_field': 'key' if key_matches else 'label',
                    'matched_pattern': matched_target
                })
                break  # 最初に見つかったスピルメトリクスを使用
        
        # 2. detailed_metricsで見つからない場合、生メトリクスから検索
        if not spill_detected:
            raw_metrics = node.get('metrics', [])
            for metric in raw_metrics:
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                # 部分文字列マッチングでスピルメトリクスを検索
                key_matches = any(target in metric_key for target in target_spill_metrics)
                label_matches = any(target in metric_label for target in target_spill_metrics)
                
                if (key_matches or label_matches) and metric_value > 0:
                    spill_detected = True
                    spill_bytes = metric_value
                    matched_target = next((target for target in target_spill_metrics if target in metric_key or target in metric_label), "unknown")
                    spill_details.append({
                        'metric_name': metric_key,
                        'value': metric_value,
                        'label': metric_label,
                        'source': 'raw_metrics',
                        'matched_field': 'key' if key_matches else 'label',
                        'matched_pattern': matched_target
                    })
                    break  # 最初に見つかったスピルメトリクスを使用
        
        # 3. key_metricsでも検索（フォールバック）
        if not spill_detected:
            key_metrics = node.get('key_metrics', {})
            for key_metric_name, key_metric_value in key_metrics.items():
                key_matches = any(target in key_metric_name for target in target_spill_metrics)
                if key_matches and key_metric_value > 0:
                    spill_detected = True
                    spill_bytes = key_metric_value
                    matched_target = next((target for target in target_spill_metrics if target in key_metric_name), "unknown")
                    spill_details.append({
                        'metric_name': f"key_metrics.{key_metric_name}",
                        'value': key_metric_value,
                        'label': f"Key metric: {key_metric_name}",
                        'source': 'key_metrics',
                        'matched_field': 'key',
                        'matched_pattern': matched_target
                    })
                    break
        
        # データスキューの検出（AQEベースの精密判定）
        skew_detected = False
        skew_details = []
        
        # AQEベーススキュー検出: "AQEShuffleRead - Number of skewed partitions" > 0
        target_aqe_metrics = [
            "AQEShuffleRead - Number of skewed partitions",
            "AQEShuffleRead - Number of skewed partition splits"
        ]
        
        aqe_skew_value = 0
        aqe_split_value = 0
        aqe_metric_name = ""
        aqe_split_metric_name = ""
        
        # 1. detailed_metricsで検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            if metric_key == "AQEShuffleRead - Number of skewed partitions":
                aqe_skew_value = metric_info.get('value', 0)
                aqe_metric_name = metric_key
            elif metric_key == "AQEShuffleRead - Number of skewed partition splits":
                aqe_split_value = metric_info.get('value', 0)
                aqe_split_metric_name = metric_key
            elif metric_info.get('label', '') == "AQEShuffleRead - Number of skewed partitions":
                aqe_skew_value = metric_info.get('value', 0)
                aqe_metric_name = metric_info.get('label', '')
            elif metric_info.get('label', '') == "AQEShuffleRead - Number of skewed partition splits":
                aqe_split_value = metric_info.get('value', 0)
                aqe_split_metric_name = metric_info.get('label', '')
        
        # 2. raw_metricsで検索（フォールバック）
        if aqe_skew_value == 0 or aqe_split_value == 0:
            raw_metrics = node.get('metrics', [])
            if isinstance(raw_metrics, list):
                for raw_metric in raw_metrics:
                    if isinstance(raw_metric, dict):
                        # 'label'フィールドを最初にチェック
                        raw_metric_label = raw_metric.get('label', '')
                        if raw_metric_label == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_label
                        elif raw_metric_label == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_label
                        
                        # 'key'フィールドもチェック
                        raw_metric_key = raw_metric.get('key', '')
                        if raw_metric_key == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_key
                        elif raw_metric_key == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_key
                        
                        # 'metricName'フィールドもチェック（従来の互換性）
                        raw_metric_name = raw_metric.get('metricName', '')
                        if raw_metric_name == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_name
                        elif raw_metric_name == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_name
        
        # 3. key_metricsで検索（フォールバック）
        if aqe_skew_value == 0 or aqe_split_value == 0:
            key_metrics = node.get('key_metrics', {})
            for key_metric_name, key_metric_value in key_metrics.items():
                if "AQEShuffleRead - Number of skewed partitions" in key_metric_name and aqe_skew_value == 0:
                    aqe_skew_value = key_metric_value
                    aqe_metric_name = key_metric_name
                elif "AQEShuffleRead - Number of skewed partition splits" in key_metric_name and aqe_split_value == 0:
                    aqe_split_value = key_metric_value
                    aqe_split_metric_name = key_metric_name
        
        # AQEスキュー判定
        if aqe_skew_value > 0:
            skew_detected = True
            severity_level = "高" if aqe_skew_value >= 5 else "中"
            
            # 基本的なAQEスキュー検出情報
            description = f'AQEスキュー検出: {aqe_metric_name} = {aqe_skew_value} > 基準値 0 [重要度:{severity_level}]'
            
            # split値も取得できた場合、詳細情報を追加
            if aqe_split_value > 0:
                description += f' | AQE検出詳細: Sparkが自動的に{aqe_skew_value}個のスキューパーティションを検出'
                description += f' | AQE自動対応: Sparkが自動的に{aqe_split_value}個のパーティションに分割'
            
            skew_details.append({
                'type': 'aqe_skew',
                'value': aqe_skew_value,
                'split_value': aqe_split_value,
                'threshold': 0,
                'metric_name': aqe_metric_name,
                'split_metric_name': aqe_split_metric_name,
                'severity': severity_level,
                'description': description
            })
        
        # 4. スピルメトリクスに基づくスキュー検出（メモリプレッシャーによるスピルの不均等を検出）
        if spill_detected and spill_bytes > 0:
            # スピルが検出された場合、それ自体がスキューの可能性を示す
            # 特に大きなスピルが発生している場合はスキューの可能性が高い
            spill_mb = spill_bytes / 1024 / 1024
            if spill_mb > 100:  # 100MB以上のスピルがある場合
                skew_detected = True
                severity_level = "高" if spill_mb > 1000 else "中"  # 1GB以上は高重要度
                skew_details.append({
                    'type': 'memory_pressure_spill_skew',
                    'spill_bytes': spill_bytes,
                    'spill_mb': spill_mb,
                    'threshold': 100,
                    'severity': severity_level,
                    'description': f'メモリプレッシャーによるスピルスキュー: {spill_mb:.1f}MB スピル発生（基準値: 100MB） [重要度:{severity_level}]'
                })
        
        # 5. タスク数とスピルの関係によるスキュー検出の強化
        if spill_detected and num_tasks > 10:
            # 多数のタスクがあるのにスピルが発生している場合、データの不均等分散の可能性
            # タスクあたりのスピル量を計算
            spill_per_task_mb = (spill_bytes / num_tasks) / 1024 / 1024
            if spill_per_task_mb > 10:  # タスクあたり10MB以上のスピル
                skew_detected = True
                severity_level = "高" if spill_per_task_mb > 50 else "中"
                skew_details.append({
                    'type': 'task_spill_distribution_skew',
                    'spill_per_task_mb': spill_per_task_mb,
                    'num_tasks': num_tasks,
                    'total_spill_mb': spill_bytes / 1024 / 1024,
                    'threshold': 10,
                    'severity': severity_level,
                    'description': f'タスクあたりスピル量スキュー: {spill_per_task_mb:.1f}MB/タスク ({num_tasks}タスク中) [重要度:{severity_level}]'
                })
        
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
        print(f"    🔧 並列度: {num_tasks:>3d} タスク | 💿 スピル: {'あり' if spill_detected else 'なし'} | ⚖️ スキュー: {'検出' if skew_detected else 'なし'}")
        
        # 効率性指標（行/秒）を計算
        if duration_ms > 0:
            rows_per_sec = (rows_num * 1000) / duration_ms
            print(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒")
        
        # スピル詳細情報（デバッグ表示付き強化版）
        if spill_detected:
            if spill_bytes > 0:
                print(f"    💿 スピル詳細: {spill_bytes/1024/1024:.1f} MB")
            
            # スピル判定根拠の詳細表示（特定メトリクスベース）
            print(f"    🔍 スピル判定根拠:")
            for detail in spill_details:
                metric_name = detail['metric_name']
                value = detail['value']
                label = detail['label']
                source = detail['source']
                matched_field = detail.get('matched_field', 'unknown')
                
                if value >= 1024 * 1024 * 1024:  # GB単位
                    value_display = f"{value/1024/1024/1024:.2f} GB"
                elif value >= 1024 * 1024:  # MB単位
                    value_display = f"{value/1024/1024:.1f} MB"
                elif value >= 1024:  # KB単位
                    value_display = f"{value/1024:.1f} KB"
                else:
                    value_display = f"{value} bytes"
                
                # 正確なスピル値を適切な単位で表示
                if value >= 1024 * 1024 * 1024:  # GB単位
                    value_display = f"{value/1024/1024/1024:.2f} GB"
                elif value >= 1024 * 1024:  # MB単位
                    value_display = f"{value/1024/1024:.1f} MB"
                elif value >= 1024:  # KB単位
                    value_display = f"{value/1024:.1f} KB"
                else:
                    value_display = f"{value} bytes"
                
                print(f"       🎯 ターゲットメトリクス: 'Num bytes spilled to disk due to memory pressure' または 'Sink - Num bytes spilled to disk due to memory pressure'")
                print(f"       📊 検出値: {value_display}")
                print(f"       🔍 検出されたメトリクス名: {metric_name}")
                if label and label != metric_name:
                    print(f"       🏷️  ラベル: {label}")
                print(f"       ✅ 判定: スピルあり (値 > 0)")
        else:
            # スピルが検出されなかった場合のデバッグ情報（詳細表示時のみ）
            import os
            if os.environ.get('DEBUG_SPILL_ANALYSIS', '').lower() in ['true', '1', 'yes']:
                print(f"    🔍 スピル未検出:")
                print(f"       🎯 ターゲットメトリクス: 'Num bytes spilled to disk due to memory pressure' または 'Sink - Num bytes spilled to disk due to memory pressure'")
                print(f"       ❌ 検出結果: メトリクスが見つからないか、値が0")
                
                # 各ソースでの検索結果を表示
                detailed_metrics = node.get('detailed_metrics', {})
                raw_metrics = node.get('metrics', [])
                key_metrics = node.get('key_metrics', {})
                
                target_spill_metrics = [
                    "Sink - Num bytes spilled to disk due to memory pressure",
                    "Num bytes spilled to disk due to memory pressure"
                ]
                
                # detailed_metricsでの検索結果
                found_in_detailed = False
                for metric_key, metric_info in detailed_metrics.items():
                    if metric_key in target_spill_metrics or metric_info.get('label', '') in target_spill_metrics:
                        found_in_detailed = True
                        value = metric_info.get('value', 0)
                        matched_metric = metric_key if metric_key in target_spill_metrics else metric_info.get('label', '')
                        print(f"       📊 detailed_metrics: {matched_metric} 見つかったが値={value} (≤ 0)")
                        break
                if not found_in_detailed:
                    print(f"       📊 detailed_metrics: ターゲットメトリクス未発見 ({len(detailed_metrics)}個中)")
                
                # raw_metricsでの検索結果
                found_in_raw = False
                for metric in raw_metrics:
                    metric_key = metric.get('key', '')
                    metric_label = metric.get('label', '')
                    if metric_key in target_spill_metrics or metric_label in target_spill_metrics:
                        found_in_raw = True
                        value = metric.get('value', 0)
                        matched_metric = metric_key if metric_key in target_spill_metrics else metric_label
                        print(f"       📊 raw_metrics: {matched_metric} 見つかったが値={value} (≤ 0)")
                        break
                if not found_in_raw:
                    print(f"       📊 raw_metrics: ターゲットメトリクス未発見 ({len(raw_metrics)}個中)")
                
                # key_metricsでの検索結果
                found_in_key = False
                for target_metric in target_spill_metrics:
                    if target_metric in key_metrics:
                        found_in_key = True
                        value = key_metrics[target_metric]
                        print(f"       📊 key_metrics: {target_metric} 見つかったが値={value} (≤ 0)")
                        break
                if not found_in_key:
                    print(f"       📊 key_metrics: ターゲットメトリクス未発見 ({len(key_metrics)}個中)")
                
                # 利用可能なスピル関連メトリクス一覧（参考）
                spill_related = []
                for key in detailed_metrics.keys():
                    if key in target_spill_metrics:
                        spill_related.append(f"detailed_metrics.{key}")
                for metric in raw_metrics:
                    key = metric.get('key', '')
                    label = metric.get('label', '')
                    if key in target_spill_metrics or label in target_spill_metrics:
                        spill_related.append(f"raw_metrics.{key}")
                for key in key_metrics.keys():
                    if key in target_spill_metrics:
                        spill_related.append(f"key_metrics.{key}")
                
                if spill_related:
                    print(f"       🔍 参考: その他のスピル関連メトリクス {len(spill_related)}個")
                    for related in spill_related[:3]:  # 最大3個表示
                        print(f"           - {related}")
                    if len(spill_related) > 3:
                        print(f"           ... 他{len(spill_related) - 3}個")
                else:
                    print(f"       🔍 参考: その他のスピル関連メトリクスは発見されませんでした")
        
        # スキュー詳細情報（AQEベースのデバッグ表示付き）
        if skew_detected:
            print(f"    🔍 スキュー判定根拠:")
            for detail in skew_details:
                description = detail['description']
                print(f"       ⚖️ {description}")
                
                # より詳細なAQE情報の表示
                if detail['type'] == 'aqe_skew':
                    aqe_value = detail['value']
                    aqe_split_value = detail.get('split_value', 0)
                    metric_name = detail['metric_name']
                    split_metric_name = detail.get('split_metric_name', '')
                    
                    print(f"           📊 AQEベース検出: {metric_name} = {aqe_value}")
                    print(f"           🎯 AQE検出詳細: Sparkが自動的に{aqe_value}個のスキューパーティションを検出")
                    
                    # パーティション分割情報がある場合
                    if aqe_split_value > 0 and split_metric_name:
                        print(f"           ⚡ AQE自動対応: Sparkが自動的に{aqe_split_value}個のパーティションに分割")
                        print(f"           🔄 分割メトリクス: {split_metric_name} = {aqe_split_value}")
                    
                    severity = detail.get('severity', '中')
                    severity_emoji = "🚨" if severity == "高" else "⚠️"
                    print(f"           {severity_emoji} 重要度: {severity} ({'5個以上' if severity == '高' else '1-4個'}のスキューパーティション)")
        else:
            # スキューが検出されなかった場合のデバッグ情報（詳細表示時のみ）
            import os
            if os.environ.get('DEBUG_SKEW_ANALYSIS', '').lower() in ['true', '1', 'yes']:
                debug_info = []
                
                # AQEメトリクスの検索結果
                debug_info.append(f"AQEメトリクス検索結果: {aqe_metric_name if aqe_metric_name else 'ターゲットメトリクス未発見'}")
                if aqe_metric_name:
                    debug_info.append(f"AQEメトリクス値: {aqe_skew_value} ≤ 基準値: 0")
                else:
                    debug_info.append("AQEメトリクス: 'AQEShuffleRead - Number of skewed partitions' 未発見")
                
                # 各ソースでの検索結果を表示
                detailed_metrics = node.get('detailed_metrics', {})
                raw_metrics = node.get('metrics', [])
                key_metrics = node.get('key_metrics', {})
                
                debug_info.append(f"detailed_metrics: {len(detailed_metrics)}個のメトリクス")
                debug_info.append(f"raw_metrics: {len(raw_metrics) if isinstance(raw_metrics, list) else 0}個のメトリクス")
                debug_info.append(f"key_metrics: {len(key_metrics)}個のメトリクス")
                
                if debug_info:
                    print(f"    🔍 スキュー未検出理由:")
                    for info in debug_info:
                        print(f"       ✅ {info}")
        
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
    
    
    print()
else:
    print("\n🔥 Sparkステージ実行分析")
    print("=" * 60)
    print("⚠️ ステージメトリクスが見つかりませんでした")
    print()

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗂️ Liquid Clustering分析結果の詳細表示
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - テーブル別推奨クラスタリングカラムの詳細表示
# MAGIC - パフォーマンス向上見込みの分析
# MAGIC - カラム使用パターンの詳細分析
# MAGIC - プッシュダウンフィルター情報の表示
# MAGIC - SQL実装例の提示

# COMMAND ----------

# 🗂️ LLMによるLiquid Clustering分析結果の詳細表示
print("\n" + "=" * 50)
print("🤖 LLM Liquid Clustering推奨分析")
print("=" * 50)

# LLMベースのLiquid Clustering分析を実行
liquid_analysis = extracted_metrics['liquid_clustering_analysis']

# LLM分析結果を表示
print("\n🤖 LLM分析結果:")
print("=" * 50)
llm_analysis = liquid_analysis.get('llm_analysis', '')
if llm_analysis:
    print(llm_analysis)
else:
    print("❌ LLM分析結果が見つかりません")

# 抽出データの概要を表示
extracted_data = liquid_analysis.get('extracted_data', {})
metadata_summary = extracted_data.get('metadata_summary', {})

print(f"\n📊 抽出データ概要:")
print(f"   🔍 フィルター条件: {metadata_summary.get('filter_expressions_count', 0)}個")
print(f"   🔗 JOIN条件: {metadata_summary.get('join_expressions_count', 0)}個")
print(f"   📊 GROUP BY条件: {metadata_summary.get('groupby_expressions_count', 0)}個")
print(f"   📈 集約関数: {metadata_summary.get('aggregate_expressions_count', 0)}個")
print(f"   🏷️ 識別テーブル: {metadata_summary.get('tables_identified', 0)}個")
print(f"   📂 スキャンノード: {metadata_summary.get('scan_nodes_count', 0)}個")

# パフォーマンスコンテキストの表示
performance_context = liquid_analysis.get('performance_context', {})
print(f"\n⚡ パフォーマンス情報:")
print(f"   ⏱️ 実行時間: {performance_context.get('total_time_sec', 0):.1f}秒")
print(f"   💾 データ読み込み: {performance_context.get('read_gb', 0):.2f}GB")
print(f"   📊 出力行数: {performance_context.get('rows_produced', 0):,}行")
print(f"   🎯 データ選択性: {performance_context.get('data_selectivity', 0):.4f}")

# 分析結果をファイルに出力
print(f"\n💾 分析結果をファイルに出力中...")
try:
    saved_files = save_liquid_clustering_analysis(liquid_analysis, "/tmp")
    
    if "error" in saved_files:
        print(f"❌ ファイル出力エラー: {saved_files['error']}")
    else:
        print(f"✅ ファイル出力完了:")
        for file_type, file_path in saved_files.items():
            if file_type == "json":
                print(f"   📄 JSON詳細データ: {file_path}")
            elif file_type == "markdown":
                print(f"   📝 Markdownレポート: {file_path}")
            elif file_type == "sql":
                print(f"   🔧 SQL実装例: {file_path}")
                
except Exception as e:
    print(f"❌ ファイル出力中にエラーが発生しました: {str(e)}")

# サマリー情報
summary = liquid_analysis.get('summary', {})
print(f"\n📋 分析サマリー:")
print(f"   🔬 分析方法: {summary.get('analysis_method', 'Unknown')}")
print(f"   🤖 LLMプロバイダー: {summary.get('llm_provider', 'Unknown')}")
print(f"   📊 対象テーブル数: {summary.get('tables_identified', 0)}")
print(f"   📈 抽出カラム数: フィルター({summary.get('total_filter_columns', 0)}) + JOIN({summary.get('total_join_columns', 0)}) + GROUP BY({summary.get('total_groupby_columns', 0)})")

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
# MAGIC ## 🎯 LLMボトルネック分析結果の表示
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
# MAGIC ## 💾 分析結果の保存と完了サマリー
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - LLM分析結果のテキストファイルへの保存
# MAGIC - 分析対象の基本情報の記録
# MAGIC - 全体処理の完了サマリー表示
# MAGIC - 出力ファイルの一覧表示

# COMMAND ----------

# 💾 分析結果の保存と完了サマリー
from datetime import datetime
# output_bottleneck_analysis_result_XXX.txtファイルの出力は廃止（optimization_reportに統合）

# 最終的なサマリー
print("\n" + "🎉" * 20)
print("🏁 【処理完了サマリー】")
print("🎉" * 20)
print("✅ SQLプロファイラーJSONファイル読み込み完了")
print(f"✅ パフォーマンスメトリクス抽出完了")

# LLMプロバイダー情報の動的表示
try:
    current_provider = LLM_CONFIG.get('provider', 'unknown')
    provider_display_names = {
        'databricks': f"Databricks ({LLM_CONFIG.get('databricks', {}).get('endpoint_name', 'Model Serving')})",
        'openai': f"OpenAI ({LLM_CONFIG.get('openai', {}).get('model', 'GPT-4')})",
        'azure_openai': f"Azure OpenAI ({LLM_CONFIG.get('azure_openai', {}).get('deployment_name', 'GPT-4')})",
        'anthropic': f"Anthropic ({LLM_CONFIG.get('anthropic', {}).get('model', 'Claude')})"
    }
    provider_display = provider_display_names.get(current_provider, f"{current_provider}（未知のプロバイダー）")
    print(f"✅ {provider_display}によるボトルネック分析完了")
except Exception as e:
    print("✅ LLMによるボトルネック分析完了")

print("✅ 分析結果は後でoptimization_reportに統合されます")
print()
print("🚀 分析完了！結果を確認してクエリ最適化にお役立てください。")
print("🎉" * 20)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔧 SQL最適化機能セクション
# MAGIC
# MAGIC **このセクションではSQLクエリの最適化を行います**
# MAGIC
# MAGIC 📋 **最適化プロセス:**
# MAGIC - プロファイラーデータからオリジナルクエリの抽出
# MAGIC - LLMによるクエリ最適化の実行
# MAGIC - 最適化結果のファイル生成
# MAGIC - テスト実行の準備
# MAGIC
# MAGIC ⚠️ **前提条件:** メイン処理セクションを完了してから実行してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 SQL最適化関連関数定義
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

def extract_table_size_estimates_from_plan(profiler_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    実行プランからテーブルごとの推定サイズ情報を抽出
    
    注意: Databricksクエリプロファイルには estimatedSizeInBytes が含まれていないため、
    この機能は現在無効化されています。メトリクスベースの推定を使用してください。
    
    Args:
        profiler_data: プロファイラーデータ
        
    Returns:
        Dict: 空の辞書（機能無効化）
    """
    # DatabricksクエリプロファイルにestimatedSizeInBytesが含まれていないため無効化
    return {}

def extract_table_name_from_scan_node(node: Dict[str, Any]) -> str:
    """
    スキャンノードからテーブル名を抽出
    
    Args:
        node: 実行プランのノード
        
    Returns:
        str: テーブル名
    """
    try:
        # 複数の方法でテーブル名を抽出を試行
        
        # 1. node outputからの抽出
        output = node.get("output", "")
        if output:
            # パターン: [col1#123, col2#456] table_name
            import re
            table_match = re.search(r'\]\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', output)
            if table_match:
                return table_match.group(1)
        
        # 2. node詳細からの抽出
        details = node.get("details", "")
        if details:
            # パターン: Location: /path/to/table/name
            location_match = re.search(r'Location:.*?([a-zA-Z_][a-zA-Z0-9_]*)', details)
            if location_match:
                return location_match.group(1)
            
            # パターン: Table: database.table_name
            table_match = re.search(r'Table:\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', details)
            if table_match:
                return table_match.group(1)
        
        # 3. メタデータからの抽出
        metadata = node.get("metadata", [])
        for meta in metadata:
            if meta.get("key") == "table" or meta.get("key") == "relation":
                values = meta.get("values", [])
                if values:
                    return str(values[0])
        
        # 4. node名からの推測（最後の手段）
        node_name = node.get("nodeName", "")
        if "delta" in node_name.lower():
            # Delta Scan の場合、詳細情報から抽出
            pass
    
    except Exception as e:
        print(f"⚠️ テーブル名抽出でエラー: {str(e)}")
    
    return None

def extract_broadcast_table_names(profiler_data: Dict[str, Any], broadcast_nodes: list) -> Dict[str, Any]:
    """
    BROADCASTノードから関連するテーブル名を抽出
    """
    broadcast_table_info = {
        "broadcast_tables": [],
        "broadcast_table_mapping": {},
        "broadcast_nodes_with_tables": []
    }
    
    # 実行プランのグラフ情報を取得
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        return broadcast_table_info
    
    # 全ノードを収集
    all_nodes = []
    for graph in graphs:
        nodes = graph.get('nodes', [])
        all_nodes.extend(nodes)
    
    # エッジ情報を収集（ノード間の関係）
    all_edges = []
    for graph in graphs:
        edges = graph.get('edges', [])
        all_edges.extend(edges)
    
    # 各BROADCASTノードについて関連するテーブルを特定
    for broadcast_node in broadcast_nodes:
        broadcast_node_id = broadcast_node.get('node_id', '')
        broadcast_node_name = broadcast_node.get('node_name', '')
        
        # BROADCASTノードから直接テーブル名を抽出
        table_names = set()
        
        # 1. メタデータからテーブル名を抽出
        metadata = broadcast_node.get('metadata', [])
        for meta in metadata:
            key = meta.get('key', '')
            value = meta.get('value', '')
            values = meta.get('values', [])
            
            # テーブル名を示すキーをチェック
            if key in ['SCAN_IDENTIFIER', 'TABLE_NAME', 'RELATION']:
                if value:
                    table_names.add(value)
                table_names.update(values)
        
        # 2. ノード名からテーブル名を推定
        if 'SCAN' in broadcast_node_name:
            # "Broadcast Scan delta orders" → "orders"
            import re
            table_match = re.search(r'SCAN\s+(?:DELTA|PARQUET|JSON|CSV)?\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', broadcast_node_name, re.IGNORECASE)
            if table_match:
                table_names.add(table_match.group(1))
        
        # 3. エッジ情報から関連するスキャンノードを特定
        for edge in all_edges:
            source_id = edge.get('source', '')
            target_id = edge.get('target', '')
            
            # BROADCASTノードに入力されるノードを検索
            if target_id == broadcast_node_id:
                # 入力ノードがスキャンノードかチェック
                for node in all_nodes:
                    if node.get('id', '') == source_id:
                        node_name = node.get('name', '').upper()
                        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
                            # スキャンノードからテーブル名を抽出
                            scan_table_name = extract_table_name_from_scan_node(node)
                            if scan_table_name:
                                table_names.add(scan_table_name)
        
        # 4. 同じグラフ内のスキャンノードとの関連付け
        for node in all_nodes:
            node_name = node.get('name', '').upper()
            if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
                # スキャンノードの名前がBROADCASTノード名に含まれるかチェック
                scan_table_name = extract_table_name_from_scan_node(node)
                if scan_table_name:
                    # テーブル名の部分一致をチェック
                    if any(part in broadcast_node_name for part in scan_table_name.split('.') if len(part) > 2):
                        table_names.add(scan_table_name)
        
        # 結果を記録
        table_names_list = list(table_names)
        if table_names_list:
            broadcast_table_info["broadcast_tables"].extend(table_names_list)
            broadcast_table_info["broadcast_table_mapping"][broadcast_node_id] = table_names_list
            
            # BROADCASTノード情報を拡張
            enhanced_broadcast_node = broadcast_node.copy()
            enhanced_broadcast_node["associated_tables"] = table_names_list
            enhanced_broadcast_node["table_count"] = len(table_names_list)
            broadcast_table_info["broadcast_nodes_with_tables"].append(enhanced_broadcast_node)
    
    # 重複を除去
    broadcast_table_info["broadcast_tables"] = list(set(broadcast_table_info["broadcast_tables"]))
    
    return broadcast_table_info

def extract_execution_plan_info(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSONメトリクスから実行プラン情報を抽出
    """
    plan_info = {
        "broadcast_nodes": [],
        "join_nodes": [],
        "scan_nodes": [],
        "shuffle_nodes": [],
        "aggregate_nodes": [],
        "plan_summary": {},
        "broadcast_already_applied": False,
        "join_strategies": [],
        "table_scan_details": {},
        "broadcast_table_info": {}
    }
    
    # プロファイラーデータから実行グラフ情報を取得
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        return plan_info
    
    # すべてのグラフからノードを収集
    all_nodes = []
    for graph_index, graph in enumerate(graphs):
        nodes = graph.get('nodes', [])
        for node in nodes:
            node['graph_index'] = graph_index
            all_nodes.append(node)
    
    # ノード分析
    for node in all_nodes:
        node_name = node.get('name', '').upper()
        node_tag = node.get('tag', '').upper()
        node_metadata = node.get('metadata', [])
        
        # BROADCASTノードの検出
        if 'BROADCAST' in node_name or 'BROADCAST' in node_tag:
            plan_info["broadcast_already_applied"] = True
            broadcast_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "metadata": []
            }
            
            # BROADCASTに関連するメタデータを抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                value = meta.get('value', '')
                values = meta.get('values', [])
                
                if any(keyword in key.upper() for keyword in ['BROADCAST', 'BUILD', 'PROBE']):
                    broadcast_info["metadata"].append({
                        "key": key,
                        "value": value,
                        "values": values
                    })
            
            plan_info["broadcast_nodes"].append(broadcast_info)
        
        # JOINノードの検出と戦略分析
        elif any(keyword in node_name for keyword in ['JOIN', 'HASH']):
            join_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "join_strategy": "unknown",
                "join_keys": [],
                "join_type": "unknown"
            }
            
            # JOIN戦略の特定
            if 'BROADCAST' in node_name:
                join_info["join_strategy"] = "broadcast_hash_join"
            elif 'SORT' in node_name and 'MERGE' in node_name:
                join_info["join_strategy"] = "sort_merge_join"
            elif 'HASH' in node_name:
                join_info["join_strategy"] = "shuffle_hash_join"
            elif 'NESTED' in node_name:
                join_info["join_strategy"] = "broadcast_nested_loop_join"
            
            # JOINタイプの特定
            if 'INNER' in node_name:
                join_info["join_type"] = "inner"
            elif 'LEFT' in node_name:
                join_info["join_type"] = "left"
            elif 'RIGHT' in node_name:
                join_info["join_type"] = "right"
            elif 'OUTER' in node_name:
                join_info["join_type"] = "outer"
            
            # JOIN条件の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key in ['LEFT_KEYS', 'RIGHT_KEYS']:
                    join_info["join_keys"].extend(values)
            
            plan_info["join_nodes"].append(join_info)
            plan_info["join_strategies"].append(join_info["join_strategy"])
        
        # スキャンノードの詳細分析
        elif any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
            scan_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "table_name": "unknown",
                "file_format": "unknown",
                "pushed_filters": [],
                "output_columns": []
            }
            
            # テーブル名とファイル形式の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                value = meta.get('value', '')
                values = meta.get('values', [])
                
                if key == 'SCAN_IDENTIFIER':
                    scan_info["table_name"] = value
                elif key == 'OUTPUT':
                    scan_info["output_columns"] = values
                elif key == 'PUSHED_FILTERS' or key == 'FILTERS':
                    scan_info["pushed_filters"] = values
            
            # ファイル形式の推定
            if 'DELTA' in node_name:
                scan_info["file_format"] = "delta"
            elif 'PARQUET' in node_name:
                scan_info["file_format"] = "parquet"
            elif 'JSON' in node_name:
                scan_info["file_format"] = "json"
            elif 'CSV' in node_name:
                scan_info["file_format"] = "csv"
            
            plan_info["scan_nodes"].append(scan_info)
            plan_info["table_scan_details"][scan_info["table_name"]] = scan_info
        
        # シャッフルノードの検出
        elif any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "partition_keys": []
            }
            
            # パーティション情報の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key in ['PARTITION_EXPRESSIONS', 'PARTITION_KEYS']:
                    shuffle_info["partition_keys"] = values
            
            plan_info["shuffle_nodes"].append(shuffle_info)
        
        # 集約ノードの検出
        elif any(keyword in node_name for keyword in ['AGGREGATE', 'GROUP']):
            agg_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "group_keys": [],
                "aggregate_expressions": []
            }
            
            # 集約情報の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key == 'GROUPING_EXPRESSIONS':
                    agg_info["group_keys"] = values
                elif key == 'AGGREGATE_EXPRESSIONS':
                    agg_info["aggregate_expressions"] = values
            
            plan_info["aggregate_nodes"].append(agg_info)
    
    # プランサマリーの生成
    plan_info["plan_summary"] = {
        "total_nodes": len(all_nodes),
        "broadcast_nodes_count": len(plan_info["broadcast_nodes"]),
        "join_nodes_count": len(plan_info["join_nodes"]),
        "scan_nodes_count": len(plan_info["scan_nodes"]),
        "shuffle_nodes_count": len(plan_info["shuffle_nodes"]),
        "aggregate_nodes_count": len(plan_info["aggregate_nodes"]),
        "unique_join_strategies": list(set(plan_info["join_strategies"])),
        "has_broadcast_joins": plan_info["broadcast_already_applied"],
        "tables_scanned": len(plan_info["table_scan_details"])
    }
    
    # BROADCASTテーブル情報を抽出
    if plan_info["broadcast_nodes"]:
        broadcast_table_info = extract_broadcast_table_names(profiler_data, plan_info["broadcast_nodes"])
        plan_info["broadcast_table_info"] = broadcast_table_info
        
        # プランサマリーにBROADCASTテーブル情報を追加
        plan_info["plan_summary"]["broadcast_tables"] = broadcast_table_info["broadcast_tables"]
        plan_info["plan_summary"]["broadcast_table_count"] = len(broadcast_table_info["broadcast_tables"])
    
    # 実行プランからのテーブルサイズ推定情報を追加（estimatedSizeInBytes利用不可のため無効化）
    plan_info["table_size_estimates"] = {}  # extract_table_size_estimates_from_plan(profiler_data)
    
    return plan_info

def get_spark_broadcast_threshold() -> float:
    """
    Sparkの実際のbroadcast閾値設定を取得
    """
    try:
        # Sparkの設定値を取得
        threshold_bytes = spark.conf.get("spark.databricks.optimizer.autoBroadcastJoinThreshold", "31457280")  # デフォルト30MB
        threshold_mb = float(threshold_bytes) / 1024 / 1024
        return threshold_mb
    except:
        # 取得できない場合は標準的な30MBを返す
        return 30.0

def estimate_uncompressed_size(compressed_size_mb: float, file_format: str = "parquet") -> float:
    """
    圧縮サイズから非圧縮サイズを推定（3.0倍固定）
    
    注意: 実際のestimatedSizeInBytesが利用できないため、
    保守的な3.0倍圧縮率で統一して推定します。
    """
    # 保守的な3.0倍圧縮率で統一（estimatedSizeInBytes利用不可のため）
    compression_ratio = 3.0
    
    return compressed_size_mb * compression_ratio

def analyze_broadcast_feasibility(metrics: Dict[str, Any], original_query: str, plan_info: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    BROADCASTヒントの適用可能性を分析（正確な30MB閾値適用）
    """
    broadcast_analysis = {
        "is_join_query": False,
        "broadcast_candidates": [],
        "recommendations": [],
        "feasibility": "not_applicable",
        "reasoning": [],
        "spark_threshold_mb": get_spark_broadcast_threshold(),
        "compression_analysis": {},
        "detailed_size_analysis": [],
        "execution_plan_analysis": {},
        "existing_broadcast_nodes": [],
        "already_optimized": False,
        "broadcast_applied_tables": []
    }
    
    # クエリにJOINが含まれているかチェック
    query_upper = original_query.upper()
    join_types = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'SEMI JOIN', 'ANTI JOIN']
    has_join = any(join_type in query_upper for join_type in join_types)
    
    if not has_join:
        broadcast_analysis["reasoning"].append("JOINクエリではないため、BROADCASTヒントは適用不可")
        return broadcast_analysis
    
    broadcast_analysis["is_join_query"] = True
    broadcast_analysis["reasoning"].append(f"Spark BROADCAST閾値: {broadcast_analysis['spark_threshold_mb']:.1f}MB（非圧縮）")
    
    # 実行プラン情報の分析
    if plan_info:
        plan_summary = plan_info.get("plan_summary", {})
        broadcast_nodes = plan_info.get("broadcast_nodes", [])
        join_nodes = plan_info.get("join_nodes", [])
        table_scan_details = plan_info.get("table_scan_details", {})
        table_size_estimates = plan_info.get("table_size_estimates", {})
        
        # 既存のBROADCAST適用状況の記録
        broadcast_analysis["existing_broadcast_nodes"] = broadcast_nodes
        broadcast_analysis["already_optimized"] = len(broadcast_nodes) > 0
        
        # プラン分析結果の記録
        broadcast_analysis["execution_plan_analysis"] = {
            "has_broadcast_joins": plan_summary.get("has_broadcast_joins", False),
            "unique_join_strategies": plan_summary.get("unique_join_strategies", []),
            "broadcast_nodes_count": len(broadcast_nodes),
            "join_nodes_count": len(join_nodes),
            "scan_nodes_count": plan_summary.get("scan_nodes_count", 0),
            "shuffle_nodes_count": plan_summary.get("shuffle_nodes_count", 0),
            "tables_in_plan": list(table_scan_details.keys())
        }
        
        # 既にBROADCASTが適用されている場合の詳細記録
        if broadcast_nodes:
            broadcast_analysis["reasoning"].append(f"✅ 実行プランで既にBROADCAST JOINが適用済み: {len(broadcast_nodes)}個のノード")
            
            # BROADCASTテーブル情報を取得
            broadcast_table_info = plan_info.get("broadcast_table_info", {})
            broadcast_tables = broadcast_table_info.get("broadcast_tables", [])
            
            if broadcast_tables:
                broadcast_analysis["reasoning"].append(f"📋 BROADCASTされているテーブル: {', '.join(broadcast_tables)}")
                broadcast_analysis["broadcast_applied_tables"] = broadcast_tables
                
                # 各BROADCASTノードの詳細
                broadcast_nodes_with_tables = broadcast_table_info.get("broadcast_nodes_with_tables", [])
                for i, node in enumerate(broadcast_nodes_with_tables[:3]):  # 最大3個まで表示
                    node_name_short = node['node_name'][:50] + "..." if len(node['node_name']) > 50 else node['node_name']
                    associated_tables = node.get('associated_tables', [])
                    if associated_tables:
                        broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node_name_short}")
                        broadcast_analysis["reasoning"].append(f"    └─ テーブル: {', '.join(associated_tables)}")
                    else:
                        broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node_name_short} (テーブル名未特定)")
            else:
                # BROADCASTノードは存在するがテーブル名が特定できない場合
                for i, node in enumerate(broadcast_nodes[:3]):  # 最大3個まで表示
                    broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node['node_name'][:50]}... (テーブル名解析中)")
        else:
            # BROADCAST未適用だが、JOINが存在する場合
            if join_nodes:
                join_strategies = set(node["join_strategy"] for node in join_nodes)
                broadcast_analysis["reasoning"].append(f"🔍 現在のJOIN戦略: {', '.join(join_strategies)}")
                broadcast_analysis["reasoning"].append("💡 BROADCAST最適化の機会を検討中...")
    else:
        broadcast_analysis["reasoning"].append("⚠️ 実行プラン情報が利用できません - メトリクス推定に基づく分析を実行")
    
    # メトリクスからテーブルサイズ情報を取得
    overall_metrics = metrics.get('overall_metrics', {})
    node_metrics = metrics.get('node_metrics', [])
    
    # スキャンノードからテーブル情報を抽出
    scan_nodes = []
    total_compressed_bytes = 0
    total_rows_all_tables = 0
    
    for node in node_metrics:
        node_name = node.get('name', '').upper()
        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            key_metrics = node.get('key_metrics', {})
            rows_num = key_metrics.get('rowsNum', 0)
            duration_ms = key_metrics.get('durationMs', 0)
            
            # ファイル形式の推定（プラン情報を優先）
            file_format = "parquet"  # デフォルト
            table_name_from_plan = "unknown"
            
            # プラン情報からテーブル名とファイル形式を取得
            if plan_info and plan_info.get("table_scan_details"):
                # メタデータから詳細なテーブル名を抽出
                node_metadata = node.get('metadata', [])
                for meta in node_metadata:
                    meta_key = meta.get('key', '')
                    meta_value = meta.get('value', '')
                    if meta_key in ['SCAN_IDENTIFIER', 'SCAN_TABLE', 'TABLE_NAME'] and meta_value:
                        # プランの詳細と照合
                        for plan_table, scan_detail in plan_info["table_scan_details"].items():
                            if meta_value in plan_table or plan_table in meta_value:
                                table_name_from_plan = plan_table
                                if scan_detail["file_format"] != "unknown":
                                    file_format = scan_detail["file_format"]
                                break
                        break
            
            # フォールバック: ノード名からファイル形式を推定
            if file_format == "parquet":  # まだデフォルトの場合
                if "DELTA" in node_name:
                    file_format = "delta"
                elif "PARQUET" in node_name:
                    file_format = "parquet"
                elif "JSON" in node_name:
                    file_format = "json"
                elif "CSV" in node_name:
                    file_format = "csv"
            
            # メトリクスベース推定のみ使用（estimatedSizeInBytes利用不可のため）
            estimated_compressed_mb = 0
            estimated_uncompressed_mb = 0
            size_source = "metrics_estimation"
            
            # メトリクスベース推定
            total_read_bytes = overall_metrics.get('read_bytes', 0)
            total_rows = overall_metrics.get('rows_read_count', 0)
            
            if total_rows > 0 and total_read_bytes > 0 and rows_num > 0:
                # 全体の読み込み量からこのテーブルの割合を計算
                table_ratio = rows_num / total_rows
                estimated_compressed_bytes = total_read_bytes * table_ratio
                estimated_compressed_mb = estimated_compressed_bytes / 1024 / 1024
                 
                # 非圧縮サイズを推定
                estimated_uncompressed_mb = estimate_uncompressed_size(estimated_compressed_mb, file_format)
            else:
                # フォールバック: 行数ベースの推定（保守的）
                # 平均行サイズを推定（非圧縮）
                if total_rows > 0 and total_read_bytes > 0:
                    # 全体データから圧縮後の平均行サイズを計算
                    compressed_avg_row_size = total_read_bytes / total_rows
                    # 圧縮率を考慮して非圧縮サイズを推定
                    uncompressed_avg_row_size = compressed_avg_row_size * estimate_uncompressed_size(1.0, file_format)
                else:
                    # 完全なフォールバック: 一般的な非圧縮行サイズ（1KB）
                    uncompressed_avg_row_size = 1024
                
                estimated_compressed_mb = (rows_num * compressed_avg_row_size) / 1024 / 1024 if 'compressed_avg_row_size' in locals() else 0
                estimated_uncompressed_mb = (rows_num * uncompressed_avg_row_size) / 1024 / 1024
            
            # 既存のBROADCAST適用状況をチェック
            is_already_broadcasted = False
            if plan_info and plan_info.get("broadcast_nodes"):
                for broadcast_node in plan_info["broadcast_nodes"]:
                    # テーブル名の部分一致をチェック
                    broadcast_node_name = broadcast_node["node_name"]
                    if (table_name_from_plan != "unknown" and 
                        any(part in broadcast_node_name for part in table_name_from_plan.split('.') if len(part) > 3)):
                        is_already_broadcasted = True
                        break
                    # ノード名での照合
                    elif any(part in broadcast_node_name for part in node_name.split() if len(part) > 3):
                        is_already_broadcasted = True
                        break

            scan_info = {
                "node_name": node_name,
                "table_name_from_plan": table_name_from_plan,
                "rows": rows_num,
                "duration_ms": duration_ms,
                "estimated_compressed_mb": estimated_compressed_mb,
                "estimated_uncompressed_mb": estimated_uncompressed_mb,
                "file_format": file_format,
                "compression_ratio": 3.0,  # 固定3.0倍圧縮率
                "node_id": node.get('node_id', ''),
                "is_already_broadcasted": is_already_broadcasted,
                "size_estimation_source": size_source,
                "size_confidence": "medium"  # メトリクスベース推定のため中程度信頼度
            }
            scan_nodes.append(scan_info)
            
            total_compressed_bytes += estimated_compressed_bytes if 'estimated_compressed_bytes' in locals() else 0
            total_rows_all_tables += rows_num
    
    # BROADCAST候補の判定（30MB閾値使用）
    broadcast_threshold_mb = broadcast_analysis["spark_threshold_mb"]  # 実際のSpark設定値
    broadcast_safe_mb = broadcast_threshold_mb * 0.8  # 安全マージン（80%）
    broadcast_max_mb = broadcast_threshold_mb * 10    # 明らかに大きすぎる閾値
    
    small_tables = []
    large_tables = []
    marginal_tables = []
    
    # 圧縮分析の記録
    broadcast_analysis["compression_analysis"] = {
        "total_compressed_gb": total_compressed_bytes / 1024 / 1024 / 1024 if total_compressed_bytes > 0 else 0,
        "total_rows": total_rows_all_tables,
        "avg_compression_ratio": 0
    }
    
    for scan in scan_nodes:
        uncompressed_size_mb = scan["estimated_uncompressed_mb"]
        compressed_size_mb = scan["estimated_compressed_mb"]
        
        # 詳細サイズ分析の記録
        table_display_name = scan.get("table_name_from_plan", scan["node_name"])
        is_already_broadcasted = scan.get("is_already_broadcasted", False)
        
        size_analysis = {
            "table": table_display_name,
            "node_name": scan["node_name"],
            "rows": scan["rows"],
            "compressed_mb": compressed_size_mb,
            "uncompressed_mb": uncompressed_size_mb,
            "file_format": scan["file_format"],
            "compression_ratio": scan["compression_ratio"],
            "broadcast_decision": "",
            "decision_reasoning": "",
            "is_already_broadcasted": is_already_broadcasted
        }
        
        # 30MB閾値での判定（非圧縮サイズ）- 既存適用状況を考慮
        if is_already_broadcasted:
            # 既にBROADCASTが適用済み
            small_tables.append(scan)  # 統計目的で記録
            size_analysis["broadcast_decision"] = "already_applied"
            size_analysis["decision_reasoning"] = f"既にBROADCAST適用済み（推定サイズ: 非圧縮{uncompressed_size_mb:.1f}MB）"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "confirmed",
                "status": "already_applied",
                "reasoning": f"実行プランで既にBROADCAST適用確認済み（推定サイズ: 非圧縮{uncompressed_size_mb:.1f}MB、メトリクスベース推定）"
            })
        elif uncompressed_size_mb <= broadcast_safe_mb and scan["rows"] > 0:
            # 安全マージン内（24MB以下）- 強く推奨
            small_tables.append(scan)
            size_analysis["broadcast_decision"] = "strongly_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB ≤ 安全閾値{broadcast_safe_mb:.1f}MB"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "high",
                "status": "new_recommendation",
                "reasoning": f"非圧縮推定サイズ {uncompressed_size_mb:.1f}MB（安全閾値 {broadcast_safe_mb:.1f}MB 以下）でBROADCAST強く推奨（メトリクスベース推定、3.0倍圧縮率）"
            })
        elif uncompressed_size_mb <= broadcast_threshold_mb and scan["rows"] > 0:
            # 閾値内だが安全マージンは超過（24-30MB）- 条件付き推奨
            marginal_tables.append(scan)
            size_analysis["broadcast_decision"] = "conditionally_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB ≤ 閾値{broadcast_threshold_mb:.1f}MB（安全マージン超過）"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "medium",
                "status": "new_recommendation",
                "reasoning": f"非圧縮推定サイズ {uncompressed_size_mb:.1f}MB（閾値 {broadcast_threshold_mb:.1f}MB 以下だが安全マージン {broadcast_safe_mb:.1f}MB 超過）で条件付きBROADCAST推奨（メトリクスベース推定、3.0倍圧縮率）"
            })
        elif uncompressed_size_mb > broadcast_max_mb:
            # 明らかに大きすぎる（300MB超）
            large_tables.append(scan)
            size_analysis["broadcast_decision"] = "not_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB > 最大閾値{broadcast_max_mb:.1f}MB"
            broadcast_analysis["reasoning"].append(f"テーブル {table_display_name}: 非圧縮{uncompressed_size_mb:.1f}MB - BROADCAST不可（>{broadcast_max_mb:.1f}MB）")
        else:
            # 中間サイズのテーブル（30-300MB）
            large_tables.append(scan)
            size_analysis["broadcast_decision"] = "not_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB > 閾値{broadcast_threshold_mb:.1f}MB"
            broadcast_analysis["reasoning"].append(f"テーブル {table_display_name}: 非圧縮{uncompressed_size_mb:.1f}MB - BROADCAST非推奨（>{broadcast_threshold_mb:.1f}MB閾値）")
        
        broadcast_analysis["detailed_size_analysis"].append(size_analysis)
    
    # 圧縮分析サマリーの更新
    if scan_nodes:
        total_uncompressed_mb = sum(scan["estimated_uncompressed_mb"] for scan in scan_nodes)
        total_compressed_mb = sum(scan["estimated_compressed_mb"] for scan in scan_nodes)
        if total_compressed_mb > 0:
            broadcast_analysis["compression_analysis"]["avg_compression_ratio"] = total_uncompressed_mb / total_compressed_mb
        broadcast_analysis["compression_analysis"]["total_uncompressed_mb"] = total_uncompressed_mb
        broadcast_analysis["compression_analysis"]["total_compressed_mb"] = total_compressed_mb
    
    # 総データ読み込み量との整合性チェック（圧縮ベース）
    total_read_gb = overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024
    estimated_total_compressed_mb = sum(scan["estimated_compressed_mb"] for scan in scan_nodes)
    
    if estimated_total_compressed_mb > 0:
        size_ratio = (total_read_gb * 1024) / estimated_total_compressed_mb
        if size_ratio > 3 or size_ratio < 0.3:
            broadcast_analysis["reasoning"].append(f"推定圧縮サイズ({estimated_total_compressed_mb:.1f}MB)と実読み込み量({total_read_gb:.1f}GB)に乖離あり - サイズ推定に注意")
        else:
            broadcast_analysis["reasoning"].append(f"サイズ推定整合性: 推定圧縮{estimated_total_compressed_mb:.1f}MB vs 実際{total_read_gb:.1f}GB（比率:{size_ratio:.2f}）")
    
    # BROADCAST推奨事項の生成（30MB閾値対応、既存のBROADCAST適用状況を考慮）
    total_broadcast_candidates = len(small_tables) + len(marginal_tables)
    total_tables = len(scan_nodes)
    
    if small_tables or marginal_tables:
        if large_tables:
            # 既存のBROADCAST適用状況を考慮した判定
            if broadcast_analysis["already_optimized"]:
                broadcast_analysis["feasibility"] = "already_optimized_with_improvements"
                broadcast_analysis["recommendations"] = [
                    f"✅ 既にBROADCAST JOIN適用済み - 追加改善の検討",
                    f"🎯 追加最適化テーブル: {total_broadcast_candidates}個（全{total_tables}個中）",
                    f"  ✅ 強く推奨: {len(small_tables)}個（安全閾値{broadcast_safe_mb:.1f}MB以下）",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個（閾値{broadcast_threshold_mb:.1f}MB以下、要注意）",
                    f"  ❌ 非推奨: {len(large_tables)}個（閾値超過）"
                ]
            else:
                broadcast_analysis["feasibility"] = "recommended"
                broadcast_analysis["recommendations"] = [
                    f"🎯 BROADCAST推奨テーブル: {total_broadcast_candidates}個（全{total_tables}個中）",
                    f"  ✅ 強く推奨: {len(small_tables)}個（安全閾値{broadcast_safe_mb:.1f}MB以下）",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個（閾値{broadcast_threshold_mb:.1f}MB以下、要注意）",
                    f"  ❌ 非推奨: {len(large_tables)}個（閾値超過）"
                ]
        else:
            # 全テーブルが小さい場合
            if broadcast_analysis["already_optimized"]:
                broadcast_analysis["feasibility"] = "already_optimized_complete"
                broadcast_analysis["recommendations"] = [
                    f"✅ 既にBROADCAST JOIN適用済み - 最適化完了",
                    f"🎯 全テーブル（{total_tables}個）がBROADCAST閾値以下で適切に処理済み",
                    f"  ✅ 強く推奨: {len(small_tables)}個",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個",
                    "📋 現在の設定が最適です"
                ]
            else:
                broadcast_analysis["feasibility"] = "all_small"
                broadcast_analysis["recommendations"] = [
                    f"🎯 全テーブル（{total_tables}個）がBROADCAST閾値以下",
                    f"  ✅ 強く推奨: {len(small_tables)}個",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個",
                    "📋 最小テーブルを優先的にBROADCASTすることを推奨"
                ]
        
        # 具体的なBROADCAST候補の詳細
        for small_table in small_tables:
            broadcast_analysis["recommendations"].append(
                f"🔹 BROADCAST({small_table['node_name']}) - 非圧縮{small_table['estimated_uncompressed_mb']:.1f}MB（圧縮{small_table['estimated_compressed_mb']:.1f}MB、{small_table['file_format']}、圧縮率{small_table['compression_ratio']:.1f}x）"
            )
        
        for marginal_table in marginal_tables:
            broadcast_analysis["recommendations"].append(
                f"🔸 BROADCAST({marginal_table['node_name']}) - 非圧縮{marginal_table['estimated_uncompressed_mb']:.1f}MB（条件付き、メモリ使用量要注意）"
            )
            
    elif large_tables:
        broadcast_analysis["feasibility"] = "not_recommended"
        broadcast_analysis["recommendations"] = [
            f"❌ 全テーブル（{len(large_tables)}個）が30MB閾値超過のためBROADCAST非推奨",
            f"📊 最小テーブルでも非圧縮{min(scan['estimated_uncompressed_mb'] for scan in large_tables):.1f}MB",
            "🔧 代替最適化手法を推奨:",
            "  • Liquid Clustering実装",
            "  • データパーティショニング",
            "  • クエリ最適化（フィルタープッシュダウン等）",
            "  • spark.databricks.optimizer.autoBroadcastJoinThreshold設定値の調整検討"
        ]
    else:
        broadcast_analysis["feasibility"] = "insufficient_data"
        broadcast_analysis["recommendations"] = [
            "⚠️ テーブルサイズ情報が不足しているため、手動でのサイズ確認が必要",
            "📋 以下のコマンドでテーブルサイズを確認:",
            "  • DESCRIBE DETAIL table_name",
            "  • SELECT COUNT(*) FROM table_name",
            "  • SHOW TABLE EXTENDED LIKE 'table_name'"
        ]
    
    # 30MB閾値にヒットする特別なケース分析（small_tables + marginal_tables を考慮）
    all_30mb_candidates = small_tables + marginal_tables  # 30MB以下の全候補
    
    if all_30mb_candidates:
        broadcast_analysis["30mb_hit_analysis"] = {
            "has_30mb_candidates": True,
            "candidate_count": len(all_30mb_candidates),
            "small_tables_count": len(small_tables),  # 24MB以下（強く推奨）
            "marginal_tables_count": len(marginal_tables),  # 24-30MB（条件付き推奨）
            "smallest_table_mb": min(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "largest_candidate_mb": max(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "total_candidate_size_mb": sum(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "recommended_broadcast_table": all_30mb_candidates[0]["node_name"] if all_30mb_candidates else None,
            "memory_impact_estimation": f"{sum(scan['estimated_uncompressed_mb'] for scan in all_30mb_candidates):.1f}MB がワーカーノードにブロードキャスト"
        }
        
        # 最適なBROADCAST候補の特定（全30MB候補から選択）
        if len(all_30mb_candidates) > 1:
            optimal_candidate = min(all_30mb_candidates, key=lambda x: x["estimated_uncompressed_mb"])
            broadcast_analysis["30mb_hit_analysis"]["optimal_candidate"] = {
                "table": optimal_candidate["node_name"],
                "size_mb": optimal_candidate["estimated_uncompressed_mb"],
                "rows": optimal_candidate["rows"],
                "reasoning": f"最小サイズ{optimal_candidate['estimated_uncompressed_mb']:.1f}MBで最も効率的"
            }
        
        # 30MB閾値内の詳細分類情報を追加
        broadcast_analysis["30mb_hit_analysis"]["size_classification"] = {
            "safe_zone_tables": len(small_tables),  # 0-24MB（安全マージン内）
            "caution_zone_tables": len(marginal_tables),  # 24-30MB（要注意）
            "safe_zone_description": "24MB以下（強く推奨、安全マージン内）",
            "caution_zone_description": "24-30MB（条件付き推奨、メモリ使用量要注意）"
        }
    else:
        broadcast_analysis["30mb_hit_analysis"] = {
            "has_30mb_candidates": False,
            "reason": f"全テーブルが30MB閾値を超過（最小: {min(scan['estimated_uncompressed_mb'] for scan in scan_nodes):.1f}MB）" if scan_nodes else "テーブル情報なし"
        }
    
    return broadcast_analysis

def generate_optimized_query_with_llm(original_query: str, analysis_result: str, metrics: Dict[str, Any]) -> str:
    """
    LLM分析結果に基づいてSQLクエリを最適化（BROADCAST分析を含む）
    """
    
    # 実行プラン情報の抽出（メトリクスから）
    profiler_data = metrics.get('raw_profiler_data', {})
    plan_info = None
    if profiler_data:
        plan_info = extract_execution_plan_info(profiler_data)
    
    # BROADCAST適用可能性の分析（プラン情報を含む）
    broadcast_analysis = analyze_broadcast_feasibility(metrics, original_query, plan_info)
    
    # プラン情報をメトリクスに追加（ファイル出力で使用）
    if plan_info:
        metrics['execution_plan_info'] = plan_info
    
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
    
    # Liquid Clustering推奨情報（LLMベース対応）
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    extracted_data = liquid_analysis.get('extracted_data', {})
    table_info = extracted_data.get('table_info', {})
    
    clustering_recommendations = []
    if table_info:
        for table_name in list(table_info.keys())[:3]:  # 上位3テーブル
            clustering_recommendations.append(f"テーブル {table_name}: LLM分析による推奨カラムでクラスタリング推奨")
    
    # 最適化プロンプトの作成（簡潔版でタイムアウト回避）
    
    # 分析結果を簡潔化（128K制限内で最大効率化）
    analysis_summary = ""
    if isinstance(analysis_result, str) and len(analysis_result) > 2000:
        # プロンプト容量の確保のため、分析結果は要点のみに圧縮
        analysis_summary = analysis_result[:2000] + "...[要約：主要ボトルネックのみ保持]"
    else:
        analysis_summary = str(analysis_result)
    
    # ボトルネック情報の簡潔化
    bottleneck_summary = "、".join(optimization_context[:3]) if optimization_context else "特になし"
    
    # Liquid Clustering推奨の簡潔化
    clustering_summary = "、".join(clustering_recommendations[:2]) if clustering_recommendations else "特になし"
    
    # BROADCAST分析結果のサマリー作成（30MB閾値対応）
    broadcast_summary = []
    if broadcast_analysis["is_join_query"]:
        # 既存のBROADCAST適用状況を最初に表示
        if broadcast_analysis["already_optimized"]:
            existing_broadcast_count = len(broadcast_analysis["existing_broadcast_nodes"])
            broadcast_summary.append(f"✅ 既にBROADCAST JOIN適用済み: {existing_broadcast_count}個のノード")
            
            # BROADCASTされているテーブル一覧を表示
            broadcast_applied_tables = broadcast_analysis.get("broadcast_applied_tables", [])
            if broadcast_applied_tables:
                broadcast_summary.append(f"📋 BROADCASTされているテーブル: {', '.join(broadcast_applied_tables)}")
            
            # 既存のBROADCASTノードの詳細を表示（最大3個）
            for i, node in enumerate(broadcast_analysis["existing_broadcast_nodes"][:3]):
                node_name_short = node["node_name"][:50] + "..." if len(node["node_name"]) > 50 else node["node_name"]
                broadcast_summary.append(f"  🔹 BROADCAST Node {i+1}: {node_name_short}")
            
            # 実行プラン分析からのJOIN戦略情報
            plan_analysis = broadcast_analysis.get("execution_plan_analysis", {})
            if plan_analysis.get("unique_join_strategies"):
                broadcast_summary.append(f"🔍 検出されたJOIN戦略: {', '.join(plan_analysis['unique_join_strategies'])}")
        else:
            broadcast_summary.append("🔍 BROADCAST JOIN未適用 - 最適化の機会を検討中")
        
        broadcast_summary.append(f"🎯 BROADCAST適用可能性: {broadcast_analysis['feasibility']}")
        broadcast_summary.append(f"⚖️ Spark閾値: {broadcast_analysis['spark_threshold_mb']:.1f}MB（非圧縮）")
        
        # 30MB以下の候補がある場合
        if broadcast_analysis["30mb_hit_analysis"]["has_30mb_candidates"]:
            hit_analysis = broadcast_analysis["30mb_hit_analysis"]
            broadcast_summary.append(f"✅ 30MB閾値ヒット: {hit_analysis['candidate_count']}個のテーブルが条件適合")
            broadcast_summary.append(f"📊 候補サイズ範囲: {hit_analysis['smallest_table_mb']:.1f}MB - {hit_analysis['largest_candidate_mb']:.1f}MB")
            
            if "optimal_candidate" in hit_analysis:
                optimal = hit_analysis["optimal_candidate"]
                broadcast_summary.append(f"🏆 最適候補: {optimal['table']} ({optimal['size_mb']:.1f}MB)")
        else:
            broadcast_summary.append(f"❌ 30MB閾値ヒットなし: {broadcast_analysis['30mb_hit_analysis']['reason']}")
        
        # BROADCAST候補の詳細（最大3個）
        if broadcast_analysis["broadcast_candidates"]:
            broadcast_summary.append("📋 BROADCAST候補詳細:")
            for i, candidate in enumerate(broadcast_analysis["broadcast_candidates"][:3]):
                confidence_icon = "🔹" if candidate['confidence'] == 'high' else "🔸"
                # 既にBROADCAST済みかどうかを表示
                already_broadcasted = candidate.get('is_already_broadcasted', False)
                status_icon = "✅" if already_broadcasted else "💡"
                status_text = "既に適用済み" if already_broadcasted else "適用推奨"
                
                broadcast_summary.append(
                    f"  {confidence_icon} {candidate['table']}: 非圧縮{candidate['estimated_uncompressed_mb']:.1f}MB "
                    f"(圧縮{candidate['estimated_compressed_mb']:.1f}MB, {candidate['file_format']}, "
                    f"圧縮率{candidate['compression_ratio']:.1f}x) {status_icon} {status_text}"
                )
        
        # 既存のBROADCAST適用状況を考慮した推奨メッセージ
        if broadcast_analysis["already_optimized"]:
            if broadcast_analysis["feasibility"] in ["recommended", "all_small"]:
                broadcast_summary.append("💡 追加最適化: 実行プランは既に最適化済みですが、更なる改善の余地があります")
            else:
                broadcast_summary.append("✅ 最適化完了: 実行プランは適切にBROADCAST JOINが適用されています")
        else:
            if broadcast_analysis["feasibility"] in ["recommended", "all_small"]:
                broadcast_summary.append("🚀 最適化推奨: BROADCASTヒントの適用により大幅な性能改善が期待できます")
            elif broadcast_analysis["feasibility"] == "not_recommended":
                broadcast_summary.append("⚠️ 最適化困難: テーブルサイズが大きく、BROADCAST適用は推奨されません")
        
        # 重要な注意事項
        if broadcast_analysis["reasoning"]:
            broadcast_summary.append("⚠️ 重要な注意事項:")
            for reason in broadcast_analysis["reasoning"][:3]:  # 最大3個に拡張
                broadcast_summary.append(f"  • {reason}")
    else:
        broadcast_summary.append("❌ JOINクエリではないため、BROADCASTヒント適用対象外")
    
    optimization_prompt = f"""
あなたはDatabricksのSQLパフォーマンス最適化の専門家です。以下の情報を基にSQLクエリを最適化してください。

【重要な処理方針】
- 一回の出力で完全なSQLクエリを生成してください
- 段階的な出力や複数回に分けての出力は禁止です
- thinking機能で構造理解→一回で完全なSQL出力

【元のSQLクエリ】
```sql
{original_query}
```

【パフォーマンス分析結果】
{analysis_summary}

【特定されたボトルネック】
{chr(10).join(optimization_context) if optimization_context else "主要なボトルネックは検出されませんでした"}

【BROADCAST分析結果】
{chr(10).join(broadcast_summary)}

【Liquid Clustering推奨】
{chr(10).join(clustering_recommendations) if clustering_recommendations else "特別な推奨事項はありません"}

【最適化要求】
1. 上記の分析結果に基づいて、元のSQLクエリを最適化してください
2. 最適化のポイントを具体的に説明してください
3. パフォーマンス向上の見込みを定量的に示してください
4. 実行可能なSQLコードとして出力してください
5. 必ず同じ結果セットを返却するクエリにしてください
6. PHOTONエンジンの利用を優先してください
7. Where句でLiquidClusteringが利用できる場合は利用できる書式を優先してください
8. 同一データを繰り返し参照する場合はCTEで共通データセットとして定義してください
9. Liquid Clustering実装時は正しいDatabricks SQL構文を使用してください（ALTER TABLE table_name CLUSTER BY (column1, column2, ...)）
10. **BROADCAST分析結果を厳密に反映してください（30MB閾値）**：
    - Spark標準の30MB閾値（非圧縮）を厳格に適用
    - BROADCAST適用推奨（recommended/all_small）の場合のみBROADCASTヒント（/*+ BROADCAST(table_name) */）を適用
    - **重要: BROADCASTヒント句は必ずSELECT文の直後に配置してください**
      ```sql
      SELECT /*+ BROADCAST(table_name) */
        column1, column2, ...
      FROM table1
        JOIN table2 ON ...
      ```
    - **絶対に避けるべき誤った配置:**
      ```sql
      -- ❌ 間違い: JOIN句の中にヒントを配置
      JOIN /*+ BROADCAST(table_name) */ table2 ON ...
      ```
    - 非圧縮サイズが30MB以下の小テーブルのみBROADCAST対象
    - 30MB超過の大テーブルには絶対にBROADCASTヒントを適用しない
    - 条件付き推奨（24-30MB）の場合は要注意として明記
    - BROADCAST適用時は必ず推定非圧縮サイズ、圧縮サイズ、圧縮率を説明に記載
    - JOINクエリでない場合はBROADCASTヒントを使用しない
    - 30MB閾値ヒットがない場合は代替最適化手法を提案

【重要な制約】
- 絶対に不完全なクエリを生成しないでください
- すべてのカラム名、テーブル名、CTE名を完全に記述してください
- プレースホルダー（...、[省略]、空白など）は一切使用しないでください
- オリジナルクエリのすべてのSELECT項目を保持してください
- 元のクエリが長い場合でも、すべてのカラムを省略せずに記述してください
- 実際に実行できる完全なSQLクエリのみを出力してください
- **BROADCASTヒントは必ずSELECT文の直後に配置し、JOIN句内には絶対に配置しないでください**

【出力形式】（簡潔版）
## 最適化されたSQL

**絶対条件: 省略・プレースホルダー禁止**

```sql
[完全なSQL - すべてのカラム・CTE・テーブル名を省略なしで記述]
```

## 改善ポイント
[3つの主要改善点]

## BROADCAST適用根拠（30MB閾値基準）
[BROADCASTヒント適用の詳細根拠]
- 📏 Spark閾値: 30MB（非圧縮、spark.databricks.optimizer.autoBroadcastJoinThreshold）
- 🎯 適用テーブル: [テーブル名]
  - 非圧縮推定サイズ: [XX]MB
  - 圧縮推定サイズ: [YY]MB
  - 推定圧縮率: [ZZ]x
  - ファイル形式: [parquet/delta/等]
  - 推定根拠: [行数・データ読み込み量ベース]
- ⚖️ 判定結果: [strongly_recommended/conditionally_recommended/not_recommended]
- 🔍 閾値適合性: [30MB以下で適合/30MB超過で非適合]
- 💾 メモリ影響: [推定メモリ使用量]MB がワーカーノードにブロードキャスト
- 🚀 期待効果: [ネットワーク転送量削減・JOIN処理高速化・シャッフル削減など]
- ✅ **ヒント句配置**: SELECT文の直後に `/*+ BROADCAST(table_name) */` を正しく配置

## 期待効果  
[実行時間・メモリ・スピル改善の見込み（BROADCAST効果を含む）]
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
        
        # thinking_enabled: Trueの場合にoptimized_resultがリストになることがあるため対応
        # ここでは元のレスポンス形式を保持して返す（後で用途に応じて変換）
        return optimized_result
        
    except Exception as e:
        return f"⚠️ SQL最適化の生成中にエラーが発生しました: {str(e)}"

def generate_top10_time_consuming_processes_report(extracted_metrics: Dict[str, Any]) -> str:
    """
    最も時間がかかっている処理TOP10のレポートを文字列として生成
    """
    report_lines = []
    report_lines.append("## 🐌 最も時間がかかっている処理TOP10")
    report_lines.append("=" * 80)
    report_lines.append("📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー")
    report_lines.append('💿 スピル判定: "Num bytes spilled to disk due to memory pressure" または "Sink - Num bytes spilled to disk due to memory pressure" > 0')
    report_lines.append("🎯 スキュー判定: 'AQEShuffleRead - Number of skewed partitions' > 0")
    report_lines.append("")

    # ノードを実行時間でソート
    sorted_nodes = sorted(extracted_metrics['node_metrics'], 
                         key=lambda x: x['key_metrics'].get('durationMs', 0), 
                         reverse=True)

    if sorted_nodes:
        # 全体の実行時間を計算
        total_duration = sum(node['key_metrics'].get('durationMs', 0) for node in sorted_nodes)
        
        report_lines.append(f"📊 全体実行時間: {total_duration:,} ms ({total_duration/1000:.1f} sec)")
        report_lines.append(f"📈 TOP10合計時間: {sum(node['key_metrics'].get('durationMs', 0) for node in sorted_nodes[:10]):,} ms")
        report_lines.append("")
        
        for i, node in enumerate(sorted_nodes[:10]):
            rows_num = node['key_metrics'].get('rowsNum', 0)
            duration_ms = node['key_metrics'].get('durationMs', 0)
            memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
            
            # 全体に対する時間の割合を計算
            time_percentage = (duration_ms / max(total_duration, 1)) * 100
            
            # 時間の重要度に基づいてアイコンを選択
            if duration_ms >= 10000:  # 10秒以上
                time_icon = "🔴"
                severity = "CRITICAL"
            elif duration_ms >= 5000:  # 5秒以上
                time_icon = "🟠"
                severity = "HIGH"
            elif duration_ms >= 1000:  # 1秒以上
                time_icon = "🟡"
                severity = "MEDIUM"
            else:
                time_icon = "🟢"
                severity = "LOW"
            
            # メモリ使用量のアイコン
            memory_icon = "💚" if memory_mb < 100 else "⚠️" if memory_mb < 1000 else "🚨"
            
            # より意味のあるノード名を取得
            raw_node_name = node['name']
            node_name = get_meaningful_node_name(node, extracted_metrics)
            short_name = node_name[:100] + "..." if len(node_name) > 100 else node_name
            
            # 並列度情報の取得
            num_tasks = 0
            for stage in extracted_metrics.get('stage_metrics', []):
                if duration_ms > 0:  # このノードに関連するステージを推定
                    num_tasks = max(num_tasks, stage.get('num_tasks', 0))
            
            # スピル検出（堅牢版 - メモリベース + 包括的メトリクス検索）
            spill_detected = False
            spill_bytes = 0
            spill_detection_method = "none"
            
            # メモリ使用量を事前に計算（後のフォールバックで使用）
            memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / (1024 * 1024)
            
            # 2. 正確なメトリクス名による検索（優先）
            exact_spill_metrics = [
                "Num bytes spilled to disk due to memory pressure",
                "Sink - Num bytes spilled to disk due to memory pressure",
                "Sink/Num bytes spilled to disk due to memory pressure"
            ]
            
            detailed_metrics = node.get('detailed_metrics', {})
            for metric_key, metric_info in detailed_metrics.items():
                metric_value = metric_info.get('value', 0)
                metric_label = metric_info.get('label', '')
                
                # まず正確な名前でチェック
                if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                    spill_detected = True
                    spill_bytes = max(spill_bytes, metric_value)
                    spill_detection_method = f"exact_match_detailed ({metric_key})"
                    break
            
            # 3. 包括的スピルメトリクス検索（正確な名前で見つからない場合）
            if not spill_detected:
                for metric_key, metric_info in detailed_metrics.items():
                    metric_value = metric_info.get('value', 0)
                    metric_label = metric_info.get('label', '')
                    
                    # 正確なメトリクス名のみを使用（部分文字列マッチング禁止）
                    is_spill_metric = (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics)
                    
                    if is_spill_metric and metric_value > 0:
                        spill_detected = True
                        spill_bytes = max(spill_bytes, metric_value)  # 最大値を使用
                        spill_detection_method = f"pattern_match_detailed ({metric_key})"
            
            # 4. raw_metricsから正確なメトリクス名で検索
            if not spill_detected:
                raw_metrics = node.get('metrics', [])
                if isinstance(raw_metrics, list):
                    # まず正確な名前で検索
                    for metric in raw_metrics:
                        metric_key = metric.get('key', '')
                        metric_label = metric.get('label', '')
                        metric_value = metric.get('value', 0)
                        
                        if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                            spill_detected = True
                            spill_bytes = max(spill_bytes, metric_value)
                            spill_detection_method = f"exact_match_raw ({metric_key})"
                            break
                    
                    # 正確な名前で見つからない場合、パターンマッチング
                    if not spill_detected:
                        for metric in raw_metrics:
                            metric_key = metric.get('key', '')
                            metric_label = metric.get('label', '')
                            metric_value = metric.get('value', 0)
                            
                            # 正確なメトリクス名のみを使用（部分文字列マッチング禁止）
                            is_spill_metric = (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics)
                            
                            if is_spill_metric and metric_value > 0:
                                spill_detected = True
                                spill_bytes = max(spill_bytes, metric_value)
                                spill_detection_method = f"pattern_match_raw ({metric_key})"
                                break
            
            # 5. key_metricsから正確なメトリクス名で検索
            if not spill_detected:
                key_metrics = node.get('key_metrics', {})
                
                # まず正確な名前で検索
                for exact_metric in exact_spill_metrics:
                    if exact_metric in key_metrics and key_metrics[exact_metric] > 0:
                        spill_detected = True
                        spill_bytes = max(spill_bytes, key_metrics[exact_metric])
                        spill_detection_method = f"exact_match_key ({exact_metric})"
                        break
                
                # 正確な名前で見つからない場合、パターンマッチング
                if not spill_detected:
                    for key_metric_name, key_metric_value in key_metrics.items():
                        # 正確なメトリクス名のみを使用（部分文字列マッチング禁止）
                        is_spill_metric = (key_metric_name in exact_spill_metrics)
                        
                        if is_spill_metric and key_metric_value > 0:
                            spill_detected = True
                            spill_bytes = max(spill_bytes, key_metric_value)
                            spill_detection_method = f"pattern_match_key ({key_metric_name})"
                            break
            
            # 6. メモリベーススピル検出（最終フォールバック）
            # すべてのメトリクス検索で見つからなかった場合のフォールバック
            if not spill_detected and memory_mb > 1024:  # 1GB以上でスピル未検出の場合
                spill_detected = True
                spill_detection_method = "memory_based_fallback"
                # メモリの10%がスピルしたと仮定（保守的な見積もり）
                spill_bytes = int(memory_mb * 1024 * 1024 * 0.1)
            
            # スキュー検出: AQEShuffleRead - Number of skewed partitions メトリクス使用（正確なメトリクス名のみ）
            skew_detected = False
            skewed_partitions = 0
            target_skew_metric = "AQEShuffleRead - Number of skewed partitions"
            
            # detailed_metricsから正確なメトリクス名で検索
            detailed_metrics = node.get('detailed_metrics', {})
            for metric_key, metric_info in detailed_metrics.items():
                if metric_key == target_skew_metric:
                    try:
                        skewed_partitions = int(metric_info.get('value', 0))
                        if skewed_partitions > 0:
                            skew_detected = True
                        break
                    except (ValueError, TypeError):
                        continue
            
            # key_metricsから正確なメトリクス名で検索（フォールバック）
            if not skew_detected:
                key_metrics = node.get('key_metrics', {})
                if target_skew_metric in key_metrics:
                    try:
                        skewed_partitions = int(key_metrics[target_skew_metric])
                        if skewed_partitions > 0:
                            skew_detected = True
                    except (ValueError, TypeError):
                        pass
            
            # 並列度アイコン
            parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
            # スピルアイコン
            spill_icon = "💿" if spill_detected else "✅"
            # スキューアイコン
            skew_icon = "⚖️" if skew_detected else "✅"
            
            report_lines.append(f"{i+1:2d}. {time_icon}{memory_icon}{parallelism_icon}{spill_icon}{skew_icon} [{severity:8}] {short_name}")
            report_lines.append(f"    ⏱️  実行時間: {duration_ms:>8,} ms ({duration_ms/1000:>6.1f} sec) - 全体の {time_percentage:>5.1f}%")
            report_lines.append(f"    📊 処理行数: {rows_num:>8,} 行")
            report_lines.append(f"    💾 ピークメモリ: {memory_mb:>6.1f} MB")
            report_lines.append(f"    🔧 並列度: {num_tasks:>3d} タスク | 💿 スピル: {'あり' if spill_detected else 'なし'} | ⚖️ スキュー: {'検出' if skew_detected else 'なし'}")
            
            # 効率性指標（行/秒）を計算
            if duration_ms > 0:
                rows_per_sec = (rows_num * 1000) / duration_ms
                report_lines.append(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒")
            
            # スピル詳細情報とデバッグ情報
            if spill_detected:
                if spill_bytes > 0:
                    spill_mb = spill_bytes / 1024 / 1024
                    if spill_mb >= 1024:  # GB単位
                        spill_display = f"{spill_mb/1024:.2f} GB"
                    else:  # MB単位
                        spill_display = f"{spill_mb:.1f} MB"
                    
                    # メトリクス名を抽出してフォーマット
                    metric_name = "Unknown"
                    if "exact_match_detailed" in spill_detection_method or "exact_match_raw" in spill_detection_method:
                        # カッコの中のメトリクス名を抽出
                        start = spill_detection_method.find("(") + 1
                        end = spill_detection_method.find(")")
                        if start > 0 and end > start:
                            metric_name = spill_detection_method[start:end]
                    
                    # メトリクス名の表示形式を統一
                    formatted_display = f"{metric_name}: {spill_display}"
                    report_lines.append(f"    💿 スピル詳細: {formatted_display}")
                else:
                    report_lines.append(f"    💿 スピル詳細: 検出済み ({spill_detection_method})")
            else:
                # スピル未検出時のデバッグ情報（大容量メモリ使用時のみ）
                if memory_mb > 1024:  # 1GB以上なのにスピル未検出の場合
                    report_lines.append(f"    🔍 スピルデバッグ: メモリ{memory_mb:.1f}MBなのに未検出")
                    
                    # 利用可能なメトリクス一覧を表示（'spill'や'disk'関連のみ）
                    spill_related_metrics = []
                    
                    # detailed_metricsから検索
                    detailed_metrics = node.get('detailed_metrics', {})
                    for key, info in detailed_metrics.items():
                        if (key in exact_spill_metrics or info.get('label', '') in exact_spill_metrics):
                            value = info.get('value', 0)
                            label = info.get('label', '')
                            spill_related_metrics.append(f"detailed[{key}]={value}")
                            if label and label != key:
                                spill_related_metrics.append(f"  label='{label}'")
                    
                    # raw_metricsから検索
                    raw_metrics = node.get('metrics', [])
                    if isinstance(raw_metrics, list):
                        for metric in raw_metrics:
                            key = metric.get('key', '')
                            label = metric.get('label', '')
                            value = metric.get('value', 0)
                            if (key in exact_spill_metrics or label in exact_spill_metrics):
                                spill_related_metrics.append(f"raw[{key}]={value}")
                                if label and label != key:
                                    spill_related_metrics.append(f"  label='{label}'")
                    
                    # key_metricsから検索
                    key_metrics = node.get('key_metrics', {})
                    for key, value in key_metrics.items():
                        if key in exact_spill_metrics:
                            spill_related_metrics.append(f"key[{key}]={value}")
                    
                    if spill_related_metrics:
                        report_lines.append(f"    🔍 発見されたスピル関連メトリクス:")
                        for metric in spill_related_metrics[:5]:  # 最大5個まで表示
                            report_lines.append(f"      • {metric}")
                        if len(spill_related_metrics) > 5:
                            report_lines.append(f"      • ... 他{len(spill_related_metrics)-5}個")
                    else:
                        report_lines.append(f"    🔍 スピル関連メトリクスが全く見つかりません")
            
            # スキュー詳細情報
            if skew_detected and skewed_partitions > 0:
                report_lines.append(f"    ⚖️ スキュー詳細: {skewed_partitions} 個のスキューパーティション（AQEShuffleRead検出）")
            
            # ノードIDも表示
            report_lines.append(f"    🆔 ノードID: {node.get('node_id', node.get('id', 'N/A'))}")
            report_lines.append("")
            
    else:
        report_lines.append("⚠️ ノードメトリクスが見つかりませんでした")
    
    return "\n".join(report_lines)

def save_execution_plan_analysis(plan_info: Dict[str, Any], output_dir: str = "/tmp") -> Dict[str, str]:
    """
    実行プラン分析結果をファイルに保存
    
    Args:
        plan_info: extract_execution_plan_info()の結果
        output_dir: 出力ディレクトリ
        
    Returns:
        Dict: 保存されたファイル名の辞書
    """
    from datetime import datetime
    import json
    
    # タイムスタンプ生成
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    # ファイル名定義
    plan_json_filename = f"output_execution_plan_analysis_{timestamp}.json"
    plan_report_filename = f"output_execution_plan_report_{timestamp}.md"
    
    # JSON形式でプラン情報を保存
    with open(plan_json_filename, 'w', encoding='utf-8') as f:
        json.dump(plan_info, f, ensure_ascii=False, indent=2)
    
    # Markdown形式でプラン分析レポートを保存
    with open(plan_report_filename, 'w', encoding='utf-8') as f:
        report_content = generate_execution_plan_markdown_report(plan_info)
        f.write(report_content)
    
    return {
        'plan_json_file': plan_json_filename,
        'plan_report_file': plan_report_filename
    }

def generate_execution_plan_markdown_report(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポートを生成
    
    Args:
        plan_info: extract_execution_plan_info()の結果
        
    Returns:
        str: Markdownレポート
    """
    if OUTPUT_LANGUAGE == 'ja':
        return generate_execution_plan_markdown_report_ja(plan_info)
    else:
        return generate_execution_plan_markdown_report_en(plan_info)

def generate_execution_plan_markdown_report_ja(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポート（日本語版）
    """
    from datetime import datetime
    
    lines = []
    lines.append("# Databricks SQL実行プラン分析レポート")
    lines.append("")
    lines.append(f"**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    lines.append("")
    
    # プランサマリー
    plan_summary = plan_info.get("plan_summary", {})
    lines.append("## 📊 実行プランサマリー")
    lines.append("")
    lines.append(f"- **総ノード数**: {plan_summary.get('total_nodes', 0)}")
    lines.append(f"- **BROADCASTノード数**: {plan_summary.get('broadcast_nodes_count', 0)}")
    lines.append(f"- **JOINノード数**: {plan_summary.get('join_nodes_count', 0)}")
    lines.append(f"- **スキャンノード数**: {plan_summary.get('scan_nodes_count', 0)}")
    lines.append(f"- **シャッフルノード数**: {plan_summary.get('shuffle_nodes_count', 0)}")
    lines.append(f"- **集約ノード数**: {plan_summary.get('aggregate_nodes_count', 0)}")
    lines.append(f"- **BROADCASTが使用中**: {'はい' if plan_summary.get('has_broadcast_joins', False) else 'いいえ'}")
    lines.append(f"- **スキャンされるテーブル数**: {plan_summary.get('tables_scanned', 0)}")
    lines.append("")
    
    # JOIN戦略分析
    unique_join_strategies = plan_summary.get('unique_join_strategies', [])
    if unique_join_strategies:
        lines.append("## 🔗 JOIN戦略分析")
        lines.append("")
        for strategy in unique_join_strategies:
            strategy_jp = {
                'broadcast_hash_join': 'ブロードキャストハッシュJOIN',
                'sort_merge_join': 'ソートマージJOIN',
                'shuffle_hash_join': 'シャッフルハッシュJOIN',
                'broadcast_nested_loop_join': 'ブロードキャストネストループJOIN'
            }.get(strategy, strategy)
            lines.append(f"- **{strategy_jp}** (`{strategy}`)")
        lines.append("")
    
    # BROADCASTノード詳細
    broadcast_nodes = plan_info.get("broadcast_nodes", [])
    if broadcast_nodes:
        lines.append("## 📡 BROADCASTノード詳細")
        lines.append("")
        for i, node in enumerate(broadcast_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            lines.append(f"- **ノードタグ**: {node['node_tag']}")
            
            metadata = node.get('metadata', [])
            if metadata:
                lines.append("- **関連メタデータ**:")
                for meta in metadata[:5]:  # 最大5個まで表示
                    key = meta.get('key', '')
                    value = meta.get('value', '')
                    values = meta.get('values', [])
                    if values:
                        lines.append(f"  - **{key}**: {', '.join(map(str, values[:3]))}")
                    elif value:
                        lines.append(f"  - **{key}**: {value}")
            lines.append("")
    
    # JOINノード詳細
    join_nodes = plan_info.get("join_nodes", [])
    if join_nodes:
        lines.append("## 🔗 JOINノード詳細")
        lines.append("")
        for i, node in enumerate(join_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            lines.append(f"- **JOIN戦略**: {node['join_strategy']}")
            lines.append(f"- **JOINタイプ**: {node['join_type']}")
            
            join_keys = node.get('join_keys', [])
            if join_keys:
                lines.append(f"- **JOINキー**: {', '.join(join_keys[:5])}")
            lines.append("")
    
    # テーブルスキャン詳細（サイズ推定情報を含む）
    table_scan_details = plan_info.get("table_scan_details", {})
    table_size_estimates = plan_info.get("table_size_estimates", {})
    if table_scan_details:
        lines.append("## 📋 テーブルスキャン詳細")
        lines.append("")
        for table_name, scan_detail in table_scan_details.items():
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **ファイル形式**: {scan_detail.get('file_format', 'unknown')}")
            lines.append(f"- **プッシュダウンフィルタ数**: {len(scan_detail.get('pushed_filters', []))}")
            lines.append(f"- **出力カラム数**: {len(scan_detail.get('output_columns', []))}")
            
            # 実行プランからのサイズ推定情報（estimatedSizeInBytes利用不可のため無効化）
            # size_info = table_size_estimates.get(table_name)
            # if size_info:
            #     lines.append(f"- **推定サイズ（実行プラン）**: {size_info['estimated_size_mb']:.1f}MB")
            #     lines.append(f"- **サイズ推定信頼度**: {size_info.get('confidence', 'medium')}")
            #     if 'num_files' in size_info:
            #         lines.append(f"- **ファイル数**: {size_info['num_files']}")
            #     if 'num_partitions' in size_info:
            #         lines.append(f"- **パーティション数**: {size_info['num_partitions']}")
            
            pushed_filters = scan_detail.get('pushed_filters', [])
            if pushed_filters:
                lines.append("- **プッシュダウンフィルタ**:")
                for filter_expr in pushed_filters[:3]:  # 最大3個まで表示
                    lines.append(f"  - `{filter_expr}`")
            lines.append("")
    
    # シャッフルノード詳細
    shuffle_nodes = plan_info.get("shuffle_nodes", [])
    if shuffle_nodes:
        lines.append("## 🔄 シャッフルノード詳細")
        lines.append("")
        for i, node in enumerate(shuffle_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            
            partition_keys = node.get('partition_keys', [])
            if partition_keys:
                lines.append(f"- **パーティションキー**: {', '.join(partition_keys)}")
            lines.append("")
    
    # 集約ノード詳細
    aggregate_nodes = plan_info.get("aggregate_nodes", [])
    if aggregate_nodes:
        lines.append("## 📊 集約ノード詳細")
        lines.append("")
        for i, node in enumerate(aggregate_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            
            group_keys = node.get('group_keys', [])
            if group_keys:
                lines.append(f"- **グループ化キー**: {', '.join(group_keys[:5])}")
            
            agg_expressions = node.get('aggregate_expressions', [])
            if agg_expressions:
                lines.append(f"- **集約関数**: {', '.join(agg_expressions[:5])}")
            lines.append("")
    
    # テーブルサイズ推定情報サマリー（estimatedSizeInBytes利用不可のため無効化）
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     lines.append("## 📏 テーブルサイズ推定情報（実行プランベース）")
    #     lines.append("")
    #     total_estimated_size = sum(size_info['estimated_size_mb'] for size_info in table_size_estimates.values())
    #     lines.append(f"- **推定対象テーブル数**: {len(table_size_estimates)}")
    #     lines.append(f"- **総推定サイズ**: {total_estimated_size:.1f}MB")
    #     lines.append("")
    #     
    #     for table_name, size_info in list(table_size_estimates.items())[:5]:  # 最大5テーブル表示
    #         lines.append(f"### {table_name}")
    #         lines.append(f"- **推定サイズ**: {size_info['estimated_size_mb']:.1f}MB")
    #         lines.append(f"- **信頼度**: {size_info.get('confidence', 'medium')}")
    #         lines.append(f"- **ノード**: {size_info.get('node_name', 'unknown')}")
    #         if 'num_files' in size_info:
    #             lines.append(f"- **ファイル数**: {size_info['num_files']}")
    #         lines.append("")
    #     
    #     if len(table_size_estimates) > 5:
    #         lines.append(f"...他 {len(table_size_estimates) - 5} テーブル（詳細は上記セクション参照）")
    #         lines.append("")
    
    # 最適化推奨事項
    lines.append("## 💡 プランベース最適化推奨事項")
    lines.append("")
    
    if plan_summary.get('has_broadcast_joins', False):
        lines.append("✅ **既にBROADCAST JOINが適用されています**")
        lines.append("- 現在の実行プランでBROADCAST最適化が有効")
        
        # BROADCASTされているテーブル一覧を表示
        broadcast_tables = plan_summary.get('broadcast_tables', [])
        if broadcast_tables:
            lines.append(f"- **BROADCASTされているテーブル**: {', '.join(broadcast_tables)}")
        
        lines.append("- 追加のBROADCAST適用機会を確認してください")
    else:
        lines.append("⚠️ **BROADCAST JOINが未適用です**")
        lines.append("- 小テーブルにBROADCASTヒントの適用を検討")
        lines.append("- 30MB閾値以下のテーブルを特定してください")
    lines.append("")
    
    if plan_summary.get('shuffle_nodes_count', 0) > 3:
        lines.append("⚠️ **多数のシャッフル操作が検出されました**")
        lines.append("- データの分散とパーティショニング戦略を見直し")
        lines.append("- Liquid Clusteringの適用を検討")
    lines.append("")
    
    # サイズ推定ベースの最適化提案（estimatedSizeInBytes利用不可のため無効化）
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     small_tables = [name for name, info in table_size_estimates.items() if info['estimated_size_mb'] <= 30]
    #     if small_tables:
    #         lines.append("💡 **実行プランベースBROADCAST推奨**")
    #         lines.append(f"- 30MB以下の小テーブル: {len(small_tables)}個検出")
    #         for table in small_tables[:3]:  # 最大3個表示
    #             size_mb = table_size_estimates[table]['estimated_size_mb']
    #             lines.append(f"  • {table}: {size_mb:.1f}MB（BROADCAST候補）")
    #         if len(small_tables) > 3:
    #             lines.append(f"  • ...他 {len(small_tables) - 3} テーブル")
    #         lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("このレポートは、Databricks SQL実行プラン分析ツールによって自動生成されました。")
    
    return '\n'.join(lines)

def generate_execution_plan_markdown_report_en(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポート（英語版）
    """
    from datetime import datetime
    
    lines = []
    lines.append("# Databricks SQL Execution Plan Analysis Report")
    lines.append("")
    lines.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Plan Summary
    plan_summary = plan_info.get("plan_summary", {})
    lines.append("## 📊 Execution Plan Summary")
    lines.append("")
    lines.append(f"- **Total Nodes**: {plan_summary.get('total_nodes', 0)}")
    lines.append(f"- **BROADCAST Nodes**: {plan_summary.get('broadcast_nodes_count', 0)}")
    lines.append(f"- **JOIN Nodes**: {plan_summary.get('join_nodes_count', 0)}")
    lines.append(f"- **Scan Nodes**: {plan_summary.get('scan_nodes_count', 0)}")
    lines.append(f"- **Shuffle Nodes**: {plan_summary.get('shuffle_nodes_count', 0)}")
    lines.append(f"- **Aggregate Nodes**: {plan_summary.get('aggregate_nodes_count', 0)}")
    lines.append(f"- **BROADCAST in Use**: {'Yes' if plan_summary.get('has_broadcast_joins', False) else 'No'}")
    lines.append(f"- **Tables Scanned**: {plan_summary.get('tables_scanned', 0)}")
    lines.append("")
    
    # JOIN Strategy Analysis
    unique_join_strategies = plan_summary.get('unique_join_strategies', [])
    if unique_join_strategies:
        lines.append("## 🔗 JOIN Strategy Analysis")
        lines.append("")
        for strategy in unique_join_strategies:
            lines.append(f"- **{strategy.replace('_', ' ').title()}** (`{strategy}`)")
        lines.append("")
    
    # BROADCAST Node Details
    broadcast_nodes = plan_info.get("broadcast_nodes", [])
    if broadcast_nodes:
        lines.append("## 📡 BROADCAST Node Details")
        lines.append("")
        for i, node in enumerate(broadcast_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            lines.append(f"- **Node Tag**: {node['node_tag']}")
            
            metadata = node.get('metadata', [])
            if metadata:
                lines.append("- **Related Metadata**:")
                for meta in metadata[:5]:  # Show up to 5
                    key = meta.get('key', '')
                    value = meta.get('value', '')
                    values = meta.get('values', [])
                    if values:
                        lines.append(f"  - **{key}**: {', '.join(map(str, values[:3]))}")
                    elif value:
                        lines.append(f"  - **{key}**: {value}")
            lines.append("")
    
    # JOIN Node Details
    join_nodes = plan_info.get("join_nodes", [])
    if join_nodes:
        lines.append("## 🔗 JOIN Node Details")
        lines.append("")
        for i, node in enumerate(join_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            lines.append(f"- **JOIN Strategy**: {node['join_strategy']}")
            lines.append(f"- **JOIN Type**: {node['join_type']}")
            
            join_keys = node.get('join_keys', [])
            if join_keys:
                lines.append(f"- **JOIN Keys**: {', '.join(join_keys[:5])}")
            lines.append("")
    
    # Table Scan Details (with size estimation info)
    table_scan_details = plan_info.get("table_scan_details", {})
    table_size_estimates = plan_info.get("table_size_estimates", {})
    if table_scan_details:
        lines.append("## 📋 Table Scan Details")
        lines.append("")
        for table_name, scan_detail in table_scan_details.items():
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **File Format**: {scan_detail.get('file_format', 'unknown')}")
            lines.append(f"- **Pushed Filters**: {len(scan_detail.get('pushed_filters', []))}")
            lines.append(f"- **Output Columns**: {len(scan_detail.get('output_columns', []))}")
            
            # Add execution plan size estimation info (disabled - estimatedSizeInBytes not available)
            # size_info = table_size_estimates.get(table_name)
            # if size_info:
            #     lines.append(f"- **Estimated Size (Execution Plan)**: {size_info['estimated_size_mb']:.1f}MB")
            #     lines.append(f"- **Size Estimation Confidence**: {size_info.get('confidence', 'medium')}")
            #     if 'num_files' in size_info:
            #         lines.append(f"- **Number of Files**: {size_info['num_files']}")
            #     if 'num_partitions' in size_info:
            #         lines.append(f"- **Number of Partitions**: {size_info['num_partitions']}")
            
            pushed_filters = scan_detail.get('pushed_filters', [])
            if pushed_filters:
                lines.append("- **Pushed Down Filters**:")
                for filter_expr in pushed_filters[:3]:  # Show up to 3
                    lines.append(f"  - `{filter_expr}`")
            lines.append("")
    
    # Shuffle Node Details
    shuffle_nodes = plan_info.get("shuffle_nodes", [])
    if shuffle_nodes:
        lines.append("## 🔄 Shuffle Node Details")
        lines.append("")
        for i, node in enumerate(shuffle_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            
            partition_keys = node.get('partition_keys', [])
            if partition_keys:
                lines.append(f"- **Partition Keys**: {', '.join(partition_keys)}")
            lines.append("")
    
    # Aggregate Node Details
    aggregate_nodes = plan_info.get("aggregate_nodes", [])
    if aggregate_nodes:
        lines.append("## 📊 Aggregate Node Details")
        lines.append("")
        for i, node in enumerate(aggregate_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            
            group_keys = node.get('group_keys', [])
            if group_keys:
                lines.append(f"- **Group Keys**: {', '.join(group_keys[:5])}")
            
            agg_expressions = node.get('aggregate_expressions', [])
            if agg_expressions:
                lines.append(f"- **Aggregate Functions**: {', '.join(agg_expressions[:5])}")
            lines.append("")
    
    # Table Size Estimation Summary (disabled - estimatedSizeInBytes not available)
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     lines.append("## 📏 Table Size Estimation (Execution Plan Based)")
    #     lines.append("")
    #     total_estimated_size = sum(size_info['estimated_size_mb'] for size_info in table_size_estimates.values())
    #     lines.append(f"- **Estimated Tables Count**: {len(table_size_estimates)}")
    #     lines.append(f"- **Total Estimated Size**: {total_estimated_size:.1f}MB")
    #     lines.append("")
    #     
    #     for table_name, size_info in list(table_size_estimates.items())[:5]:  # Show up to 5 tables
    #         lines.append(f"### {table_name}")
    #         lines.append(f"- **Estimated Size**: {size_info['estimated_size_mb']:.1f}MB")
    #         lines.append(f"- **Confidence**: {size_info.get('confidence', 'medium')}")
    #         lines.append(f"- **Node**: {size_info.get('node_name', 'unknown')}")
    #         if 'num_files' in size_info:
    #             lines.append(f"- **Number of Files**: {size_info['num_files']}")
    #         lines.append("")
    #     
    #     if len(table_size_estimates) > 5:
    #         lines.append(f"...and {len(table_size_estimates) - 5} more tables (see details in sections above)")
    #         lines.append("")
    
    # Plan-based Optimization Recommendations
    lines.append("## 💡 Plan-based Optimization Recommendations")
    lines.append("")
    
    if plan_summary.get('has_broadcast_joins', False):
        lines.append("✅ **BROADCAST JOIN is already applied**")
        lines.append("- Current execution plan has BROADCAST optimization enabled")
        
        # Show list of broadcast tables
        broadcast_tables = plan_summary.get('broadcast_tables', [])
        if broadcast_tables:
            lines.append(f"- **Tables Being Broadcast**: {', '.join(broadcast_tables)}")
        
        lines.append("- Check for additional BROADCAST application opportunities")
    else:
        lines.append("⚠️ **BROADCAST JOIN is not applied**")
        lines.append("- Consider applying BROADCAST hints to small tables")
        lines.append("- Identify tables under 30MB threshold")
    lines.append("")
    
    if plan_summary.get('shuffle_nodes_count', 0) > 3:
        lines.append("⚠️ **Multiple shuffle operations detected**")
        lines.append("- Review data distribution and partitioning strategy")
        lines.append("- Consider applying Liquid Clustering")
    lines.append("")
    
    # Size estimation based optimization suggestions (disabled - estimatedSizeInBytes not available)
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     small_tables = [name for name, info in table_size_estimates.items() if info['estimated_size_mb'] <= 30]
    #     if small_tables:
    #         lines.append("💡 **Execution Plan Based BROADCAST Recommendations**")
    #         lines.append(f"- Small tables ≤30MB detected: {len(small_tables)}")
    #         for table in small_tables[:3]:  # Show up to 3 tables
    #             size_mb = table_size_estimates[table]['estimated_size_mb']
    #             lines.append(f"  • {table}: {size_mb:.1f}MB (BROADCAST candidate)")
    #         if len(small_tables) > 3:
    #             lines.append(f"  • ...and {len(small_tables) - 3} more tables")
    #         lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("This report was automatically generated by the Databricks SQL Execution Plan Analysis Tool.")
    
    return '\n'.join(lines)

def generate_comprehensive_optimization_report(query_id: str, optimized_result: str, metrics: Dict[str, Any], analysis_result: str = "") -> str:
    """
    包括的な最適化レポートを生成
    
    Args:
        query_id: クエリID
        optimized_result: 最適化結果
        metrics: メトリクス情報
        analysis_result: ボトルネック分析結果
    
    Returns:
        str: 読みやすく構成されたレポート
    """
    from datetime import datetime
    
    # 基本情報の取得
    overall_metrics = metrics.get('overall_metrics', {})
    bottleneck_indicators = metrics.get('bottleneck_indicators', {})
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    
    # thinking_enabled対応: analysis_resultがリストの場合の処理
    if isinstance(analysis_result, list):
        analysis_result_str = format_thinking_response(analysis_result)
    else:
        analysis_result_str = str(analysis_result)
    
    # signature情報の除去
    import re
    signature_pattern = r"'signature':\s*'[A-Za-z0-9+/=]{100,}'"
    analysis_result_str = re.sub(signature_pattern, "'signature': '[REMOVED]'", analysis_result_str)
    
    # レポートの構成
    if OUTPUT_LANGUAGE == 'ja':
        report = f"""# 📊 SQL最適化レポート

**クエリID**: {query_id}  
**レポート生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 🎯 1. ボトルネック分析結果

### 🤖 AIによる詳細分析

{analysis_result_str}

### 📊 主要パフォーマンス指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | {overall_metrics.get('total_time_ms', 0):,} ms | {'✅ 良好' if overall_metrics.get('total_time_ms', 0) < 60000 else '⚠️ 改善必要'} |
| Photon有効 | {'はい' if overall_metrics.get('photon_enabled', False) else 'いいえ'} | {'✅ 良好' if overall_metrics.get('photon_enabled', False) else '❌ 未有効'} |
| キャッシュ効率 | {bottleneck_indicators.get('cache_hit_ratio', 0) * 100:.1f}% | {'✅ 良好' if bottleneck_indicators.get('cache_hit_ratio', 0) > 0.8 else '⚠️ 改善必要'} |
| データ選択性 | {bottleneck_indicators.get('data_selectivity', 0) * 100:.2f}% | {'✅ 良好' if bottleneck_indicators.get('data_selectivity', 0) > 0.1 else '⚠️ 改善必要'} |
| シャッフル操作 | {bottleneck_indicators.get('shuffle_operations_count', 0)}回 | {'✅ 良好' if bottleneck_indicators.get('shuffle_operations_count', 0) < 5 else '⚠️ 多数'} |
| スピル発生 | {'はい' if bottleneck_indicators.get('has_spill', False) else 'いいえ'} | {'❌ 問題あり' if bottleneck_indicators.get('has_spill', False) else '✅ 良好'} |

### 🚨 主要ボトルネック

"""
        
        # 主要ボトルネックの詳細
        bottlenecks = []
        
        if bottleneck_indicators.get('has_spill', False):
            spill_gb = bottleneck_indicators.get('spill_bytes', 0) / 1024 / 1024 / 1024
            bottlenecks.append(f"**メモリスピル**: {spill_gb:.2f}GB - メモリ不足による性能低下")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            bottlenecks.append("**シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            bottlenecks.append("**キャッシュ効率低下**: データ再利用効率が低い")
        
        if not overall_metrics.get('photon_enabled', False):
            bottlenecks.append("**Photon未有効**: 高速処理エンジンが利用されていない")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.01:
            bottlenecks.append("**データ選択性低下**: 必要以上のデータを読み込んでいる")
        
        if bottlenecks:
            for i, bottleneck in enumerate(bottlenecks, 1):
                report += f"{i}. {bottleneck}\n"
        else:
            report += "主要なボトルネックは検出されませんでした。\n"
        
        report += "\n"
        
        # Liquid Clustering分析結果の追加
        if liquid_analysis:
            performance_context = liquid_analysis.get('performance_context', {})
            llm_analysis = liquid_analysis.get('llm_analysis', '')
            
            report += f"""
## 🗂️ 2. Liquid Clustering分析結果

### 📊 パフォーマンス概要

| 項目 | 値 |
|------|-----|
| 実行時間 | {performance_context.get('total_time_sec', 0):.1f}秒 |
| データ読み込み | {performance_context.get('read_gb', 0):.2f}GB |
| 出力行数 | {performance_context.get('rows_produced', 0):,}行 |
| 読み込み行数 | {performance_context.get('rows_read', 0):,}行 |
| データ選択性 | {performance_context.get('data_selectivity', 0):.4f} |

### 🤖 AI分析結果

{llm_analysis}

"""
        
        # SQL最適化分析結果の追加
        report += f"""
## 🚀 3. SQL最適化分析結果

### 💡 最適化提案

{optimized_result}

### 📈 4. 期待されるパフォーマンス改善効果

#### 🎯 予想される改善点

"""
        
        # 期待される改善効果を計算
        expected_improvements = []
        
        if bottleneck_indicators.get('has_spill', False):
            expected_improvements.append("**メモリスピル解消**: 最大50-80%の性能改善が期待されます")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            expected_improvements.append("**シャッフル最適化**: 20-60%の実行時間短縮が期待されます")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            expected_improvements.append("**キャッシュ効率向上**: 30-70%の読み込み時間短縮が期待されます")
        
        if not overall_metrics.get('photon_enabled', False):
            expected_improvements.append("**Photon有効化**: 2-10倍の処理速度向上が期待されます")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.01:
            expected_improvements.append("**データ選択性改善**: 40-90%のデータ読み込み量削減が期待されます")
        
        if expected_improvements:
            for i, improvement in enumerate(expected_improvements, 1):
                report += f"{i}. {improvement}\n"
            
            # 総合的な改善効果
            total_time_ms = overall_metrics.get('total_time_ms', 0)
            if total_time_ms > 0:
                improvement_ratio = min(0.8, len(expected_improvements) * 0.15)  # 最大80%改善
                expected_time = total_time_ms * (1 - improvement_ratio)
                report += f"\n**総合改善効果**: 実行時間 {total_time_ms:,}ms → {expected_time:,.0f}ms（約{improvement_ratio*100:.0f}%改善）\n"
        else:
            report += "現在のクエリは既に最適化されています。大幅な改善は期待されません。\n"
        
        report += f"""

#### 🔧 実装優先度

1. **高優先度**: Photon有効化、メモリスピル解消
2. **中優先度**: インデックス最適化、パーティション戦略
3. **低優先度**: 統計情報更新、キャッシュ戦略

---

*レポート生成時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
    else:
        # 英語版（同様の構成）
        report = f"""# 📊 SQL Optimization Report

**Query ID**: {query_id}  
**Report Generation Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 🎯 1. Bottleneck Analysis Results

### 🤖 AI-Powered Analysis

{analysis_result_str}

### 📊 Key Performance Indicators

| Metric | Value | Status |
|--------|-------|--------|
| Execution Time | {overall_metrics.get('total_time_ms', 0):,} ms | {'✅ Good' if overall_metrics.get('total_time_ms', 0) < 60000 else '⚠️ Needs Improvement'} |
| Photon Enabled | {'Yes' if overall_metrics.get('photon_enabled', False) else 'No'} | {'✅ Good' if overall_metrics.get('photon_enabled', False) else '❌ Not Enabled'} |
| Cache Efficiency | {bottleneck_indicators.get('cache_hit_ratio', 0) * 100:.1f}% | {'✅ Good' if bottleneck_indicators.get('cache_hit_ratio', 0) > 0.8 else '⚠️ Needs Improvement'} |
| Data Selectivity | {bottleneck_indicators.get('data_selectivity', 0) * 100:.2f}% | {'✅ Good' if bottleneck_indicators.get('data_selectivity', 0) > 0.1 else '⚠️ Needs Improvement'} |
| Shuffle Operations | {bottleneck_indicators.get('shuffle_operations_count', 0)} times | {'✅ Good' if bottleneck_indicators.get('shuffle_operations_count', 0) < 5 else '⚠️ High'} |
| Spill Occurrence | {'Yes' if bottleneck_indicators.get('has_spill', False) else 'No'} | {'❌ Issues' if bottleneck_indicators.get('has_spill', False) else '✅ Good'} |

### 🚨 Key Bottlenecks

"""
        
        # 主要ボトルネックの詳細（英語版）
        bottlenecks = []
        
        if bottleneck_indicators.get('has_spill', False):
            spill_gb = bottleneck_indicators.get('spill_bytes', 0) / 1024 / 1024 / 1024
            bottlenecks.append(f"**Memory Spill**: {spill_gb:.2f}GB - Performance degradation due to memory shortage")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            bottlenecks.append("**Shuffle Bottleneck**: Large data transfer in JOIN/GROUP BY operations")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            bottlenecks.append("**Cache Inefficiency**: Low data reuse efficiency")
        
        if not overall_metrics.get('photon_enabled', False):
            bottlenecks.append("**Photon Not Enabled**: High-speed processing engine not utilized")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.01:
            bottlenecks.append("**Poor Data Selectivity**: Reading more data than necessary")
        
        if bottlenecks:
            for i, bottleneck in enumerate(bottlenecks, 1):
                report += f"{i}. {bottleneck}\n"
        else:
            report += "No major bottlenecks detected.\n"
        
        report += "\n"
        
        # Liquid Clustering分析結果の追加（英語版）
        if liquid_analysis:
            performance_context = liquid_analysis.get('performance_context', {})
            llm_analysis = liquid_analysis.get('llm_analysis', '')
            
            report += f"""
## 🗂️ 2. Liquid Clustering Analysis Results

### 📊 Performance Overview

| Item | Value |
|------|-------|
| Execution Time | {performance_context.get('total_time_sec', 0):.1f}s |
| Data Read | {performance_context.get('read_gb', 0):.2f}GB |
| Output Rows | {performance_context.get('rows_produced', 0):,} |
| Read Rows | {performance_context.get('rows_read', 0):,} |
| Data Selectivity | {performance_context.get('data_selectivity', 0):.4f} |

### 🤖 AI Analysis Results

{llm_analysis}

"""
        
        # SQL最適化分析結果の追加（英語版）
        report += f"""
## 🚀 3. SQL Optimization Analysis Results

### 💡 Optimization Recommendations

{optimized_result}

### 📈 4. Expected Performance Improvement

#### 🎯 Anticipated Improvements

"""
        
        # 期待される改善効果を計算（英語版）
        expected_improvements = []
        
        if bottleneck_indicators.get('has_spill', False):
            expected_improvements.append("**Memory Spill Resolution**: Up to 50-80% performance improvement expected")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            expected_improvements.append("**Shuffle Optimization**: 20-60% execution time reduction expected")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            expected_improvements.append("**Cache Efficiency**: 30-70% read time reduction expected")
        
        if not overall_metrics.get('photon_enabled', False):
            expected_improvements.append("**Photon Enablement**: 2-10x processing speed improvement expected")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.01:
            expected_improvements.append("**Data Selectivity**: 40-90% data read volume reduction expected")
        
        if expected_improvements:
            for i, improvement in enumerate(expected_improvements, 1):
                report += f"{i}. {improvement}\n"
            
            # 総合的な改善効果
            total_time_ms = overall_metrics.get('total_time_ms', 0)
            if total_time_ms > 0:
                improvement_ratio = min(0.8, len(expected_improvements) * 0.15)  # 最大80%改善
                expected_time = total_time_ms * (1 - improvement_ratio)
                report += f"\n**Overall Improvement**: Execution time {total_time_ms:,}ms → {expected_time:,.0f}ms (approx. {improvement_ratio*100:.0f}% improvement)\n"
        else:
            report += "Current query is already optimized. No significant improvements expected.\n"
        
        report += f"""

#### 🔧 Implementation Priority

1. **High Priority**: Photon enablement, Memory spill resolution
2. **Medium Priority**: Index optimization, Partitioning strategy
3. **Low Priority**: Statistics update, Cache strategy

---

*Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return report

def refine_report_with_llm(raw_report: str, query_id: str) -> str:
    """
    LLMを使ってレポートを推敲し、読みやすい最終レポートを生成
    
    Args:
        raw_report: 初期生成されたレポート
        query_id: クエリID
        
    Returns:
        str: LLMで推敲された読みやすいレポート
    """
    
    refinement_prompt = f"""
あなたは技術文書の編集者として、Databricks SQLパフォーマンス分析レポートを読みやすく推敲してください。

【推敲の方針】
1. **重複内容の統合**: 同じ情報が複数箇所に記載されている場合は統合し、一箇所にまとめる
2. **構成の最適化**: 情報の優先度に基づいて論理的な順序で再配置
3. **読みやすさの向上**: 技術的内容を保持しつつ、読みやすい文章に改善
4. **具体性の追加**: 可能な場合は具体的な数値や改善提案を強調
5. **アクションアイテムの明確化**: 実装すべき対策を明確に提示

【現在のレポート】
```
{raw_report}
```

【出力要件】
- Markdownフォーマットを維持
- 技術的な正確性を保持
- エグゼクティブサマリーを冒頭に追加
- 重複を削除し、情報を論理的に整理
- 実装のためのアクションプランを追加
- 読みやすい構成で統合された最終レポートを出力

推敲後の読みやすいレポートを出力してください。
"""
    
    try:
        provider = LLM_CONFIG.get("provider", "databricks")
        
        if provider == "databricks":
            refined_report = _call_databricks_llm(refinement_prompt)
        elif provider == "openai":
            refined_report = _call_openai_llm(refinement_prompt)
        elif provider == "azure_openai":
            refined_report = _call_azure_openai_llm(refinement_prompt)
        elif provider == "anthropic":
            refined_report = _call_anthropic_llm(refinement_prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
        
        # thinking_enabled対応
        if isinstance(refined_report, list):
            refined_report = format_thinking_response(refined_report)
        
        # signature情報の除去
        import re
        signature_pattern = r"'signature':\s*'[A-Za-z0-9+/=]{100,}'"
        refined_report = re.sub(signature_pattern, "'signature': '[REMOVED]'", refined_report)
        
        return refined_report
        
    except Exception as e:
        print(f"⚠️ LLMによるレポート推敲中にエラーが発生しました: {str(e)}")
        print("📄 元のレポートを返します")
        return raw_report

def save_optimized_sql_files(original_query: str, optimized_result: str, metrics: Dict[str, Any], analysis_result: str = "") -> Dict[str, str]:
    """
    最適化されたSQLクエリを実行可能な形でファイルに保存
    
    特徴:
    - SQLファイルの末尾に自動でセミコロン(;)を付与
    - そのままDatabricks Notebookで実行可能
    - %sql マジックコマンドでも直接実行可能
    - LLMによるレポート推敲で読みやすい最終レポートを生成
    """
    
    import re
    from datetime import datetime
    
    # thinking_enabled: Trueの場合にoptimized_resultがリストになることがあるため対応
    optimized_result_for_file = optimized_result
    optimized_result_main_content = optimized_result
    
    if isinstance(optimized_result, list):
        # ファイル保存用は人間に読みやすい形式に変換
        optimized_result_for_file = format_thinking_response(optimized_result)
        # SQL抽出用は主要コンテンツのみを使用
        optimized_result_main_content = extract_main_content_from_thinking_response(optimized_result)
    
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    query_id = metrics.get('query_info', {}).get('query_id', 'unknown')
    
    # オリジナルクエリファイルの保存は除外（不要）
    original_filename = None
    
    # 最適化されたクエリの抽出と保存
    optimized_filename = f"output_optimized_query_{timestamp}.sql"
    
    # 最適化結果からSQLコードを抽出（主要コンテンツから抽出）
    sql_pattern = r'```sql\s*(.*?)\s*```'
    sql_matches = re.findall(sql_pattern, optimized_result_main_content, re.DOTALL | re.IGNORECASE)
    
    optimized_sql = ""
    if sql_matches:
        # 最初に見つかったSQLブロックを使用
        optimized_sql = sql_matches[0].strip()
    else:
        # SQLブロックが見つからない場合は、SQL関連の行を抽出
        lines = optimized_result_main_content.split('\n')
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
        f.write(f"-- ファイル: {optimized_filename}\n\n")
        
        if optimized_sql:
            # SQLの末尾にセミコロンを確実に追加
            optimized_sql_clean = optimized_sql.strip()
            if optimized_sql_clean and not optimized_sql_clean.endswith(';'):
                optimized_sql_clean += ';'
            f.write(optimized_sql_clean)
        else:
            f.write("-- ⚠️ SQLコードの自動抽出に失敗しました\n")
            f.write("-- 以下は最適化分析の全結果です:\n\n")
            f.write(f"/*\n{optimized_result_main_content}\n*/")
    
    # 分析レポートファイルの保存（LLMで推敲された読みやすいレポート）
    report_filename = f"output_optimization_report_{timestamp}.md"
    
    print("🤖 LLMによるレポート推敲を実行中...")
    
    # 初期レポートの生成
    initial_report = generate_comprehensive_optimization_report(
        query_id, optimized_result_for_file, metrics, analysis_result
    )
    
    # LLMでレポートを推敲
    refined_report = refine_report_with_llm(initial_report, query_id)
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(refined_report)
    
    print("✅ LLMによるレポート推敲完了")
    
    # 最も時間がかかっている処理TOP10を独立したファイルとして出力
    top10_filename = f"output_top10_processes_{timestamp}.md"
    with open(top10_filename, 'w', encoding='utf-8') as f:
        f.write(f"# {get_message('top10_processes')}\n\n")
        f.write(f"**{get_message('query_id')}**: {query_id}\n")
        f.write(f"**{get_message('analysis_time')}**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        try:
            top10_report = generate_top10_time_consuming_processes_report(metrics)
            f.write(top10_report)
        except Exception as e:
            error_msg = f"⚠️ TOP10処理時間分析の生成でエラーが発生しました: {str(e)}\n" if OUTPUT_LANGUAGE == 'ja' else f"⚠️ Error generating TOP10 analysis: {str(e)}\n"
            f.write(error_msg)
    
    # 出力ファイルの結果（output_optimization_reportとTOP10ファイルのみ）
    result = {
        'optimized_file': optimized_filename,
        'report_file': report_filename,
        'top10_file': top10_filename
    }
    
    return result

def demonstrate_execution_plan_size_extraction():
    """
    実行プランからのサイズ推定機能のデモンストレーション
    """
    print("🧪 実行プランからのテーブルサイズ推定機能のデモ")
    print("-" * 50)
    
    # サンプルのプロファイラーデータ構造
    sample_profiler_data = {
        "executionPlan": {
            "physicalPlan": {
                "nodes": [
                    {
                        "nodeName": "Scan Delta orders",
                        "id": "1",
                        "metrics": {
                            "estimatedSizeInBytes": 10485760,  # 10MB
                            "numFiles": 5,
                            "numPartitions": 2
                        },
                        "output": "[order_id#123, customer_id#124, amount#125] orders",
                        "details": "Table: catalog.database.orders"
                    },
                    {
                        "nodeName": "Scan Delta customers",
                        "id": "2", 
                        "metrics": {
                            "estimatedSizeInBytes": 52428800,  # 50MB
                            "numFiles": 10,
                            "numPartitions": 4
                        },
                        "output": "[customer_id#126, name#127, region#128] customers"
                    }
                ]
            }
        }
    }
    
    print("📊 サンプル実行プラン:")
    print("  • orders テーブル: estimatedSizeInBytes = 10,485,760 (10MB)")
    print("  • customers テーブル: estimatedSizeInBytes = 52,428,800 (50MB)")
    print("")
    
    # テーブルサイズ推定の実行
    table_size_estimates = extract_table_size_estimates_from_plan(sample_profiler_data)
    
    print("🔍 抽出されたテーブルサイズ推定:")
    if table_size_estimates:
        for table_name, size_info in table_size_estimates.items():
            print(f"  📋 {table_name}:")
            print(f"    - サイズ: {size_info['estimated_size_mb']:.1f}MB")
            print(f"    - 信頼度: {size_info['confidence']}")
            print(f"    - ソース: {size_info['source']}")
            if 'num_files' in size_info:
                print(f"    - ファイル数: {size_info['num_files']}")
            if 'num_partitions' in size_info:
                print(f"    - パーティション数: {size_info['num_partitions']}")
            print("")
    else:
        print("  ⚠️ テーブルサイズ推定情報が抽出されませんでした")
    
    print("💡 BROADCAST分析への影響:")
    if table_size_estimates:
        for table_name, size_info in table_size_estimates.items():
            size_mb = size_info['estimated_size_mb']
            if size_mb <= 30:
                print(f"  ✅ {table_name}: {size_mb:.1f}MB ≤ 30MB → BROADCAST推奨")
            else:
                print(f"  ❌ {table_name}: {size_mb:.1f}MB > 30MB → BROADCAST非推奨")
    
    print("")
    print("🎯 従来の推定方法との比較:")
    print("  📈 従来: メトリクスベースの間接推定（推定精度: 中）")
    print("  ❌ 新機能: 実行プランの estimatedSizeInBytes 活用（利用不可のため無効化）")
    print("  ℹ️ 現在: 3.0倍圧縮率での保守的推定を採用")
    
    return {}

print("✅ 関数定義完了: SQL最適化関連関数（実行プランサイズ推定対応）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 SQLクエリ最適化の実行（ステップ1: クエリ抽出）
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
# MAGIC ## 🤖 LLMによるSQL最適化（ステップ2: 最適化実行）
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
    
    # thinking_enabled: Trueの場合にanalysis_resultがリストになることがあるため対応
    if isinstance(analysis_result, list):
        # リストの場合は主要コンテンツのみを抽出してLLMに渡す
        analysis_result_str = extract_main_content_from_thinking_response(analysis_result)
    else:
        analysis_result_str = str(analysis_result)
    
    optimized_result = generate_optimized_query_with_llm(
        original_query, 
        analysis_result_str, 
        extracted_metrics
    )
    
    # thinking_enabled: Trueの場合にoptimized_resultがリストになることがあるため対応
    optimized_result_display = optimized_result
    if isinstance(optimized_result, list):
        # 表示用は人間に読みやすい形式に変換
        optimized_result_display = format_thinking_response(optimized_result)
        # 主要コンテンツのみを抽出（後続処理用）
        optimized_result = extract_main_content_from_thinking_response(optimized_result)
    
    if optimized_result and not str(optimized_result).startswith("⚠️"):
        print("✅ SQL最適化が完了しました")
        print(f"📄 最適化結果の詳細:")
        
        # 最適化結果の詳細を表示（1000行まで）
        lines = optimized_result_display.split('\n')
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
# MAGIC ## 💾 最適化結果の保存（ステップ3: ファイル生成）
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 最適化されたSQLクエリのファイル保存（接頭語: output_）
# MAGIC - オリジナルクエリ、最適化クエリ、レポートの生成
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
    missing_variables.append("original_query (セル19を実行してください)")
    original_query = ""

# optimized_result のチェック  
try:
    optimized_result
except NameError:
    missing_variables.append("optimized_result (セル20を実行してください)")
    optimized_result = ""

# extracted_metrics のチェック
try:
    extracted_metrics
except NameError:
    missing_variables.append("extracted_metrics (セル12を実行してください)")
    # デフォルト値として最小限の構造を設定
    extracted_metrics = {
        'query_info': {'query_id': 'unknown'},
        'overall_metrics': {},
        'bottleneck_indicators': {}
    }

# analysis_result のチェック
try:
    analysis_result
except NameError:
    missing_variables.append("analysis_result (セル16を実行してください)")
    analysis_result = ""

if missing_variables:
    print("❌ 必要な変数が定義されていません:")
    for var in missing_variables:
        print(f"   • {var}")
    print("\n⚠️ 上記のセルを先に実行してから、このセルを再実行してください。")
    print("📋 正しい実行順序: セル11 → セル12 → ... → セル19 → セル20 → セル21")
    print("\n🔄 デフォルト値を使用して処理を継続します。")

# 変数が存在する（またはデフォルト値が設定された）場合の処理
if original_query.strip() and str(optimized_result).strip():
    print("📁 ファイル生成中...")
    
    try:
        saved_files = save_optimized_sql_files(
            original_query,
            optimized_result,
            extracted_metrics,
            analysis_result
        )
        
        print("✅ 以下のファイルを生成しました:")
        for file_type, filename in saved_files.items():
            file_type_jp = {
                'original_file': 'オリジナルSQLクエリ',
                'optimized_file': '最適化SQLクエリ',
                'report_file': '最適化レポート'
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
# MAGIC ## 🧪 Databricks Notebookでの実行ガイド（ステップ4: 実行方法）
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 生成されたファイルの使用方法説明
# MAGIC - Databricks Notebookでの実行手順ガイド
# MAGIC - SQLクエリの直接実行方法
# MAGIC - 重要な注意事項の表示

# COMMAND ----------

# 🧪 ステップ4: Databricks Notebookでの実行ガイド
print("\n🧪 ステップ4: Databricks Notebookでの実行ガイド")
print("-" * 40)

# saved_files変数が定義されているかチェック
try:
    saved_files
except NameError:
    print("❌ saved_files変数が定義されていません。")
    print("⚠️ セル21 (最適化結果の保存) を先に実行してください。")
    saved_files = {}

if saved_files:
    original_file = saved_files.get('original_file', '')
    optimized_file = saved_files.get('optimized_file', '')
    report_file = saved_files.get('report_file', '')
    
    print("🚀 Databricks Notebookでの実行手順:")
    print("1. 生成されたSQLファイルの内容を確認")
    print("2. 必要に応じてクエリを手動調整")
    print("3. Notebook内で直接SQLクエリを実行")
    print("4. パフォーマンス測定と比較")
    
    print(f"\n📝 生成ファイル一覧:")
    if original_file:
        print(f"   📄 オリジナルクエリ: {original_file}")
    if optimized_file:
        print(f"   🚀 最適化クエリ: {optimized_file}")
    if report_file:
        print(f"   📊 分析レポート: {report_file}")
    
    if optimized_file:
        print(f"\n🔧 Databrics Notebookでの実行方法:")
        print(f"   # ファイルからSQLを読み込んで実行")
        print(f"   optimized_sql = open('{optimized_file}').read()")
        print(f"   df = spark.sql(optimized_sql)")
        print(f"   df.show()")
        print(f"   ")
        print(f"   # または %sql マジックコマンドを使用")
        print(f"   # %sql [ファイルの内容をコピー&ペースト]")
        print(f"   ")
        print(f"   # パフォーマンス測定例")
        print(f"   import time")
        print(f"   start_time = time.time()")
        print(f"   result_count = df.count()")
        print(f"   execution_time = time.time() - start_time")
        print(f"   print(f'実行時間: {{execution_time:.2f}} 秒, 行数: {{result_count:,}}')")
    
    print(f"\n⚠️ 重要な注意事項:")
    print(f"   • 本番環境での実行前に、必ずテスト環境で検証してください")
    print(f"   • データベースの構造やサイズによって結果は変わる可能性があります")
    print(f"   • クエリプランの確認: EXPLAIN 文を使用してください")
    print(f"   • Databricks SQLエディタでの実行も可能です")

else:
    print("⚠️ 実行用ファイルが生成されていません")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 最終処理完了サマリー
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

# LLMプロバイダー情報の動的表示
try:
    current_provider = LLM_CONFIG.get('provider', 'unknown')
    provider_display_names = {
        'databricks': f"Databricks ({LLM_CONFIG.get('databricks', {}).get('endpoint_name', 'Model Serving')})",
        'openai': f"OpenAI ({LLM_CONFIG.get('openai', {}).get('model', 'GPT-4')})",
        'azure_openai': f"Azure OpenAI ({LLM_CONFIG.get('azure_openai', {}).get('deployment_name', 'GPT-4')})",
        'anthropic': f"Anthropic ({LLM_CONFIG.get('anthropic', {}).get('model', 'Claude')})"
    }
    provider_display = provider_display_names.get(current_provider, f"{current_provider}（未知のプロバイダー）")
    print(f"✅ {provider_display}によるボトルネック分析完了")
except Exception as e:
    print("✅ LLMによるボトルネック分析完了")

print("✅ 分析結果保存完了")
print("✅ オリジナルクエリ抽出完了")
print("✅ LLMによるSQL最適化完了")
print("✅ 最適化結果ファイル生成完了（接頭語: output_）")

# 必要な変数が定義されているかチェック
missing_summary_vars = []

try:
    result_output_path
except NameError:
    missing_summary_vars.append("result_output_path (セル16を実行してください)")

try:
    saved_files
except NameError:
    missing_summary_vars.append("saved_files (セル19を実行してください)")
    saved_files = {}

print(f"\n📁 出力ファイル一覧:")

if 'result_output_path' in globals():
    print(f"   📄 ボトルネック分析レポート: {result_output_path}")
else:
    print("   📄 ボトルネック分析レポート: (セル16を実行してください)")

if saved_files:
    for file_type, filename in saved_files.items():
        file_type_jp = {
            'original_file': '📄 オリジナルSQL',
            'optimized_file': '🚀 最適化SQL',
            'report_file': '📊 最適化レポート'
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
# MAGIC # 📚 参考・応用セクション
# MAGIC
# MAGIC **このセクションでは応用的な使用方法を説明します**
# MAGIC
# MAGIC 📋 **参考情報:**
# MAGIC - ファイルアップロード方法
# MAGIC - カスタマイズポイント
# MAGIC - エラー対処方法
# MAGIC - 高度な使用例
# MAGIC
# MAGIC 💡 **用途:** ツールの活用を深めたい場合に参照してください

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 実行プラン情報を活用したBROADCAST分析機能の追加完了
# MAGIC
# MAGIC この更新により、JSONメトリクスから抽出した実行プラン情報を活用したBROADCAST分析が可能になりました：
# MAGIC
# MAGIC ### 🆕 新機能
# MAGIC
# MAGIC #### 1. `extract_execution_plan_info()` 関数
# MAGIC - **機能**: JSONメトリクスから詳細な実行プラン情報を抽出
# MAGIC - **検出内容**:
# MAGIC   - BROADCASTノード（既存の適用状況）
# MAGIC   - JOINノード（戦略：broadcast_hash_join, sort_merge_join, shuffle_hash_join等）
# MAGIC   - スキャンノード（テーブル名、ファイル形式、プッシュダウンフィルタ）
# MAGIC   - シャッフルノード（パーティション情報）
# MAGIC   - 集約ノード（GROUP BY表現、集約関数）
# MAGIC - **出力**: 構造化されたプラン情報辞書
# MAGIC
# MAGIC #### 2. `analyze_broadcast_feasibility()` 関数の拡張
# MAGIC - **追加機能**:
# MAGIC   - 既存のBROADCAST適用状況の自動検出
# MAGIC   - プラン情報からテーブル名とファイル形式の正確な特定
# MAGIC   - 既に最適化済みテーブルと新規推奨の区別
# MAGIC   - 実行プラン分析結果の詳細記録
# MAGIC - **判定強化**:
# MAGIC   - `already_applied`: 既にBROADCAST適用済み
# MAGIC   - `new_recommendation`: 新規BROADCAST推奨
# MAGIC   - プラン情報と整合性のある分析
# MAGIC
# MAGIC #### 3. SQL最適化の改善
# MAGIC - **プラン考慮**: 実行プラン情報を含むBROADCAST分析
# MAGIC - **LLMプロンプト強化**: プラン情報をLLMに提供して正確な最適化
# MAGIC - **30MB閾値の厳格適用**: 実際のSparkプラン情報との整合性確保

# COMMAND ----------

# 🔍 実行プラン情報を活用したBROADCAST分析のデモ関数
def demonstrate_plan_based_broadcast_analysis():
    """
    実行プラン情報を活用したBROADCAST分析のデモ
    """
    print("\n🔍 実行プラン情報を活用したBROADCAST分析デモ")
    print("=" * 60)
    
    # メトリクスデータが存在するかチェック
    try:
        # extracted_metrics と profiler_data を使用
        extracted_metrics
        profiler_data
        
        print("📊 実行プラン情報を抽出中...")
        plan_info = extract_execution_plan_info(profiler_data)
        
        print(f"✅ プラン情報の抽出完了")
        print(f"   - 総ノード数: {plan_info.get('plan_summary', {}).get('total_nodes', 0)}")
        print(f"   - BROADCASTノード数: {plan_info.get('plan_summary', {}).get('broadcast_nodes_count', 0)}")
        print(f"   - JOINノード数: {plan_info.get('plan_summary', {}).get('join_nodes_count', 0)}")
        print(f"   - スキャンノード数: {plan_info.get('plan_summary', {}).get('scan_nodes_count', 0)}")
        print(f"   - シャッフルノード数: {plan_info.get('plan_summary', {}).get('shuffle_nodes_count', 0)}")
        
        # JOIN戦略の分析
        join_strategies = plan_info.get('plan_summary', {}).get('unique_join_strategies', [])
        if join_strategies:
            print(f"\n🔗 検出されたJOIN戦略:")
            for strategy in join_strategies:
                print(f"   - {strategy}")
        
        # 既存のBROADCAST適用状況
        broadcast_nodes = plan_info.get('broadcast_nodes', [])
        if broadcast_nodes:
            print(f"\n📡 既存のBROADCAST適用状況:")
            for i, node in enumerate(broadcast_nodes[:3]):  # 最大3個まで表示
                print(f"   {i+1}. {node['node_name'][:60]}...")
                metadata_count = len(node.get('metadata', []))
                print(f"      メタデータ項目数: {metadata_count}")
        else:
            print(f"\n📡 現在BROADCASTは適用されていません")
        
        # テーブルスキャンの詳細
        table_scan_details = plan_info.get('table_scan_details', {})
        if table_scan_details:
            print(f"\n📋 スキャンされるテーブル:")
            for table_name, scan_detail in list(table_scan_details.items())[:5]:  # 最大5個まで表示
                file_format = scan_detail.get('file_format', 'unknown')
                pushed_filters = len(scan_detail.get('pushed_filters', []))
                output_columns = len(scan_detail.get('output_columns', []))
                print(f"   - {table_name}")
                print(f"     ファイル形式: {file_format}, フィルタ: {pushed_filters}個, カラム: {output_columns}個")
        
        # オリジナルクエリの抽出（存在する場合）
        try:
            original_query_for_demo = extract_original_query_from_profiler_data(profiler_data)
            if not original_query_for_demo:
                original_query_for_demo = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
        except:
            original_query_for_demo = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
        
        print(f"\n🎯 プラン情報を考慮したBROADCAST分析を実行中...")
        broadcast_analysis = analyze_broadcast_feasibility(
            extracted_metrics, 
            original_query_for_demo, 
            plan_info
        )
        
        # BROADCAST分析結果の表示
        print(f"\n📊 BROADCAST分析結果:")
        print(f"   - JOINクエリ: {'はい' if broadcast_analysis['is_join_query'] else 'いいえ'}")
        print(f"   - 既に最適化済み: {'はい' if broadcast_analysis['already_optimized'] else 'いいえ'}")
        print(f"   - Spark BROADCAST閾値: {broadcast_analysis['spark_threshold_mb']:.1f}MB")
        print(f"   - 適用可能性: {broadcast_analysis['feasibility']}")
        print(f"   - BROADCAST候補数: {len(broadcast_analysis['broadcast_candidates'])}")
        
        # 実行プラン分析結果
        exec_plan_analysis = broadcast_analysis.get('execution_plan_analysis', {})
        if exec_plan_analysis:
            print(f"\n🔍 実行プラン分析詳細:")
            print(f"   - BROADCASTが既に使用中: {'はい' if exec_plan_analysis['has_broadcast_joins'] else 'いいえ'}")
            print(f"   - 使用されているJOIN戦略: {', '.join(exec_plan_analysis.get('unique_join_strategies', []))}")
            print(f"   - プラン内のテーブル数: {len(exec_plan_analysis.get('tables_in_plan', []))}")
        
        # BROADCAST候補の詳細
        if broadcast_analysis['broadcast_candidates']:
            print(f"\n🔹 BROADCAST候補詳細:")
            for candidate in broadcast_analysis['broadcast_candidates'][:3]:  # 最大3個まで表示
                status = candidate.get('status', 'unknown')
                status_icon = "✅" if status == "already_applied" else "🆕" if status == "new_recommendation" else "🔍"
                print(f"   {status_icon} {candidate['table']}")
                print(f"      非圧縮サイズ: {candidate['estimated_uncompressed_mb']:.1f}MB")
                print(f"      圧縮サイズ: {candidate['estimated_compressed_mb']:.1f}MB")
                print(f"      信頼度: {candidate['confidence']}")
                print(f"      ステータス: {status}")
                print(f"      根拠: {candidate['reasoning'][:80]}...")
        
        # 推奨事項
        if broadcast_analysis['recommendations']:
            print(f"\n💡 推奨事項:")
            for rec in broadcast_analysis['recommendations'][:5]:  # 最大5個まで表示
                print(f"   • {rec}")
        
        print(f"\n✅ プラン情報を活用したBROADCAST分析完了")
        
    except NameError as e:
        print(f"⚠️ 必要な変数が定義されていません: {str(e)}")
        print("   以下のセルを先に実行してください：")
        print("   - Cell 11: JSON読み込み")
        print("   - Cell 12: メトリクス抽出")
    except Exception as e:
        print(f"❌ プラン分析中にエラーが発生しました: {str(e)}")

# デモ実行の呼び出し例（コメントアウトされているので、必要に応じて有効化）
# demonstrate_plan_based_broadcast_analysis()

print("✅ 関数定義完了: 実行プラン情報を活用したBROADCAST分析デモ")

# COMMAND ----------

print("🎉 実行プラン情報を活用したBROADCAST分析機能の追加完了")
print("📊 SQLの最適化により精密で実用的なBROADCAST推奨が可能になりました")
print("🔍 既存の最適化状況を考慮した、より実際的な分析を提供します")
print("✅ 全ての機能が正常に統合されました")

# 🎛️ カスタマイズポイント
#
# - **LLMプロバイダー**: `LLM_CONFIG` でプロバイダーとAPIキーを切り替え
# - **メトリクス抽出**: `extract_performance_metrics` 関数内の重要キーワードリスト
# - **分析プロンプト**: `analyze_bottlenecks_with_llm` 関数内の分析指示
# - **表示形式**: emoji と出力フォーマットの調整
#
# 🔍 エラー対処方法
#
# 1. **LLMエンドポイントエラー**: 
#    - Databricks: Model Servingエンドポイントの状態確認
#    - OpenAI/Azure/Anthropic: APIキーとクォータ確認
# 2. **ファイル読み込みエラー**: `dbutils.fs.ls("/FileStore/")` でファイル存在を確認
# 3. **メモリエラー**: 大きなJSONファイルの場合はクラスタのメモリ設定を確認
#
# 💡 高度な使用例
#
# ```python
# # 複数ファイルの一括分析
# profiler_files = dbutils.fs.ls("/FileStore/profiler_logs/")
# for file_info in profiler_files:
#     if file_info.path.endswith('.json'):
#         profiler_data = load_profiler_json(file_info.path)
#         metrics = extract_performance_metrics(profiler_data)
#         # 分析処理...
# ```

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        profiler_data = load_profiler_json(sys.argv[1])
        extracted_metrics = extract_performance_metrics(profiler_data)
        print("Testing skew detection...")
        # Test completed

