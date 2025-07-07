# Databricks SQLプロファイラー分析ツール

DatabricksのSQLプロファイラーJSONログファイルを読み込み、ボトルネック特定と改善案の提示に必要なメトリクスを抽出・分析するツールです。

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

## ファイル構成

- `databricks_sql_profiler_analysis_script.py` - 完全版スクリプト（一括実行用）
- `databricks_notebook_cells.py` - **Notebook版（推奨）**：セル分割されたコード
- `simple0.json` - サンプルのSQLプロファイラーJSONファイル
- `extracted_metrics.json` - 抽出されたメトリクス（出力）
- `bottleneck_analysis_result.txt` - AI分析結果（出力）
- `README.md` - このファイル

## 📚 使用方法

### 🔥 推奨: Databricks Notebook版

#### ステップ 1: Notebookの作成

1. Databricks ワークスペースで新しいNotebookを作成
2. 言語を「Python」に設定

#### ステップ 2: セルのコピーペースト

`databricks_notebook_cells.py` ファイルを開き、各セクションを以下の手順でコピーペーストします：

```python
# === セル 1: マークダウンセル ===
# セルタイプを「Markdown」に変更して以下をコピー
```

**具体的な手順:**

1. **セル 1 (Markdown)**: セルタイプを「Markdown」に変更して、機能説明をコピー
2. **セル 2 (Python)**: ライブラリインポート
3. **セル 3 (Python)**: JSONファイル読み込み関数
4. **セル 4 (Python)**: メトリクス抽出関数
5. **セル 5 (Python)**: ボトルネック指標計算関数
6. **セル 6 (Python)**: Claude 3.7 Sonnet分析関数
7. **セル 7 (Markdown)**: メイン処理の説明
8. **セル 8 (Python)**: 設定とファイル読み込み
9. **セル 9 (Python)**: メトリクス抽出と表示
10. **セル 10 (Python)**: メトリクス保存とDataFrame表示
11. **セル 11 (Python)**: AI分析実行
12. **セル 12 (Python)**: 分析結果表示と保存
13. **セル 13 (Markdown)**: 追加の使用方法とカスタマイズ

#### ステップ 3: ファイルパスの設定

セル 8 で `JSON_FILE_PATH` を実際のファイルパスに変更：

```python
# 例: DBFSにアップロードしたファイルの場合
JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'

# 例: dbfs:// 形式の場合
JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'
```

#### ステップ 4: 実行

セルを順番に実行（Shift+Enter または 「▶ Run All」）

### 🖥️ 代替: Python スクリプト版

```python
# スクリプトファイルを実行
%run /FileStore/databricks_sql_profiler_analysis_script.py
```

## セットアップ手順

### 1. Databricks環境の準備

#### Model Servingエンドポイントの設定

1. **Databricks ワークスペースにログイン**

2. **Claude 3.7 Sonnetエンドポイントの作成**
   
   **UI での作成:**
   - Databricks UI > Serving > Model Serving
   - "Create Serving Endpoint" をクリック
   - エンドポイント名: `databricks-claude-3-7-sonnet`
   - モデル: Claude 3.7 Sonnet
   - ワークロードサイズ: Small または Medium

   **CLI での作成:**
   ```bash
   databricks serving-endpoints create \
     --name "databricks-claude-3-7-sonnet" \
     --config '{
       "served_entities": [{
         "entity_name": "databricks-claude-3-7-sonnet",
         "entity_version": "1",
         "workload_type": "GPU_MEDIUM",
         "workload_size": "Small"
       }]
     }'
   ```

3. **エンドポイントの確認**
   - Databricks UI > Serving > Model Serving
   - `databricks-claude-3-7-sonnet` エンドポイントが「Ready」状態であることを確認

#### 権限設定

1. **Personal Access Token の作成**
   - Settings > Developer > Access tokens
   - 新しいトークンを生成（Model Servingの権限が必要）

2. **ワークスペース権限**
   - Model Servingエンドポイントへのアクセス権限
   - クラスターでの実行権限

### 2. 必要な依存関係

Databricks環境には以下が標準装備されています：

```python
# 標準ライブラリ（通常は既にインストール済み）
import json
import pandas as pd
from typing import Dict, List, Any
import requests

# Spark関連（Databricksに標準装備）
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

### 3. SQLプロファイラーJSONファイルの取得と配置

#### Databricks UI から取得

1. **SQL Editor または Notebook でクエリを実行**

2. **Query History から該当クエリを選択**
   - Compute > SQL Warehouses > Query History
   - または直接 SQL Editor の履歴から

3. **Query Profile を開く**
   - クエリ詳細ページの "Query Profile" タブ

4. **JSONエクスポート**
   - "Download Profile JSON" ボタンをクリック
   - ファイルを保存

#### ファイルアップロード方法

**方法 1: Databricks UI アップロード**
1. Data > Create Table
2. "Upload File" を選択
3. JSONファイルをアップロード
4. パスをメモ（例: `/FileStore/shared_uploads/user@company.com/profile.json`）

**方法 2: dbutils でアップロード**
```python
# ローカルファイルをDBFSにアップロード
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# 外部ストレージからのコピー
dbutils.fs.cp("s3a://bucket/profiler.json", "dbfs:/FileStore/profiler.json")
```

**方法 3: Databricks CLI**
```bash
# ローカルからDBFSへアップロード
databricks fs cp profiler.json dbfs:/FileStore/profiler.json
```

#### SQL での取得

```sql
-- クエリ履歴からプロファイル情報を取得
SELECT query_id, query_text, profile_json 
FROM system.query.history 
WHERE query_start_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY query_start_time DESC
LIMIT 10;
```

## 📊 出力ファイル

### extracted_metrics.json

```json
{
  "query_info": {
    "query_id": "01f0565c-48f6-1283-a782-14ed6494eee0",
    "status": "FINISHED",
    "user": "mitsuhiro.itagaki@databricks.com"
  },
  "overall_metrics": {
    "total_time_ms": 84224,
    "compilation_time_ms": 876,
    "execution_time_ms": 83278,
    "read_bytes": 123926013605,
    "cache_hit_ratio": 0.003
  },
  "bottleneck_indicators": {
    "compilation_ratio": 0.010,
    "cache_hit_ratio": 0.003,
    "data_selectivity": 0.000022,
    "slowest_stage_id": "229"
  }
}
```

### bottleneck_analysis_result.txt

AI による詳細な分析結果（日本語）：
- 主要なボトルネックの特定
- パフォーマンス改善の優先順位
- 具体的な最適化案
- 予想される改善効果

## 🔧 トラブルシューティング

### よくある問題

1. **Claude 3.7 Sonnetエンドポイントエラー**
   ```
   APIエラー: ステータスコード 404
   ```
   - エンドポイント名を確認: `databricks-claude-3-7-sonnet`
   - Model Servingでエンドポイントが稼働中であることを確認
   - エンドポイントの状態が「Ready」になっているか確認

2. **認証エラー**
   ```
   分析エラー: 'dbutils' is not defined
   ```
   - Databricksクラスター上で実行されていることを確認
   - Personal Access Tokenの権限を確認

3. **JSONファイル読み込みエラー**
   ```
   ファイル読み込みエラー: [Errno 2] No such file or directory
   ```
   - ファイルパスを確認
   - ファイルがDBFS または ローカルに正しくアップロードされているか確認

### デバッグ方法

1. **ステップバイステップ実行**
   ```python
   # 各段階での確認
   print("1. JSON読み込み:", bool(profiler_data))
   print("2. メトリクス抽出:", len(extracted_metrics.get('node_metrics', [])))
   print("3. ボトルネック指標:", extracted_metrics.get('bottleneck_indicators', {}))
   ```

2. **エンドポイント接続テスト**
   ```python
   # Claude エンドポイントの接続確認
   try:
       token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
       workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
       print(f"Workspace URL: {workspace_url}")
       print(f"Token length: {len(token)}")
   except Exception as e:
       print(f"認証設定エラー: {e}")
   ```

3. **ファイル存在確認**
   ```python
   # DBFSファイルの確認
   dbutils.fs.ls("/FileStore/shared_uploads/")
   
   # ローカルファイルの確認
   import os
   print(f"Current directory: {os.getcwd()}")
   print(f"Files: {os.listdir('.')}")
   ```

## ⚙️ カスタマイズ

### メトリクス抽出のカスタマイズ

```python
# extract_performance_metrics 関数内の重要キーワードを変更
important_keywords = [
    'TIME', 'DURATION', 'MEMORY', 'ROWS', 'BYTES', 'SPILL',
    'PEAK', 'CUMULATIVE', 'EXCLUSIVE', 'WAIT', 'CPU',
    'NETWORK', 'DISK', 'SHUFFLE'  # 追加のキーワード
]
```

### 分析プロンプトのカスタマイズ

```python
# analyze_bottlenecks_with_claude 関数内の analysis_prompt を変更
analysis_prompt = f"""
あなたは特定の業界のSQLパフォーマンス専門家です...
追加の分析観点:
- ETL処理の最適化
- リアルタイム分析への適用性
- コスト効率性の評価
...
"""
```

### 出力フォーマットのカスタマイズ

```python
# HTML レポート生成例
def generate_html_report(metrics, analysis_result):
    html_content = f"""
    <html>
    <head>
        <title>SQL Performance Analysis Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .metric {{ background: #f5f5f5; padding: 10px; margin: 5px 0; }}
            .bottleneck {{ color: red; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>ボトルネック分析レポート</h1>
        <div class="metric">
            <h3>クエリ基本情報</h3>
            <p>ID: {metrics['query_info']['query_id']}</p>
            <p>実行時間: {metrics['overall_metrics']['total_time_ms']:,} ms</p>
        </div>
        <div class="analysis">
            <h3>AI分析結果</h3>
            <pre>{analysis_result}</pre>
        </div>
    </body>
    </html>
    """
    with open('analysis_report.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print("HTML レポートを生成しました: analysis_report.html")

# 使用例
generate_html_report(extracted_metrics, analysis_result)
```

### SparkDataFrame での詳細分析

```python
# ノードメトリクスの詳細分析
node_data = []
for node in extracted_metrics['node_metrics']:
    node_data.append({
        'node_id': node['node_id'],
        'name': node['name'],
        'tag': node['tag'],
        'rows_num': node['key_metrics'].get('rowsNum', 0),
        'duration_ms': node['key_metrics'].get('durationMs', 0),
        'peak_memory_bytes': node['key_metrics'].get('peakMemoryBytes', 0)
    })

node_df = spark.createDataFrame(node_data)
node_df.createOrReplaceTempView("node_metrics")

# SQL での分析
result = spark.sql("""
    SELECT 
        name,
        duration_ms,
        peak_memory_bytes / 1024 / 1024 as peak_memory_mb,
        rows_num,
        CASE 
            WHEN duration_ms > 10000 THEN 'HIGH'
            WHEN duration_ms > 1000 THEN 'MEDIUM'
            ELSE 'LOW'
        END as duration_category
    FROM node_metrics
    WHERE rows_num > 0
    ORDER BY duration_ms DESC
""")

result.show()
```

## 🚀 高度な使用例

### 複数クエリの一括分析

```python
# 複数のプロファイラーファイルを一括処理
profiler_files = [
    'dbfs:/FileStore/profiles/query1.json',
    'dbfs:/FileStore/profiles/query2.json',
    'dbfs:/FileStore/profiles/query3.json'
]

all_results = []
for file_path in profiler_files:
    profiler_data = load_profiler_json(file_path)
    if profiler_data:
        metrics = extract_performance_metrics(profiler_data)
        all_results.append(metrics)

# 一括分析結果の比較
comparison_df = spark.createDataFrame([
    {
        'query_id': result['query_info']['query_id'],
        'total_time_ms': result['overall_metrics']['total_time_ms'],
        'read_gb': result['overall_metrics']['read_bytes'] / 1024 / 1024 / 1024,
        'cache_hit_ratio': result['bottleneck_indicators'].get('cache_hit_ratio', 0)
    }
    for result in all_results
])

comparison_df.show()
```

### 自動スケジュール分析

```python
# Databricks Jobs での定期実行設定例
def scheduled_analysis():
    """定期実行用の分析関数"""
    # 最新のクエリプロファイルを取得
    latest_profiles = spark.sql("""
        SELECT query_id, profile_json
        FROM system.query.history 
        WHERE query_start_time >= current_timestamp() - INTERVAL 1 HOUR
        AND total_time_ms > 30000  -- 30秒以上のクエリのみ
        ORDER BY total_time_ms DESC
        LIMIT 5
    """)
    
    for row in latest_profiles.collect():
        profile_data = json.loads(row.profile_json)
        metrics = extract_performance_metrics(profile_data)
        analysis = analyze_bottlenecks_with_claude(metrics)
        
        # Slack通知やメール送信などの処理
        send_alert_if_bottleneck_detected(metrics, analysis)

# 使用例（Jobsで定期実行）
scheduled_analysis()
```

## 📝 ライセンス・注意事項

- このツールはサンプル・教育目的での使用を想定しています
- プロダクション環境での使用前には十分なテストを行ってください
- Databricks Claude 3.7 Sonnetの利用には適切なライセンスと権限が必要です
- 大量のデータやクエリの分析には実行時間とコストに注意してください
- 機密性の高いクエリログの取り扱いには十分注意してください

## 📞 サポート・フィードバック

問題や改善提案がある場合は、以下の観点で情報を整理してください：

1. **Databricks環境の詳細**
   - Runtime version
   - Cluster configuration
   - Spark version

2. **エラー情報**
   - エラーメッセージの全文
   - 実行時のログ
   - スクリーンショット

3. **使用状況**
   - 使用したJSONファイルのサイズと構造概要
   - 期待する動作と実際の動作の差異
   - カスタマイズ内容

4. **環境設定**
   - Model Servingエンドポイントの設定状況
   - Personal Access Tokenの権限
   - ファイルアップロード方法

このツールを使用してSQLクエリのパフォーマンス改善に役立てていただければ幸いです。
