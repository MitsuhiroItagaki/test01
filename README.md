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

- `databricks_sql_profiler_analysis.py` - メインスクリプト（Pythonファイル）
- `simple0.json` - サンプルのSQLプロファイラーJSONファイル
- `extracted_metrics.json` - 抽出されたメトリクス（出力）
- `bottleneck_analysis_result.txt` - AI分析結果（出力）
- `README.md` - このファイル

## セットアップ手順

### 1. Databricks環境の準備

#### Model Servingエンドポイントの設定

1. **Databricks ワークスペースにログイン**

2. **Claude 3.7 Sonnetエンドポイントの作成**
   ```bash
   # Databricks CLIを使用してエンドポイントを作成
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
   - `databricks-claude-3-7-sonnet` エンドポイントが存在することを確認

#### 権限設定

1. **Personal Access Token の作成**
   - Settings > Developer > Access tokens
   - 新しいトークンを生成（Model Servingの権限が必要）

2. **ワークスペース権限**
   - Model Servingエンドポイントへのアクセス権限
   - クラスターでの実行権限

### 2. 必要な依存関係

Databricks環境には以下が必要です：

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

### 3. SQLプロファイラーJSONファイルの取得

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

#### SQL での取得

```sql
-- クエリ履歴からプロファイル情報を取得
SELECT query_id, query_text, profile_json 
FROM system.query.history 
WHERE query_start_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY query_start_time DESC
LIMIT 10;
```

## 使用方法

### Python スクリプト版

```python
# ファイルを適切な場所に配置
# - databricks_sql_profiler_analysis.py
# - simple0.json（または分析対象のJSONファイル）

# スクリプトを実行
%run /path/to/databricks_sql_profiler_analysis.py
```

### Notebook セル版

以下の順序でセルを作成・実行：

#### セル 1: ライブラリインポート

```python
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
```

#### セル 2: ユーティリティ関数定義

```python
# databricks_sql_profiler_analysis.py の関数定義をコピー
# load_profiler_json, extract_performance_metrics, calculate_bottleneck_indicators 等
```

#### セル 3: データ読み込み・分析実行

```python
# JSONファイル読み込み
profiler_data = load_profiler_json('simple0.json')

# メトリクス抽出
extracted_metrics = extract_performance_metrics(profiler_data)

# 結果確認
print("抽出されたメトリクス概要:")
print(f"ステージ数: {len(extracted_metrics['stage_metrics'])}")
print(f"ノード数: {len(extracted_metrics['node_metrics'])}")
```

#### セル 4: AI分析実行

```python
# Claude 3.7 Sonnetによるボトルネック分析
analysis_result = analyze_bottlenecks_with_claude(extracted_metrics)

# 結果表示
print("=== ボトルネック分析結果 ===")
print(analysis_result)
```

#### セル 5: 結果保存

```python
# 抽出メトリクスとAI分析結果の保存
save_extracted_metrics(extracted_metrics, 'extracted_metrics.json')
save_analysis_result(analysis_result, 'bottleneck_analysis_result.txt')
```

## 出力ファイル

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

## トラブルシューティング

### よくある問題

1. **Claude 3.7 Sonnetエンドポイントエラー**
   ```
   APIエラー: ステータスコード 404
   ```
   - エンドポイント名を確認: `databricks-claude-3-7-sonnet`
   - Model Servingでエンドポイントが稼働中であることを確認

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

## カスタマイズ

### メトリクス抽出のカスタマイズ

```python
# important_keywords を変更してフィルタリング条件を調整
important_keywords = [
    'TIME', 'DURATION', 'MEMORY', 'ROWS', 'BYTES', 'SPILL',
    'PEAK', 'CUMULATIVE', 'EXCLUSIVE', 'WAIT', 'CPU',
    'YOUR_CUSTOM_KEYWORD'  # 追加のキーワード
]
```

### 分析プロンプトのカスタマイズ

```python
# analyze_bottlenecks_with_claude 関数内の analysis_prompt を変更
analysis_prompt = f"""
あなたの独自の分析指示...
{metrics_summary}
追加の分析観点...
"""
```

### 出力フォーマットのカスタマイズ

```python
# HTML レポート生成
def generate_html_report(metrics, analysis_result):
    html_content = f"""
    <html><head><title>SQL Performance Analysis</title></head>
    <body>
    <h1>ボトルネック分析レポート</h1>
    <pre>{analysis_result}</pre>
    </body></html>
    """
    with open('analysis_report.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
```

## ライセンス・注意事項

- このツールはサンプル・教育目的での使用を想定しています
- プロダクション環境での使用前には十分なテストを行ってください
- Databricks Claude 3.7 Sonnetの利用には適切なライセンスと権限が必要です
- 大量のデータやクエリの分析には実行時間とコストに注意してください

## サポート・フィードバック

問題や改善提案がある場合は、以下の観点で情報を整理してください：

1. Databricks環境の詳細（Runtime version, Cluster configuration）
2. エラーメッセージの全文
3. 使用したJSONファイルのサイズと構造概要
4. 期待する動作と実際の動作の差異

このツールを使用してSQLクエリのパフォーマンス改善に役立てていただければ幸いです。
