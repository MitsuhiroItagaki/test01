# Databricks SQLプロファイラー分析ツール

**最先端のAI駆動SQLパフォーマンス分析ツール**

DatabricksのSQLプロファイラーJSONログファイルを読み込み、AI（LLM）を活用してボトルネック特定・改善案提示・SQL最適化を行う包括的な分析ツールです。

## ✨ 主要機能

### 🔍 **高度なパフォーマンス分析**
- SQLプロファイラーJSONファイルの自動解析
- 時間消費TOP10プロセスの詳細分析（カタログ.スキーマ.テーブルのフルパス表示対応）
- スピル検出・データスキュー・並列度問題の特定
- Photonエンジン利用状況の可視化

### 🤖 **マルチプロバイダーAI分析**
- **Databricks Claude 3.7 Sonnet**（推奨・128K tokens）
- **OpenAI GPT-4/GPT-4 Turbo**（16K tokens）
- **Azure OpenAI**（16K tokens）
- **Anthropic Claude**（16K tokens）
- **思考プロセス表示機能**（thinking_enabled）で分析過程を可視化

### 🗂️ **Liquid Clustering最適化**
- プロファイラーデータからカラム使用パターンを分析
- フィルター・JOIN・GROUP BY条件の自動抽出
- テーブル別クラスタリング推奨カラムの特定
- パフォーマンス向上見込みの定量評価

### 🚀 **SQL自動最適化**
- オリジナルクエリの自動抽出
- AI駆動によるクエリ最適化
- 実行可能な最適化SQLの生成
- **Databricks Notebook専用設計**（テスト実行方法を内包）

### 📊 **包括的レポーティング**
- **output_**接頭語付きファイル自動生成
- 視覚的なダッシュボード表示
- 詳細なボトルネック分析レポート
- **TOP10処理時間分析**も自動保存
- パフォーマンス改善の定量的評価

## 📁 ファイル構成

```
📦 Databricks SQL Profiler Analysis Tool
├── 📄 databricks_sql_profiler_analysis.py    # 🌟 メインノートブック（55セル構成）
├── 📄 simple0.json                           # サンプルSQLプロファイラーファイル
├── 📄 README.md                              # このファイル
├── 📁 outputs/                               # 生成ファイル（実行時作成）
│   ├── 📄 output_extracted_metrics_YYYYMMDD-HHMISS.json
│   ├── 📄 output_bottleneck_analysis_result_YYYYMMDD-HHMISS.txt
│   ├── 📄 output_original_query_YYYYMMDD-HHMISS.sql
│   ├── 📄 output_optimized_query_YYYYMMDD-HHMISS.sql
│   └── 📄 output_optimization_report_YYYYMMDD-HHMISS.md
└──  samples/                               # 追加サンプル（オプション）
    ├── 📄 largeplan.json
    └── 📄 nophoton.json
```

## 🚀 クイックスタート

### ステップ 1: Notebookの作成

1. **Databricks ワークスペース**で新しいNotebookを作成
2. 言語を「**Python**」に設定
3. `databricks_sql_profiler_analysis.py`の内容をコピー＆ペースト

### ステップ 2: 基本設定

```python
# 📁 分析対象ファイル設定（セル4）
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'

# 🤖 LLMエンドポイント設定（セル6）
LLM_CONFIG = {
    "provider": "databricks",  # "databricks", "openai", "azure_openai", "anthropic"
    "thinking_enabled": True,  # 思考プロセス表示（推奨）
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens
        "temperature": 0.1
    },
    "openai": {
        "api_key": "",  # OpenAI APIキー
        "model": "gpt-4o",
        "max_tokens": 16000,  # 16K tokens
        "temperature": 0.1
    },
    "azure_openai": {
        "api_key": "",
        "endpoint": "",
        "deployment_name": "",
        "api_version": "2024-02-01",
        "max_tokens": 16000,  # 16K tokens
        "temperature": 0.1
    },
    "anthropic": {
        "api_key": "",
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000,  # 16K tokens
        "temperature": 0.1
    }
}
```

### ステップ 3: 順次実行

```bash
🔧 設定・準備セクション     → セル3〜17を実行
🚀 メイン処理実行セクション  → セル18〜40を実行
🔧 SQL最適化機能セクション   → セル43〜53を実行（オプション）
📚 参考・応用セクション      → セル55参照
```

## 📋 セル構成詳細

### 🔧 設定・準備セクション（セル3-17）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 4 | 📁 分析対象ファイル設定 | JSONファイルパスの指定 |
| 6 | 🤖 LLMエンドポイント設定 | AI分析プロバイダーの選択（thinking機能含む） |
| 8 | 📂 ファイル読み込み関数 | DBFS/FileStore/ローカル対応 |
| 9 | 📊 メトリクス抽出関数 | パフォーマンス指標の抽出 |
| 10 | 🏷️ ノード名解析関数 | 意味のあるノード名への変換（フルパス対応） |
| 11 | 🎯 ボトルネック計算関数 | 指標計算とスピル検出 |
| 12 | 🧬 Liquid Clustering関数 | クラスタリング分析 |
| 13 | 🤖 LLM分析関数 | AI分析用プロンプト生成 |
| 14-17 | 🔌 LLMプロバイダー関数 | 各AIサービス接続 |

### 🚀 メイン処理実行セクション（セル18-40）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 23 | 🚀 ファイル読み込み実行 | JSONデータの読み込み |
| 26 | 📊 メトリクス抽出 | 性能指標の抽出と表示 |
| 33 | 🔍 ボトルネック詳細分析 | TOP10時間消費プロセス（フルパス表示） |
| 35 | 💾 メトリクス保存 | output_extracted_metrics_*.json出力 |
| 37 | 🗂️ Liquid Clustering分析 | クラスタリング推奨 |
| 39 | 📋 LLM分析準備 | AI分析の実行準備 |
| 40 | 🎯 AI分析結果表示 | ボトルネック分析結果（結論のみ表示） |

### 🔧 SQL最適化機能セクション（セル43-53）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 43 | 🔧 最適化関数定義 | SQL最適化関数の定義（thinking対応） |
| 46 | 🚀 クエリ抽出 | オリジナルクエリの抽出 |
| 47 | 🤖 LLM最適化実行 | AI駆動クエリ最適化（結論のみ表示） |
| 49 | 💾 結果保存 | output_*ファイル生成（TOP10分析含む） |
| 50 | 🧪 実行ガイド | Databricks Notebook実行方法 |
| 53 | 🏁 完了サマリー | 全処理の完了確認（動的プロバイダー表示） |

## 🔧 セットアップ詳細

### 1. LLMエンドポイントの設定

#### Databricks Claude 3.7 Sonnet（推奨）

```bash
# Databricks CLI での作成
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

#### 他のLLMプロバイダー

```python
# OpenAI設定例（16K tokens）
LLM_CONFIG = {
    "provider": "openai",
    "thinking_enabled": False,  # OpenAIでは標準的なレスポンス
    "openai": {
        "api_key": "sk-...",  # または環境変数OPENAI_API_KEY
        "model": "gpt-4o",
        "max_tokens": 16000  # 16K tokens設定
    }
}

# Azure OpenAI設定例（16K tokens）
LLM_CONFIG = {
    "provider": "azure_openai",
    "thinking_enabled": False,
    "azure_openai": {
        "api_key": "your-azure-key",
        "endpoint": "https://your-resource.openai.azure.com/",
        "deployment_name": "gpt-4",
        "api_version": "2024-02-01",
        "max_tokens": 16000  # 16K tokens設定
    }
}

# Anthropic設定例（16K tokens）
LLM_CONFIG = {
    "provider": "anthropic", 
    "thinking_enabled": True,  # Anthropicでもthinking対応
    "anthropic": {
        "api_key": "sk-ant-...",  # または環境変数ANTHROPIC_API_KEY
        "model": "claude-3-5-sonnet-20241022",
        "max_tokens": 16000  # 16K tokens設定
    }
}
```

### 2. 思考プロセス表示機能（thinking_enabled）

```python
# thinking_enabled: True の場合
LLM_CONFIG = {
    "provider": "databricks",
    "thinking_enabled": True,  # 拡張思考モード（結論のみ表示）
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet"
    }
}

# 出力例：（思考過程は除外し、結論のみを表示）
分析結果として、主要なボトルネックは...
推奨される改善策は...
```

### 3. SQLプロファイラーファイルの取得

#### Databricks SQLエディタから取得

1. **SQLクエリを実行**
2. **Query History** → 対象クエリを選択
3. **Query Profile** タブ → **Download Profile JSON**

#### ファイルアップロード方法

```python
# 方法1: Databricks UI
# Data → Create Table → Upload File → JSONファイルをドラッグ&ドロップ

# 方法2: dbutils
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# 方法3: Volumes（推奨）
# Unity Catalog Volumes内にアップロード
```

## 📊 出力ファイル詳細（output_接頭語付き）

### 📄 output_extracted_metrics_YYYYMMDD-HHMISS.json

```json
{
  "query_info": {
    "query_id": "01f0565c-48f6-1283-a782-14ed6494eee0",
    "status": "FINISHED", 
    "user": "user@company.com",
    "query_text": "SELECT customer_id, SUM(amount)..."
  },
  "overall_metrics": {
    "total_time_ms": 84224,
    "compilation_time_ms": 876,
    "execution_time_ms": 83278,
    "read_bytes": 123926013605,
    "photon_enabled": true,
    "photon_utilization_ratio": 0.85
  },
  "bottleneck_indicators": {
    "compilation_ratio": 0.010,
    "cache_hit_ratio": 0.003,
    "data_selectivity": 0.000022,
    "has_spill": true,
    "spill_bytes": 1073741824,
    "shuffle_operations_count": 3,
    "has_shuffle_bottleneck": true
  },
  "liquid_clustering_analysis": {
    "recommended_tables": {
      "catalog.schema.customer": {
        "clustering_columns": ["customer_id", "region"],
        "scan_performance": {
          "rows_scanned": 1000000,
          "scan_duration_ms": 15000,
          "efficiency_score": 66.67
        }
      }
    }
  }
}
```

### 📄 output_bottleneck_analysis_result_YYYYMMDD-HHMISS.txt

```text
🔍 Databricks SQL プロファイラー分析結果

📊 【クエリ基本情報】
🆔 クエリID: 01f0565c-48f6-1283-a782-14ed6494eee0
⏱️ 実行時間: 84,224 ms (84.2秒)
💾 読み込みデータ: 115.4 GB
📈 出力行数: 2,753 行

## 🤔 思考過程
このクエリの分析を始めます...
実行時間84秒は一般的なクエリとしては長いため、ボトルネックを特定する必要があります...
スピルが1GBも発生しているのが主要な問題のようです...
============================================================

## � 回答内容

�🚨 【特定されたボトルネック】

1. 🔥 **大量スピル発生 (HIGH PRIORITY)**
   - スピル量: 1.0 GB
   - 原因: メモリ不足による中間結果のディスク書き込み
   - 影響: 実行時間の30-50%増加

2. ⚡ **シャッフル操作ボトルネック (MEDIUM PRIORITY)**  
   - シャッフル回数: 3回
   - 最大シャッフル時間: 15,234 ms
   - 影響: 全体実行時間の18%

## � 最も時間がかかっている処理TOP10
=================================================================================
�📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー

 1. 🔴💚🔥💿✅ [CRITICAL] Data Source Scan (catalog.schema.large_table)
    ⏱️  実行時間:   45,234 ms ( 45.2 sec) - 全体の 53.7%
    📊 処理行数: 12,345,678 行
    💾 ピークメモリ: 2048.0 MB
    🔧 並列度: 128 タスク | 💿 スピル: あり | ⚖️ スキュー: なし

 2. 🟠⚠️🔥✅⚖️ [HIGH    ] HashAggregate
    ⏱️  実行時間:   18,456 ms ( 18.5 sec) - 全体の 21.9%
    📊 処理行数:  1,234,567 行
    💾 ピークメモリ: 1024.0 MB
    🔧 並列度:  64 タスク | 💿 スピル: なし | ⚖️ スキュー: あり
...

🚀 【推奨改善策】

1. **メモリ設定の最適化**
   - spark.sql.adaptive.coalescePartitions.enabled = true
   - クラスターメモリ増強 (32GB → 64GB推奨)

2. **Liquid Clusteringの適用**
   - catalog.schema.customer テーブル: customer_id, region でクラスタリング
   - 期待効果: スキャン時間50-70%削減

📈 【期待される改善効果】
- 実行時間: 84.2秒 → 35-45秒 (約50%削減)
- コスト削減: 約60%
- スピル解消: 100%削減見込み
```

### 📄 output_optimization_report_YYYYMMDD-HHMISS.md

```markdown
# SQL最適化レポート

**クエリID**: 01f0565c-48f6-1283-a782-14ed6494eee0
**最適化日時**: 2024-01-15 14:30:22
**オリジナルファイル**: output_original_query_20240115-143022.sql
**最適化ファイル**: output_optimized_query_20240115-143022.sql

## 最適化分析結果

## 🤔 思考過程
このクエリの最適化を検討します...
まず、データソーススキャンが最も時間を消費していることがわかります...
スピルを解消するためにはメモリ効率的なクエリ構造が必要です...
============================================================

## 📄 回答内容

## 🚀 最適化されたSQLクエリ

```sql
-- PHOTONエンジン最適化とLiquid Clustering対応
WITH customer_filtered AS (
  SELECT customer_id, region, signup_date
  FROM catalog.schema.customer 
  WHERE region IN ('US', 'EU')  -- 早期フィルタリング
    AND signup_date >= '2023-01-01'
),
orders_summary AS (
  SELECT 
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as order_count
  FROM catalog.schema.orders 
  WHERE order_date >= '2023-01-01'  -- Liquid Clustering活用
  GROUP BY customer_id
)
SELECT /*+ BROADCAST(c) */
  c.customer_id,
  c.region,
  COALESCE(o.total_amount, 0) as total_amount,
  COALESCE(o.order_count, 0) as order_count
FROM customer_filtered c
LEFT JOIN orders_summary o ON c.customer_id = o.customer_id
ORDER BY total_amount DESC
LIMIT 100;
```

## � 最適化のポイント

1. **早期フィルタリング**: WHERE句を各CTEに配置してデータ量を削減
2. **Liquid Clustering活用**: パーティション剪定による効率的スキャン
3. **Broadcast JOIN**: 小さなテーブルをブロードキャストして性能向上

## 📈 期待される効果

- **実行時間**: 84.2秒 → 35-45秒 (改善率: 50%)
- **メモリ使用量**: スピル解消により30%削減
- **スピル削減**: 1GB → 0GB (100%削減)

## パフォーマンスメトリクス参考情報

- **実行時間**: 84,224 ms
- **読み込みデータ**: 115.40 GB
- **スピル**: 1.00 GB

## � 最も時間がかかっている処理TOP10
[TOP10の詳細分析が含まれます...]
```

## 🔍 高度な機能

### 📊 思考プロセス表示（thinking_enabled）

```python
# thinking_enabled: True の詳細設定
LLM_CONFIG = {
    "provider": "databricks",
    "thinking_enabled": True,  # 思考プロセス表示を有効化
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens（思考プロセス含む）
        "temperature": 0.1
    }
}

# 出力の構造化（思考過程除外）
## � 分析結果
- 簡潔で理解しやすい最終結論
- 具体的な推奨事項
- 冗長な思考過程は除外
```

### � Databricks Notebook専用実行

```python
# %sql マジックコマンドでの実行
optimized_sql = open('output_optimized_query_20240115-143022.sql').read()

# Spark SQLでの実行
df = spark.sql(optimized_sql)
df.show()

# パフォーマンス測定
import time
start_time = time.time()
result_count = df.count()
execution_time = time.time() - start_time
print(f'実行時間: {execution_time:.2f} 秒, 行数: {result_count:,}')

# クエリプランの確認
df.explain(True)
```

## 🛠️ トラブルシューティング

### ❌ よくあるエラーと解決方法

#### 1. thinking_enabled関連エラー

```bash
# エラー例
AttributeError: 'list' object has no attribute 'startswith'
TypeError: write() argument must be str, not list

# 解決方法
✅ 自動対応済み: format_thinking_response()関数で適切に処理
- リスト形式のレスポンスを人間に読みやすい形式に変換
- 改行コード(\n)を実際の改行に変換
- 思考過程は除外し、結論のみを表示
```

#### 2. LLMプロバイダー設定エラー

```python
# エラー例  
動的プロバイダー表示でのKeyError

# 解決方法
✅ 動的表示機能: 設定されたプロバイダーに応じて自動表示
# Databricks設定時
"✅ Databricks (databricks-claude-3-7-sonnet)によるボトルネック分析完了"

# OpenAI設定時  
"✅ OpenAI (gpt-4o)によるボトルネック分析完了"

# エラー時のフォールバック
"✅ LLMによるボトルネック分析完了"
```

#### 3. ファイル出力の問題

```python
# 問題: 出力ファイルの識別が困難
# 解決: output_接頭語の統一

生成されるファイル:
✅ output_extracted_metrics_20240115-143022.json
✅ output_bottleneck_analysis_result_20240115-143022.txt  
✅ output_original_query_20240115-143022.sql
✅ output_optimized_query_20240115-143022.sql
✅ output_optimization_report_20240115-143022.md

# TOP10分析も自動でレポートに含まれます
```

### � 最適化のベストプラクティス

#### 1. thinking_enabled使用時

```python
# 本格的な分析にはthinking_enabledを推奨
LLM_CONFIG = {
    "provider": "databricks",
    "thinking_enabled": True,  # 詳細な分析プロセスを表示
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072  # 128K tokens（思考プロセス込み）
    }
}

# 高速実行が必要な場合
LLM_CONFIG = {
    "provider": "databricks", 
    "thinking_enabled": False,  # 結果のみ表示（高速）
    "databricks": {
        "max_tokens": 131072
    }
}
```

#### 2. プロバイダー選択の指針

```python
# 用途別推奨プロバイダー
推奨設定:
🥇 Databricks: 128K tokens、thinking対応、Unity Catalog統合
🥈 Anthropic: 16K tokens、thinking対応、高品質分析  
🥉 OpenAI: 16K tokens、安定性重視
🥉 Azure OpenAI: 16K tokens、企業利用向け
```

## � 今後の機能拡張

### � 計画中の機能

- **リアルタイム監視**: クエリ実行時の自動分析
- **比較分析**: 複数クエリの性能比較機能
- **自動チューニング**: 推奨設定の自動適用
- **ダッシュボード**: Grafana/Tableau連携
- **アラート**: 性能劣化の自動検知・通知

---

## 📞 サポート・コミュニティ

- **GitHub Issues**: バグレポート・機能要望
- **Databricks Community**: 使用方法・ベストプラクティス
- **技術ブログ**: 詳細な使用例・カスタマイズ方法

**🎯 目標**: すべてのDatabricksユーザーが効率的なSQLパフォーマンス分析を実現すること
