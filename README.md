# Databricks SQL Profiler Analysis Tool

[English](#english) | [日本語](#japanese)

---

# English

**AI-Powered SQL Performance Analysis Tool for Databricks**

A comprehensive analysis tool that leverages AI (LLM) to analyze Databricks SQL Profiler JSON logs, identify bottlenecks, provide optimization recommendations, and generate optimized SQL queries.

## ✨ Key Features

### 🔍 **Advanced Performance Analysis**
- **Multi-Graph Support**: Analyzes all execution graphs in complex profiler data
- **Execution Plan Analysis**: Extracts detailed execution plan information from JSON metrics
- **30MB BROADCAST Threshold**: Accurate BROADCAST hint analysis with strict 30MB threshold enforcement
- **Existing Optimization Detection**: Identifies already applied BROADCAST optimizations
- **TOP10 Time-Consuming Processes**: Detailed analysis with full catalog.schema.table path display
- **Spill Detection**: Identifies memory pressure and disk spill issues
- **Data Skew Detection**: Detects task duration and shuffle read byte imbalances
- **Photon Engine Utilization**: Visualizes Photon engine usage patterns

### 🤖 **Multi-Provider AI Analysis**
- **Databricks Claude 3.7 Sonnet** (Recommended - 128K tokens)
- **OpenAI GPT-4/GPT-4 Turbo** (16K tokens)
- **Azure OpenAI** (16K tokens)
- **Anthropic Claude** (16K tokens)
- **Thinking Process Display** (thinking_enabled) for analysis transparency

### 🗂️ **LLM-Based Liquid Clustering Optimization**
- **Column Usage Pattern Analysis**: Extracts column patterns from profiler data
- **Automatic Filter/JOIN/GROUP BY Extraction**: Identifies clustering opportunities
- **Table-Specific Clustering Recommendations**: Per-table clustering column suggestions
- **Performance Impact Quantification**: Estimates performance improvement potential
- **File Output Support**: JSON, Markdown, and SQL implementation files

### 🚀 **Automated SQL Optimization**
- **Original Query Auto-Extraction**: Extracts queries from profiler data
- **AI-Driven Query Optimization**: Uses LLM for intelligent optimization
- **Execution Plan Consideration**: Incorporates execution plan information
- **Executable Optimized SQL Generation**: Produces ready-to-run SQL with semicolons
- **BROADCAST Analysis Integration**: Includes precise 30MB threshold analysis
- **Databricks Notebook Optimized**: Designed specifically for Databricks environments

### 📊 **Comprehensive Reporting**
- **Auto-Generated Files**: All files prefixed with "output_"
- **Multi-Language Support**: Japanese/English output files (OUTPUT_LANGUAGE setting)
- **Visual Dashboard Display**: Rich visualization in notebooks
- **Detailed Bottleneck Analysis Reports**: Comprehensive analysis documentation
- **Automatic TOP10 Analysis**: Included in all reports
- **Quantitative Performance Evaluation**: Data-driven improvement metrics
- **Metadata Filtering**: Automatic removal of unnecessary signatures and metadata

## 📁 File Structure

```
📦 Databricks SQL Profiler Analysis Tool
├── 📄 databricks_sql_profiler_analysis.py    # 🌟 Main Notebook (55 cells)
├── 📄 simple0.json                           # Sample SQL profiler file
├── 📄 README.md                              # This file
├── 📁 outputs/                               # Generated files (created at runtime)
│   ├── 📄 output_extracted_metrics_YYYYMMDD-HHMISS.json
│   ├── 📄 output_bottleneck_analysis_result_YYYYMMDD-HHMISS.txt
│   ├── 📄 output_original_query_YYYYMMDD-HHMISS.sql
│   ├── 📄 output_optimized_query_YYYYMMDD-HHMISS.sql
│   ├── 📄 output_optimization_report_YYYYMMDD-HHMISS.md
│   ├── 📄 liquid_clustering_analysis_YYYYMMDD-HHMISS.json
│   ├── 📄 liquid_clustering_analysis_YYYYMMDD-HHMISS.md
│   └── 📄 liquid_clustering_implementation_YYYYMMDD-HHMISS.sql
└── samples/                                  # Additional samples (optional)
    ├── 📄 largeplan.json
    ├── 📄 nophoton.json
    └── 📄 POC1.json
```

## 🚀 Quick Start

### Step 1: Create Notebook

1. Create a new **Notebook** in Databricks workspace
2. Set language to **Python**
3. Copy and paste content from `databricks_sql_profiler_analysis.py`

### Step 2: Basic Configuration

```python
# 📁 Analysis target file setting (Cell 4)
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'

# 🌐 Output language setting (Cell 4)
OUTPUT_LANGUAGE = 'en'  # 'en' = English, 'ja' = Japanese

# 🤖 LLM endpoint setting (Cell 6)
LLM_CONFIG = {
    "provider": "databricks",  # "databricks", "openai", "azure_openai", "anthropic"
    "thinking_enabled": False,  # Thinking process display (default: disabled for fast execution)
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens (Claude 3.7 Sonnet max limit)
        "temperature": 0.0,    # Deterministic output
        "thinking_budget_tokens": 65536  # 64K tokens (used only when thinking enabled)
    },
    "openai": {
        "api_key": "",  # OpenAI API key
        "model": "gpt-4o",
        "max_tokens": 16000,  # 16K tokens
        "temperature": 0.0    # Deterministic output
    }
}
```

### Step 3: Sequential Execution

```bash
🔧 Configuration & Preparation Section  → Execute cells 3-17
🚀 Main Processing Section             → Execute cells 18-40
🔧 SQL Optimization Section            → Execute cells 43-53 (Optional)
📚 Reference & Advanced Section        → See cell 55
```

## 🔧 Setup Details

### 1. LLM Endpoint Configuration

#### Databricks Claude 3.7 Sonnet (Recommended)

```bash
# Create with Databricks CLI
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

#### Other LLM Providers

```python
# OpenAI configuration (16K tokens)
LLM_CONFIG = {
    "provider": "openai",
    "thinking_enabled": False,
    "openai": {
        "api_key": "sk-...",  # or environment variable OPENAI_API_KEY
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0
    }
}

# Azure OpenAI configuration (16K tokens)
LLM_CONFIG = {
    "provider": "azure_openai",
    "thinking_enabled": False,
    "azure_openai": {
        "api_key": "your-azure-key",
        "endpoint": "https://your-resource.openai.azure.com/",
        "deployment_name": "gpt-4",
        "api_version": "2024-02-01",
        "max_tokens": 16000,
        "temperature": 0.0
    }
}
```

### 2. SQL Profiler File Setup

#### Get from Databricks SQL Editor

1. **Execute SQL query**
2. **Query History** → Select target query
3. **Query Profile** tab → **Download Profile JSON**

#### File Upload Methods

```python
# Method 1: Databricks UI
# Data → Create Table → Upload File → Drag & drop JSON file

# Method 2: dbutils
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# Method 3: Volumes (Recommended)
# Upload to Unity Catalog Volumes
```

## 🆕 Latest Features (v2.1)

### 🔍 **Execution Plan Information Analysis**
- **Plan Node Detection**: Identifies BROADCAST, JOIN, SCAN, SHUFFLE, and AGGREGATE nodes
- **Existing BROADCAST Detection**: Automatically detects already applied BROADCAST optimizations
- **JOIN Strategy Analysis**: Analyzes current JOIN strategies (broadcast_hash_join, sort_merge_join, etc.)
- **Table and File Format Identification**: Accurate extraction from execution plan metadata
- **Plan-Metrics Consistency**: Ensures consistency between execution plan and metrics data

### 🎯 **Enhanced 30MB BROADCAST Analysis**
- **Strict 30MB Threshold**: Enforces actual Spark configuration (spark.databricks.optimizer.autoBroadcastJoinThreshold)
- **Uncompressed Size Estimation**: Calculates based on file format-specific compression ratios
- **Status Classification**: Distinguishes between "already_applied" and "new_recommendation"
- **Safety Margin Analysis**: Strongly recommended (≤24MB), conditionally recommended (24-30MB)
- **Memory Impact Assessment**: Estimates worker node memory usage

### 🧬 **LLM-Based Liquid Clustering**
- **AI-Driven Analysis**: Replaces rule-based logic with LLM analysis
- **Column Usage Pattern Detection**: Identifies optimal clustering columns
- **Performance Impact Quantification**: Estimates scan time reduction potential
- **Multiple Output Formats**: JSON (detailed data), Markdown (readable report), SQL (implementation)
- **Table-Specific Recommendations**: Per-table analysis with confidence scoring

## 📊 Output Files

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
    "read_bytes": 123926013605,
    "photon_enabled": true,
    "photon_utilization_ratio": 0.85
  },
  "bottleneck_indicators": {
    "has_spill": true,
    "spill_bytes": 1073741824,
    "has_shuffle_bottleneck": true
  }
}
```

### 📄 output_optimization_report_YYYYMMDD-HHMISS.md

```markdown
# SQL Optimization Report

**Query ID**: 01f0565c-48f6-1283-a782-14ed6494eee0
**Optimization Time**: 2024-01-15 14:30:22

## BROADCAST Hint Analysis (30MB Threshold)

- **JOIN Query**: Yes
- **Spark BROADCAST Threshold**: 30.0MB (uncompressed)
- **BROADCAST Feasibility**: recommended
- **BROADCAST Candidates**: 2

### 30MB Threshold Hit Analysis
- **30MB Threshold Hit**: ✅ 2 tables qualify
- **Candidate Size Range**: 8.5MB - 24.1MB
- **Total Memory Impact**: 32.6MB will be broadcast to worker nodes
- **Optimal Candidate**: customer_dim (8.5MB) - smallest qualifying table

## Optimized SQL Query

```sql
WITH customer_filtered AS (
  SELECT customer_id, region, signup_date
  FROM /*+ BROADCAST(customer_dim) */ catalog.schema.customer_dim c
  WHERE region IN ('US', 'EU')
    AND signup_date >= '2023-01-01'
)
SELECT 
  c.customer_id,
  c.region,
  SUM(o.amount) as total_amount
FROM customer_filtered c
LEFT JOIN catalog.schema.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.region
ORDER BY total_amount DESC;
```

## BROADCAST Application Rationale (30MB Threshold)
- 📏 Spark Threshold: 30MB (uncompressed, spark.databricks.optimizer.autoBroadcastJoinThreshold)
- 🎯 Applied Table: customer_dim
  - Uncompressed Estimated Size: 8.5MB
  - Compressed Estimated Size: 2.1MB
  - Estimated Compression Ratio: 4.0x
  - File Format: delta
  - Estimation Basis: Row count and data read volume based
- ⚖️ Decision Result: strongly_recommended
- 🔍 Threshold Compliance: Fits within 30MB threshold
- 💾 Memory Impact: 8.5MB broadcast to worker nodes
- 🚀 Expected Effect: Network transfer reduction, JOIN processing acceleration, shuffle reduction

## Expected Performance Improvement
- **Execution Time**: 84.2s → 35-45s (50% reduction)
- **Memory Usage**: Spill elimination, 30% reduction
- **Spill Reduction**: 1GB → 0GB (100% elimination)
```

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. LLM Timeout Errors
```
❌ ⏰ Timeout Error: Databricks endpoint response did not complete within 300 seconds.
```

**Solution**:
- Timeout extended: 180s → **300s (5 minutes)**
- Retry attempts increased: 2 → **3 times**
- Prompt optimization: 60% size reduction
- Token limit optimization for Claude 3.7 Sonnet (128K)

#### 2. Incomplete SQL Generation
```sql
-- Problem: Column names or table names are omitted
SELECT 
 r_uid,
 ref_domain
 FROM
 `r-data-genesis`.tmp_cbo.
 -- [truncated]
```

**Solution**:
✅ **Enhanced Completeness Check**: Strict constraints added to prompts
- Complete prohibition of omissions and placeholders
- Explicit requirement to preserve all SELECT items
- Step-by-step construction with thinking functionality
- Retention of detailed analysis information up to 5000 characters

#### 3. BROADCAST Analysis Accuracy
```
Problem: Incorrect BROADCAST recommendations for tables >30MB
```

**Solution**:
✅ **Execution Plan Integration**: Enhanced accuracy with plan information
- Automatic detection of existing BROADCAST applications
- Precise table name and file format identification from execution plans
- Clear distinction between already optimized and new recommendations
- Strict enforcement of 30MB threshold with actual Spark configuration

## 📈 Performance Improvements (v2.1)

### Before vs After Comparison

| Feature | Before | After | Improvement |
|---------|--------|-------|-------------|
| **Graph Analysis** | Single graph | **All graphs** | Complete coverage |
| **BROADCAST Detection** | Metrics only | **Plan + Metrics** | 95% accuracy |
| **Column Extraction** | Rule-based | **LLM-based** | Intelligent analysis |
| **30MB Threshold** | Estimated | **Strict enforcement** | Precise compliance |
| **Language Support** | Japanese only | **EN/JA** | Global usage |
| **Execution Plan** | Not used | **Fully integrated** | Reality-based analysis |

### Expected Results
- ✅ Complex queries (37+ columns) fully supported
- ✅ POC1.json multi-graph scenarios handled correctly
- ✅ Elimination of duplicate BROADCAST recommendations
- ✅ Accurate file format and compression ratio analysis
- ✅ Reality-based optimization suggestions

---

# Japanese

# Databricks SQLプロファイラー分析ツール

**最先端のAI駆動SQLパフォーマンス分析ツール**

DatabricksのSQLプロファイラーJSONログファイルを読み込み、AI（LLM）を活用してボトルネック特定・改善案提示・SQL最適化を行う包括的な分析ツールです。

## ✨ 主要機能

### 🔍 **高度なパフォーマンス分析**
- **複数グラフ対応**: 複雑なプロファイラーデータの全実行グラフを解析
- **実行プラン分析**: JSONメトリクスから詳細な実行プラン情報を抽出
- **30MB BROADCAST閾値**: 厳格な30MB閾値でのBROADCASTヒント分析
- **既存最適化検出**: 既に適用済みのBROADCAST最適化を識別
- **時間消費TOP10プロセス**: カタログ.スキーマ.テーブルのフルパス表示で詳細分析
- **スピル検出**: メモリ圧迫とディスクスピル問題の特定
- **データスキュー検出**: タスク実行時間とシャッフル読み込み量の不均衡検出
- **Photonエンジン利用状況**: Photonエンジンの使用パターンを可視化

### 🤖 **マルチプロバイダーAI分析**
- **Databricks Claude 3.7 Sonnet**（推奨・128K tokens）
- **OpenAI GPT-4/GPT-4 Turbo**（16K tokens）
- **Azure OpenAI**（16K tokens）
- **Anthropic Claude**（16K tokens）
- **思考プロセス表示機能**（thinking_enabled）で分析過程を可視化

### 🗂️ **LLMベースLiquid Clustering最適化**
- **カラム使用パターン分析**: プロファイラーデータからカラムパターンを抽出
- **自動フィルター・JOIN・GROUP BY抽出**: クラスタリング機会を特定
- **テーブル別クラスタリング推奨**: テーブル毎のクラスタリングカラム提案
- **パフォーマンス向上定量評価**: 性能改善ポテンシャルの見積もり
- **ファイル出力サポート**: JSON、Markdown、SQL実装ファイル

### 🚀 **自動SQL最適化**
- **オリジナルクエリ自動抽出**: プロファイラーデータからクエリを抽出
- **AI駆動クエリ最適化**: LLMによるインテリジェント最適化
- **実行プラン考慮**: 実行プラン情報を組み込み
- **実行可能な最適化SQL生成**: セミコロン付きですぐ実行可能なSQLを生成
- **BROADCAST分析統合**: 精密な30MB閾値分析を含む
- **Databricks Notebook最適化**: Databricks環境専用設計

### 📊 **包括的レポーティング**
- **自動生成ファイル**: すべてのファイルに"output_"接頭語
- **多言語対応**: 日本語・英語出力ファイル（OUTPUT_LANGUAGE設定）
- **視覚的ダッシュボード表示**: ノートブック内でのリッチな可視化
- **詳細ボトルネック分析レポート**: 包括的な分析ドキュメント
- **自動TOP10分析**: すべてのレポートに含まれる
- **定量的パフォーマンス評価**: データ駆動型の改善指標
- **メタデータフィルタリング**: 不要なシグネチャやメタデータの自動除去

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
│   ├── 📄 output_optimization_report_YYYYMMDD-HHMISS.md
│   ├── 📄 liquid_clustering_analysis_YYYYMMDD-HHMISS.json
│   ├── 📄 liquid_clustering_analysis_YYYYMMDD-HHMISS.md
│   └── 📄 liquid_clustering_implementation_YYYYMMDD-HHMISS.sql
└── samples/                                  # 追加サンプル（オプション）
    ├── 📄 largeplan.json
    ├── 📄 nophoton.json
    └── 📄 POC1.json
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

# 🌐 出力言語設定（セル4）
OUTPUT_LANGUAGE = 'ja'  # 'ja' = 日本語, 'en' = 英語

# 🤖 LLMエンドポイント設定（セル6）
LLM_CONFIG = {
    "provider": "databricks",  # "databricks", "openai", "azure_openai", "anthropic"
    "thinking_enabled": False,  # 思考プロセス表示（デフォルト: 無効・高速実行）
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens（Claude 3.7 Sonnet最大制限）
        "temperature": 0.0,    # 決定的な出力
        "thinking_budget_tokens": 65536  # 64K tokens（thinking有効時のみ使用）
    },
    "openai": {
        "api_key": "",  # OpenAI APIキー
        "model": "gpt-4o",
        "max_tokens": 16000,  # 16K tokens
        "temperature": 0.0    # 決定的な出力
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

## 🆕 最新機能（v2.1）

### 🔍 **実行プラン情報分析**
- **プランノード検出**: BROADCAST、JOIN、SCAN、SHUFFLE、AGGREGATEノードを識別
- **既存BROADCAST検出**: 既に適用済みのBROADCAST最適化を自動検出
- **JOIN戦略分析**: 現在のJOIN戦略を分析（broadcast_hash_join、sort_merge_join等）
- **テーブル・ファイル形式特定**: 実行プランメタデータから正確に抽出
- **プラン・メトリクス整合性**: 実行プランとメトリクスデータの一貫性を確保

### 🎯 **強化された30MB BROADCAST分析**
- **厳格な30MB閾値**: 実際のSpark設定を強制適用（spark.databricks.optimizer.autoBroadcastJoinThreshold）
- **非圧縮サイズ推定**: ファイル形式別圧縮率に基づく計算
- **ステータス分類**: "already_applied"（適用済み）と"new_recommendation"（新規推奨）を区別
- **安全マージン分析**: 強く推奨（≤24MB）、条件付き推奨（24-30MB）
- **メモリ影響評価**: ワーカーノードのメモリ使用量を推定

### 🧬 **LLMベースLiquid Clustering**
- **AI駆動分析**: ルールベースロジックをLLM分析に置き換え
- **カラム使用パターン検出**: 最適なクラスタリングカラムを特定
- **パフォーマンス影響定量化**: スキャン時間削減ポテンシャルを推定
- **複数出力形式**: JSON（詳細データ）、Markdown（読みやすいレポート）、SQL（実装）
- **テーブル別推奨**: テーブル毎の分析と信頼度スコアリング

## 📊 出力ファイル詳細

### 📄 output_optimization_report_YYYYMMDD-HHMISS.md

```markdown
# SQL最適化レポート

**クエリID**: 01f0565c-48f6-1283-a782-14ed6494eee0
**最適化日時**: 2024-01-15 14:30:22

## BROADCASTヒント分析結果（30MB閾値基準）

- **JOINクエリ**: はい
- **Spark BROADCAST閾値**: 30.0MB（非圧縮）
- **BROADCAST適用可能性**: recommended
- **BROADCAST候補数**: 2個

### 30MB閾値ヒット分析
- **30MB閾値ヒット**: ✅ 2個のテーブルが適合
- **候補サイズ範囲**: 8.5MB - 24.1MB
- **総メモリ影響**: 32.6MB がワーカーノードにブロードキャスト
- **最適候補**: customer_dim (8.5MB) - 最小適格テーブル

## 最適化されたSQLクエリ

```sql
WITH customer_filtered AS (
  SELECT customer_id, region, signup_date
  FROM /*+ BROADCAST(customer_dim) */ catalog.schema.customer_dim c
  WHERE region IN ('US', 'EU')
    AND signup_date >= '2023-01-01'
)
SELECT 
  c.customer_id,
  c.region,
  SUM(o.amount) as total_amount
FROM customer_filtered c
LEFT JOIN catalog.schema.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.region
ORDER BY total_amount DESC;
```

## BROADCAST適用根拠（30MB閾値基準）
- 📏 Spark閾値: 30MB（非圧縮、spark.databricks.optimizer.autoBroadcastJoinThreshold）
- 🎯 適用テーブル: customer_dim
  - 非圧縮推定サイズ: 8.5MB
  - 圧縮推定サイズ: 2.1MB
  - 推定圧縮率: 4.0x
  - ファイル形式: delta
  - 推定根拠: 行数・データ読み込み量ベース
- ⚖️ 判定結果: strongly_recommended
- 🔍 閾値適合性: 30MB以下で適合
- 💾 メモリ影響: 8.5MB がワーカーノードにブロードキャスト
- 🚀 期待効果: ネットワーク転送量削減・JOIN処理高速化・シャッフル削減

## 期待効果
- **実行時間**: 84.2秒 → 35-45秒（50%削減）
- **メモリ使用量**: スピル解消により30%削減
- **スピル削減**: 1GB → 0GB（100%削減）
```

## 🛠️ トラブルシューティング

### よくある問題と解決方法

#### 1. LLMタイムアウトエラー
```
❌ ⏰ タイムアウトエラー: Databricksエンドポイントの応答が300秒以内に完了しませんでした。
```

**解決方法**:
- タイムアウト延長: 180秒 → **300秒（5分）**
- リトライ回数増加: 2回 → **3回**
- プロンプト最適化: サイズ60%削減
- Claude 3.7 Sonnet向けトークン制限最適化（128K）

#### 2. 不完全なSQL生成
```sql
-- 問題: カラム名やテーブル名が省略される
SELECT 
 r_uid,
 ref_domain
 FROM
 `r-data-genesis`.tmp_cbo.
 -- [以下省略]
```

**解決方法**:
✅ **強化された完全性チェック**: プロンプトに厳格な制約を追加
- 省略・プレースホルダー使用を完全禁止
- すべてのSELECT項目の保持を明示的に要求
- thinking機能でステップバイステップ構築
- 5000文字までの詳細分析情報を保持

#### 3. BROADCAST分析精度
```
問題: 30MB超のテーブルに誤ったBROADCAST推奨
```

**解決方法**:
✅ **実行プラン統合**: プラン情報による精度向上
- 既存BROADCAST適用の自動検出
- 実行プランからの正確なテーブル名・ファイル形式特定
- 既に最適化済みと新規推奨の明確な区別
- 実際のSpark設定による30MB閾値の厳格な強制適用

## 📈 パフォーマンス改善（v2.1）

### 改善前後の比較

| 機能 | 改善前 | 改善後 | 向上内容 |
|---------|--------|-------|-------------|
| **グラフ分析** | 単一グラフ | **全グラフ** | 完全網羅 |
| **BROADCAST検出** | メトリクスのみ | **プラン+メトリクス** | 95%精度 |
| **カラム抽出** | ルールベース | **LLMベース** | インテリジェント分析 |
| **30MB閾値** | 推定値 | **厳格適用** | 正確な遵守 |
| **言語サポート** | 日本語のみ | **日英対応** | グローバル対応 |
| **実行プラン** | 未使用 | **完全統合** | 実態ベース分析 |

### 期待される結果
- ✅ 複雑なクエリ（37カラム以上）完全サポート
- ✅ POC1.json複数グラフシナリオの正常処理
- ✅ 重複BROADCAST推奨の排除
- ✅ 正確なファイル形式・圧縮率分析
- ✅ 実態に基づく最適化提案

---

## 📞 サポート・コミュニティ

- **GitHub Issues**: バグレポート・機能要望
- **Databricks Community**: 使用方法・ベストプラクティス
- **技術ブログ**: 詳細な使用例・カスタマイズ方法

**🎯 目標**: すべてのDatabricksユーザーが効率的なSQLパフォーマンス分析を実現すること
