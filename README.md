# Databricks SQLプロファイラー分析ツール

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-orange.svg)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> DatabricksのSQLプロファイラーログファイルを分析し、AIを活用してボトルネックを特定し、具体的な改善案を提示するツールです。

## 📋 目次

- [概要](#概要)
- [主要機能](#主要機能)
- [要件](#要件)
- [セットアップ](#セットアップ)
- [使用方法](#使用方法)
- [設定オプション](#設定オプション)
- [出力例](#出力例)
- [トラブルシューティング](#トラブルシューティング)
- [ライセンス](#ライセンス)

## 🎯 概要

このツールは、DatabricksのSQLプロファイラーが出力するJSONログファイルを解析し、以下の機能を提供します：

- **パフォーマンスメトリクス抽出**: 実行時間、データ量、キャッシュ効率などの詳細分析
- **AIによるボトルネック分析**: 複数のLLMプロバイダーを使用したインテリジェントな分析
- **Liquid Clustering推奨**: テーブル最適化のための具体的な実装コード生成
- **BROADCAST分析**: 実行プランからのテーブルサイズ推定とJOIN最適化
- **SQLクエリ最適化**: 元のクエリの改善版を自動生成

## ✨ 主要機能

### 🔍 包括的な分析機能
- **実行プラン分析**: Spark実行プランの詳細解析
- **Photonエンジン分析**: Photon利用状況と最適化提案
- **並列度・シャッフル分析**: 処理効率の詳細評価
- **メモリスピル検出**: メモリ使用量の問題特定

### 🤖 AI駆動の分析
- **マルチLLMサポート**: Databricks、OpenAI、Azure OpenAI、Anthropic対応
- **日本語・英語対応**: 分析結果の多言語出力
- **コンテキスト分析**: 実行環境を考慮した最適化提案

### 📊 高度な最適化機能
- **Liquid Clustering**: Databricks SQL準拠の正しい構文での実装
- **BROADCAST最適化**: 既存の最適化状況を考慮した推奨
- **クエリ最適化**: 元のSQLクエリの改善版生成

## 📋 要件

### 基本要件
- Python 3.8以上
- Databricks Runtime 10.4以上
- DatabricksのSQLプロファイラーJSONファイル

### 依存関係
```python
import json
import pandas as pd
import requests
from typing import Dict, List, Any
from datetime import datetime
```

### LLMプロバイダー（いずれか一つ）
- **Databricks Model Serving**: 推奨（高速）
- **OpenAI API**: GPT-4o、GPT-4-turbo対応
- **Azure OpenAI**: エンタープライズ利用
- **Anthropic API**: Claude-3.5-sonnet対応

## 🚀 セットアップ

### 1. ファイルのアップロード
DatabricksのFileStoreまたはVolumesにファイルをアップロードします：

```python
# FileStoreを使用（推奨）
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# Unity Catalog Volumesを使用
dbutils.fs.cp("file:/local/path/profiler.json", "/Volumes/catalog/schema/volume/profiler.json")
```

### 2. LLMエンドポイントの設定

#### Databricks Model Serving（推奨）
```python
LLM_CONFIG = {
    "provider": "databricks",
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,
        "temperature": 0.0
    }
}
```

#### OpenAI API
```python
LLM_CONFIG = {
    "provider": "openai",
    "openai": {
        "api_key": "your-openai-api-key",
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0
    }
}
```

### 3. 基本設定
```python
# 分析対象ファイルのパス
JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler.json'

# 出力言語設定
OUTPUT_LANGUAGE = 'ja'  # 'ja' = 日本語, 'en' = 英語
```

## 💻 使用方法

### 基本的な使用方法

1. **JSONファイルの読み込み**
```python
profiler_data = load_profiler_json(JSON_FILE_PATH)
```

2. **メトリクス抽出**
```python
metrics = extract_performance_metrics(profiler_data)
```

3. **AIによる分析**
```python
analysis_result = analyze_bottlenecks_with_llm(metrics)
```

4. **最適化クエリ生成**
```python
original_query = extract_original_query_from_profiler_data(profiler_data)
optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, metrics)
```

### 完全な分析フロー

```python
# 1. データ読み込み
profiler_data = load_profiler_json(JSON_FILE_PATH)
extracted_metrics = extract_performance_metrics(profiler_data)

# 2. ボトルネック分析
analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)

# 3. Liquid Clustering分析
clustering_analysis = analyze_liquid_clustering_opportunities(profiler_data, extracted_metrics)

# 4. BROADCAST分析
original_query = extract_original_query_from_profiler_data(profiler_data)
plan_info = extract_execution_plan_info(profiler_data)
broadcast_analysis = analyze_broadcast_feasibility(extracted_metrics, original_query, plan_info)

# 5. 最適化クエリ生成
optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, extracted_metrics)

# 6. レポート生成
save_optimized_sql_files(original_query, optimized_query, extracted_metrics)
```

## ⚙️ 設定オプション

### LLMプロバイダー設定

| プロバイダー | 設定キー | 説明 |
|-------------|----------|------|
| Databricks | `endpoint_name` | Model Servingエンドポイント名 |
| OpenAI | `api_key`, `model` | APIキーとモデル名 |
| Azure OpenAI | `api_key`, `endpoint`, `deployment_name` | Azure固有の設定 |
| Anthropic | `api_key`, `model` | AnthropicのAPIキーとモデル |

### 分析オプション

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `OUTPUT_LANGUAGE` | 'ja' | 出力言語（'ja'または'en'） |
| `max_tokens` | 131072 | LLMの最大トークン数 |
| `temperature` | 0.0 | LLMの出力ランダム性 |

## 📊 出力例

### ボトルネック分析結果
```
🔧 **Databricks SQLプロファイラー ボトルネック分析結果**

## 📊 パフォーマンス概要
- **実行時間**: 45.2秒
- **読み込みデータ量**: 2.1GB
- **キャッシュ効率**: 85.3%
- **データ選択性**: 12.4%

## ⚡ Photonエンジン分析
- **Photon有効**: はい
- **Photon利用率**: 92.1%
- **推奨**: 最適化済み

## 🗂️ Liquid Clustering推奨事項
**対象テーブル**: 3個

**推奨実装**:
- orders テーブル: ALTER TABLE orders CLUSTER BY (customer_id, order_date)
- customers テーブル: ALTER TABLE customers CLUSTER BY (region, customer_type)
```

### 最適化されたSQLクエリ
```sql
-- 最適化前
SELECT customer_id, SUM(amount) 
FROM orders 
WHERE order_date >= '2023-01-01' 
GROUP BY customer_id;

-- 最適化後
SELECT /*+ BROADCAST(c) */ 
    o.customer_id, 
    SUM(o.amount) as total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2023-01-01'
    AND c.status = 'active'
GROUP BY o.customer_id
ORDER BY total_amount DESC;
```

## 🛠️ トラブルシューティング

### よくある問題と解決方法

#### 1. LLMエンドポイントエラー
```
❌ 分析エラー: Connection timeout
```
**解決方法**:
- Databricks: Model Servingエンドポイントの状態確認
- OpenAI/Azure/Anthropic: APIキーとクォータ確認
- ネットワーク接続の確認

#### 2. ファイル読み込みエラー
```
❌ ファイル読み込みエラー: FileNotFoundError
```
**解決方法**:
```python
# ファイル存在確認
dbutils.fs.ls("/FileStore/shared_uploads/")

# パス形式の確認
# 正しい: '/FileStore/shared_uploads/username/file.json'
# 正しい: '/Volumes/catalog/schema/volume/file.json'
```

#### 3. メモリエラー
```
❌ OutOfMemoryError: Java heap space
```
**解決方法**:
- クラスタのメモリ設定を増加
- より大きなインスタンスタイプを使用
- 複数のプロファイラーファイルを分割処理

#### 4. 多言語文字化け
```
❌ UnicodeDecodeError
```
**解決方法**:
```python
# UTF-8エンコーディングの確認
with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
```

## 📈 パフォーマンス最適化のヒント

### 1. LLMプロバイダーの選択
- **高速処理**: Databricks Model Serving
- **高品質分析**: OpenAI GPT-4o
- **エンタープライズ**: Azure OpenAI
- **コスト効率**: Anthropic Claude

### 2. 大容量ファイルの処理
```python
# ファイルサイズ確認
import os
file_size = os.path.getsize(JSON_FILE_PATH)
print(f"ファイルサイズ: {file_size / 1024 / 1024:.1f}MB")

# 大容量ファイルの場合は分割処理を検討
```

### 3. 並列処理
```python
# 複数ファイルの並列処理
from concurrent.futures import ThreadPoolExecutor

profiler_files = [file1, file2, file3]
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(analyze_single_file, profiler_files))
```

## 🔧 カスタマイズ

### 分析メトリクスの追加
```python
def extract_performance_metrics(profiler_data):
    # カスタムメトリクスの追加
    metrics["custom_indicators"] = {
        "custom_metric_1": calculate_custom_metric_1(profiler_data),
        "custom_metric_2": calculate_custom_metric_2(profiler_data)
    }
    return metrics
```

### 新しいLLMプロバイダーの追加
```python
def _call_custom_llm(prompt: str) -> str:
    # カスタムLLMプロバイダーの実装
    pass

# LLM_CONFIGに追加
LLM_CONFIG["custom_provider"] = {
    "api_key": "your-api-key",
    "endpoint": "your-endpoint"
}
```

## 📝 ライセンス

MIT License

## 🤝 コントリビューション

1. フォークする
2. 機能ブランチを作成 (`git checkout -b feature/new-feature`)
3. コミット (`git commit -am 'Add new feature'`)
4. プッシュ (`git push origin feature/new-feature`)
5. プルリクエストを作成

## 📞 サポート

- Issues: [GitHub Issues](https://github.com/your-username/databricks-sql-profiler-analysis/issues)
- ドキュメント: [Wiki](https://github.com/your-username/databricks-sql-profiler-analysis/wiki)
- メール: support@example.com

---

**注意**: このツールは分析と推奨事項の提供のみを行います。本番環境での実行前に、必ず推奨事項を検証してください。
