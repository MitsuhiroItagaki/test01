# Databricks SQLプロファイラー分析ツール

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-orange.svg)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> DatabricksのSQLプロファイラーログファイルを分析し、AIを活用してボトルネックを特定し、具体的な改善案を提示するツールです。

## 📋 目次

- [概要](#概要)
- [主要機能](#主要機能)
- [最新の機能強化](#最新の機能強化)
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
- **自動レポート精製**: 生成されたレポートの自動的な可読性向上

## ✨ 主要機能

### 🔍 包括的な分析機能
- **実行プラン分析**: Spark実行プランの詳細解析
- **Photonエンジン分析**: Photon利用状況と最適化提案（目標90%以上）
- **並列度・シャッフル分析**: 処理効率の詳細評価
- **メモリスピル検出**: メモリ使用量の問題特定とGB単位での定量化

### 🤖 AI駆動の分析
- **マルチLLMサポート**: Databricks、OpenAI、Azure OpenAI、Anthropic対応
- **日本語・英語対応**: 分析結果の多言語出力
- **コンテキスト分析**: 実行環境を考慮した最適化提案

### 📊 高度な最適化機能
- **Liquid Clustering**: Databricks SQL準拠の正しい構文での実装
- **BROADCAST最適化**: 既存の最適化状況を考慮した推奨
- **クエリ最適化**: 元のSQLクエリの改善版生成
- **フィルタリング率計算**: 各ノードの処理効率詳細分析

## 🚀 最新の機能強化

### 📈 セル47: 包括的なボトルネック分析
- **統合データ分析**: TOP10時間消費処理、Liquid Clustering分析、SQL最適化実行の3つのセクションを統合
- **優先度付きレポート**: HIGH/MEDIUM/LOW優先度によるアクション分類
- **定量的な改善予測**: 最大80%の実行時間短縮の定量的予測
- **PHOTONエンジン最適化**: 目標90%以上の利用率達成のための具体的推奨

### 🎯 セル48: レポート自動精製
- **自動レポート検出**: 最新の`output_optimization_report_*.md`ファイルの自動検出
- **LLMによる精製**: 「このレポートをもっと読みやすく、簡潔にしてください」プロンプトによる改善
- **自動ファイル管理**: 元ファイルの削除と精製版の自動リネーム
- **エラーハンドリング**: 包括的なエラー処理とプレビュー機能

### 🔧 Liquid Clustering強化
- **Where条件書き換え**: フィルタリング条件の最適化を含む検討実施
- **クラスタリングキー抽出**: JOIN、GROUP BY、WHERE条件に基づく最適なキー選択
- **優先度付き推奨**: HIGH/MEDIUM/LOW優先度による実装順序の明確化
- **SQL実装例**: 具体的なCLUSTER BY構文での実装コード生成

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
        "api_key": "your-api-key",
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0
    }
}
```

### 3. 基本設定
```python
# 分析対象ファイルのパス設定
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/POC1.json'

# 出力言語設定
OUTPUT_LANGUAGE = 'ja'  # 'ja' = 日本語, 'en' = 英語
```

## 📊 使用方法

### 基本的な分析フロー

1. **セル1-32**: 基本設定と分析関数の定義
2. **セル33**: TOP10時間消費処理の分析
3. **セル35**: Liquid Clustering機会の分析
4. **セル47**: 包括的なボトルネック分析（統合レポート生成）
5. **セル48**: レポートの自動精製と可読性向上

### 実行例
```python
# 基本分析の実行
profiler_data = load_profiler_json(JSON_FILE_PATH)
extracted_metrics = extract_performance_metrics(profiler_data)

# TOP10時間消費処理分析
top10_report = generate_top10_time_consuming_processes_report(extracted_metrics)

# Liquid Clustering分析
clustering_analysis = analyze_liquid_clustering_opportunities(profiler_data, extracted_metrics)

# 包括的なボトルネック分析
bottleneck_analysis = analyze_bottlenecks_with_llm(extracted_metrics)

# レポートの自動精製
refined_report = refine_report_content_with_llm(bottleneck_analysis)
```

## 🎯 出力例

### 包括的分析レポート
```markdown
# Databricks SQLプロファイラー ボトルネック分析結果

## 1. パフォーマンス概要
- 実行時間: 45.67秒
- データ読み込み: 1.2GB
- キャッシュ効率: 78%
- データ選択性: 45%

## 2. 主要ボトルネック分析（Photon、並列度、シャッフルに焦点）
- **Photonエンジン**: 利用率65% → 目標90%以上に向けた最適化が必要
- **並列度**: 128タスク → 256タスクへの増加を推奨
- **シャッフル**: 2.3GB検出 → BROADCAST JOINでの最適化が必要

## 3. TOP5処理時間ボトルネック
1. **CRITICAL**: FileScan処理 (25.2秒)
2. **HIGH**: ShuffleExchange処理 (12.4秒)
3. **MEDIUM**: HashAggregate処理 (5.8秒)
```

### Liquid Clustering推奨
```sql
-- HIGH優先度: user_transactions テーブル
ALTER TABLE user_transactions 
CLUSTER BY (user_id, transaction_date);

-- MEDIUM優先度: product_sales テーブル
ALTER TABLE product_sales 
CLUSTER BY (product_id, sales_date);
```

## 🔧 設定オプション

### 高度な設定
```python
# Photon最適化設定
PHOTON_CONFIG = {
    "target_utilization": 0.9,  # 目標利用率90%
    "enable_vectorized_execution": True,
    "optimize_shuffle_partitions": True
}

# Liquid Clustering設定
CLUSTERING_CONFIG = {
    "analyze_where_conditions": True,
    "include_join_keys": True,
    "priority_threshold": 0.8
}

# レポート精製設定
REFINEMENT_CONFIG = {
    "auto_cleanup": True,
    "preserve_original": False,
    "max_refinement_attempts": 3
}
```

## 📈 パフォーマンス改善例

### 改善前後の比較
- **実行時間**: 45.67秒 → 12.34秒（73%改善）
- **Photon利用率**: 65% → 92%（目標達成）
- **メモリスピル**: 2.3GB → 0GB（完全解消）
- **シャッフル量**: 1.8GB → 0.5GB（72%削減）

## 🛠️ トラブルシューティング

### よくある問題

1. **LLMエンドポイントエラー**
   - エンドポイント名とAPIキーを確認
   - ネットワーク接続を確認

2. **メモリ不足エラー**
   - より小さなデータセットでテスト
   - クラスター設定を見直し

3. **レポート生成エラー**
   - 入力データの形式を確認
   - ログファイルでエラー詳細を確認

## 📄 ライセンス

MIT License

## 🤝 貢献

プルリクエストや課題報告を歓迎します。詳細は[CONTRIBUTING.md](CONTRIBUTING.md)をご覧ください。

## 📞 サポート

質問やサポートが必要な場合は、[Issues](https://github.com/your-repo/issues)で報告してください。
