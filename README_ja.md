# Databricks SQL プロファイラー分析ツール

Databricks SQL クエリプロファイルJSONファイルを詳細分析し、パフォーマンスボトルネックの特定とSQL最適化提案を自動生成するツールです。

## 🚀 主要機能

### 📊 **パフォーマンス分析**
- JSONプロファイルから詳細メトリクス抽出
- ボトルネック自動検出（スピル、シャッフル、キャッシュ効率等）
- TOP10時間消費プロセス分析

### 🧠 **LLMベース分析**
- 複数LLMプロバイダー対応（Databricks、OpenAI、Azure OpenAI、Anthropic）
- 自動ボトルネック診断と最適化提案
- 日本語・英語対応

### 📄 **実行プラン詳細分析**（新機能）
- 実行プランからの正確なテーブルサイズ推定（`estimatedSizeInBytes`活用）
- BROADCAST、JOIN、シャッフル、集約ノードの詳細分析
- プラン構造の可視化とMarkdownレポート生成

### 🎯 **高精度BROADCAST分析**（強化）
- Sparkエンジンの実推定値を優先活用
- 30MB閾値での正確な判定
- 既存BROADCAST適用状況の自動検出
- サイズ推定信頼度の明示

### 🔧 **SQL最適化**
- オリジナルクエリの自動抽出
- LLMベースのクエリ最適化
- 実行可能なSQL出力

### 💾 **包括的ファイル出力**
- パフォーマンス分析JSONファイル
- 実行プラン分析JSONファイル（新機能）
- 詳細Markdownレポート（実行プラン情報含む）
- 最適化SQLファイル
- すべてタイムスタンプ付きで`output_`接頭語

## 📁 出力ファイル一覧

| ファイル | 形式 | 説明 |
|---------|-----|------|
| `output_performance_analysis_YYYYMMDD-HHMMSS.json` | JSON | パフォーマンスメトリクス詳細 |
| `output_execution_plan_analysis_YYYYMMDD-HHMMSS.json` | JSON | **実行プラン構造とサイズ推定** |
| `output_execution_plan_report_YYYYMMDD-HHMMSS.md` | Markdown | **実行プラン詳細レポート** |
| `output_bottleneck_analysis_YYYYMMDD-HHMMSS.md` | Markdown | LLMボトルネック分析レポート |
| `output_original_query_YYYYMMDD-HHMMSS.sql` | SQL | 抽出されたオリジナルクエリ |
| `output_optimized_query_YYYYMMDD-HHMMSS.sql` | SQL | LLM最適化クエリ |
| `output_optimization_report_YYYYMMDD-HHMMSS.md` | Markdown | 最適化レポート（BROADCAST分析含む） |

## 🔬 新機能詳細

### 📏 **実行プランからのテーブルサイズ推定**
```json
{
  "physicalPlan": {
    "nodes": [
      {
        "nodeName": "Scan Delta table_name",
        "metrics": {
          "estimatedSizeInBytes": 10485760,  // 10MB
          "numFiles": 5,
          "numPartitions": 2
        }
      }
    ]
  }
}
```

**利点:**
- ✅ Sparkエンジンの実推定値を直接活用（高精度）
- ✅ フィルタリング後のサイズも反映
- ✅ ファイル数・パーティション数も同時取得

### 🎯 **高精度BROADCAST分析**

**推定精度の向上:**
- **従来**: メトリクスベース間接推定（信頼度: medium）
- **新機能**: 実行プラン`estimatedSizeInBytes`（信頼度: high）

**分析例:**
```
🔹 BROADCAST(orders_table) - 非圧縮15.2MB（安全閾値 24.0MB 以下）でBROADCAST強く推奨
（execution_plan_estimateベース、信頼度: high）
```

### 📊 **実行プラン分析レポート**
- 📊 実行プランサマリー（総ノード数、JOIN戦略等）
- 📡 BROADCASTノード詳細
- 🔗 JOINノード詳細
- 📋 テーブルスキャン詳細（サイズ推定含む）
- 📏 **テーブルサイズ推定情報**（新セクション）
- 💡 プランベース最適化推奨事項

## 🛠 使用方法

### 1. **LLMプロバイダー設定**
```python
# Databricks Model Serving
LLM_CONFIG = {
    'provider': 'databricks',
    'databricks': {
        'endpoint_name': 'your-endpoint-name'
    }
}

# OpenAI
LLM_CONFIG = {
    'provider': 'openai',
    'openai': {
        'api_key': 'your-api-key',
        'model': 'gpt-4'
    }
}
```

### 2. **プロファイルファイル配置**
```bash
# ファイルアップロード先
/FileStore/shared_uploads/your-email/profiler_output.json
```

### 3. **分析実行**
```python
# Databricks Notebookで実行
# セル1-22を順次実行するだけ
```

### 4. **結果確認**
- 生成されたファイルは全て`output_`接頭語付き
- タイムスタンプで複数実行を区別
- JSON、SQL、Markdownファイルが自動生成

## 📈 分析レポート例

### **実行プラン分析レポート**（新機能）
```markdown
## 📏 テーブルサイズ推定情報（実行プランベース）

- **推定対象テーブル数**: 3
- **総推定サイズ**: 125.5MB

### orders
- **推定サイズ**: 15.2MB
- **信頼度**: high
- **ファイル数**: 5

### customers  
- **推定サイズ**: 45.8MB
- **信頼度**: high
- **ファイル数**: 12

## 💡 実行プランベースBROADCAST推奨

- 30MB以下の小テーブル: 1個検出
  • orders: 15.2MB（BROADCAST候補）
```

### **BROADCAST分析結果**（強化）
```markdown
## BROADCASTヒント分析結果（30MB閾値基準）

- **JOINクエリ**: はい
- **Spark BROADCAST閾値**: 30.0MB（非圧縮）
- **BROADCAST適用可能性**: recommended
- **BROADCAST候補数**: 1個

### BROADCAST候補テーブル（詳細分析）

🔹 **orders**
  - **非圧縮サイズ**: 15.2MB
  - **圧縮サイズ**: 3.8MB
  - **圧縮率**: 4.0x
  - **ファイル形式**: delta
  - **行数**: 50,000行
  - **信頼度**: high
  - **根拠**: 非圧縮推定サイズ 15.2MB（安全閾値 24.0MB 以下）でBROADCAST強く推奨（execution_plan_estimateベース、信頼度: high）
```

## 🔧 技術仕様

### **対応LLMプロバイダー**
- **Databricks**: Model Serving endpoints
- **OpenAI**: GPT-3.5/4 series
- **Azure OpenAI**: GPT-4 deployments  
- **Anthropic**: Claude series

### **サポートファイル形式**
- **入力**: Databricks SQLプロファイルJSON
- **出力**: JSON、SQL、Markdown

### **分析対象メトリクス**
- 実行時間・メモリ使用量
- スピル・シャッフル量
- キャッシュ効率
- **実行プランノード詳細**（新機能）
- **テーブルサイズ推定**（新機能）

## 🎯 最適化対象

### **BROADCAST最適化**
- 30MB閾値での正確な判定
- 既存適用状況の検出
- メモリ影響度評価

### **JOIN最適化**  
- JOIN戦略分析
- キー分布評価
- ネストループ回避

### **パーティショニング**
- Liquid Clustering推奨
- データ分散最適化
- スキュー回避

## 📋 動作要件

- **Databricks Runtime**: 11.3 LTS以降
- **Python**: 3.8以降
- **必要ライブラリ**: requests, json（標準ライブラリ）
- **メモリ**: 最低4GB推奨

## 🚨 注意事項

- 本番環境適用前に必ずテスト環境で検証
- LLMの推奨内容は参考として活用
- 大容量JSON（>100MB）の場合は処理時間が延長
- **実行プラン情報は高精度だが、フィルタリング前のサイズが含まれる場合あり**

## 📞 サポート

- 不具合報告やフィードバックは開発チームまで
- 新機能要望も随時受付中

---

**バージョン**: 2.1.0（実行プラン分析対応版）  
**最終更新**: 2024年12月  
**互換性**: Databricks SQL Warehouse、Databricks Notebooks