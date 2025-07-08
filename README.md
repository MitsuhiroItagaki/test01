# Databricks SQLプロファイラー分析ツール

**最先端のAI駆動SQLパフォーマンス分析ツール**

DatabricksのSQLプロファイラーJSONログファイルを読み込み、AI（LLM）を活用してボトルネック特定・改善案提示・SQL最適化を行う包括的な分析ツールです。

## ✨ 主要機能

### 🔍 **高度なパフォーマンス分析**
- SQLプロファイラーJSONファイルの自動解析
- 時間消費TOP10プロセスの詳細分析（テーブル名表示対応）
- スピル検出・データスキュー・並列度問題の特定
- Photonエンジン利用状況の可視化

### 🤖 **マルチプロバイダーAI分析**
- **Databricks Claude 3.7 Sonnet**（推奨）
- **OpenAI GPT-4/GPT-4 Turbo**
- **Azure OpenAI**
- **Anthropic Claude**
- 128K tokens対応・拡張思考モード

### 🗂️ **Liquid Clustering最適化**
- プロファイラーデータからカラム使用パターンを分析
- フィルター・JOIN・GROUP BY条件の自動抽出
- テーブル別クラスタリング推奨カラムの特定
- パフォーマンス向上見込みの定量評価

### 🚀 **SQL自動最適化**
- オリジナルクエリの自動抽出
- AI駆動によるクエリ最適化
- 実行可能な最適化SQLの生成
- テスト実行スクリプトの自動作成

### 📊 **包括的レポーティング**
- JSON・テキスト・Markdown・Pythonファイルの自動生成
- 視覚的なダッシュボード表示
- 詳細なボトルネック分析レポート
- パフォーマンス改善の定量的評価

## 📁 ファイル構成

```
📦 Databricks SQL Profiler Analysis Tool
├── 📄 databricks_sql_profiler_analysis.py    # 🌟 メインノートブック（24セル構成）
├── 📄 simple0.json                           # サンプルSQLプロファイラーファイル
├── 📄 README.md                              # このファイル
├── 📁 outputs/                               # 生成ファイル（実行時作成）
│   ├── 📄 extracted_metrics_YYYYMMDD-HHMISS.json
│   ├── 📄 bottleneck_analysis_YYYYMMDD-HHMISS.txt
│   ├── 📄 original_query_YYYYMMDD-HHMISS.sql
│   ├── 📄 optimized_query_YYYYMMDD-HHMISS.sql
│   ├── 📄 optimization_report_YYYYMMDD-HHMISS.md
│   └── 📄 test_optimized_query_YYYYMMDD-HHMISS.py
└── 📁 samples/                               # 追加サンプル（オプション）
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
# 📁 分析対象ファイル設定（セル2）
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/simple0.json'

# 🤖 LLMエンドポイント設定（セル3）
LLM_CONFIG = {
    "provider": "databricks",  # "databricks", "openai", "azure_openai", "anthropic"
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet"
    }
}
```

### ステップ 3: 順次実行

```bash
🔧 設定・準備セクション     → セル2〜10を実行
🚀 メイン処理実行セクション  → セル11〜17を実行
🔧 SQL最適化機能セクション   → セル18〜23を実行（オプション）
📚 参考・応用セクション      → セル24参照
```

## 📋 セル構成詳細

### 🔧 設定・準備セクション（セル2-10）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 2 | 📁 分析対象ファイル設定 | JSONファイルパスの指定 |
| 3 | 🤖 LLMエンドポイント設定 | AI分析プロバイダーの選択 |
| 4 | 📂 ファイル読み込み関数 | DBFS/FileStore/ローカル対応 |
| 5 | 📊 メトリクス抽出関数 | パフォーマンス指標の抽出 |
| 6 | 🏷️ ノード名解析関数 | 意味のあるノード名への変換 |
| 7 | 🎯 ボトルネック計算関数 | 指標計算とスピル検出 |
| 8 | 🧬 Liquid Clustering関数 | クラスタリング分析 |
| 9 | 🤖 LLM分析関数 | AI分析用プロンプト生成 |
| 10 | 🔌 LLMプロバイダー関数 | 各AIサービス接続 |

### 🚀 メイン処理実行セクション（セル11-17）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 11 | 🚀 ファイル読み込み実行 | JSONデータの読み込み |
| 12 | 📊 メトリクス抽出 | 性能指標の抽出と表示 |
| 13 | 🔍 ボトルネック詳細分析 | TOP10時間消費プロセス |
| 14 | 💾 メトリクス保存 | JSONファイル出力 |
| 15 | 🗂️ Liquid Clustering分析 | クラスタリング推奨 |
| 16 | 📋 LLM分析準備 | AI分析の実行準備 |
| 17 | 🎯 AI分析結果表示 | ボトルネック分析結果 |

### 🔧 SQL最適化機能セクション（セル18-23）
| セル | 機能 | 説明 |
|-----|-----|-----|
| 18 | 🔧 最適化関数定義 | SQL最適化関数の定義 |
| 19 | 🚀 クエリ抽出 | オリジナルクエリの抽出 |
| 20 | 🤖 LLM最適化実行 | AI駆動クエリ最適化 |
| 21 | 💾 結果保存 | 最適化ファイル生成 |
| 22 | 🧪 テスト準備 | 実行ガイドとスクリプト |
| 23 | 🏁 完了サマリー | 全処理の完了確認 |

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
# OpenAI設定例
LLM_CONFIG = {
    "provider": "openai",
    "openai": {
        "api_key": "sk-...",  # または環境変数OPENAI_API_KEY
        "model": "gpt-4o",
        "max_tokens": 2000
    }
}

# Azure OpenAI設定例  
LLM_CONFIG = {
    "provider": "azure_openai",
    "azure_openai": {
        "api_key": "your-azure-key",
        "endpoint": "https://your-resource.openai.azure.com/",
        "deployment_name": "gpt-4",
        "api_version": "2024-02-01"
    }
}

# Anthropic設定例
LLM_CONFIG = {
    "provider": "anthropic", 
    "anthropic": {
        "api_key": "sk-ant-...",  # または環境変数ANTHROPIC_API_KEY
        "model": "claude-3-5-sonnet-20241022"
    }
}
```

### 2. SQLプロファイラーファイルの取得

#### Databricks SQLエディタから取得

1. **SQLクエリを実行**
2. **Query History** → 対象クエリを選択
3. **Query Profile** タブ → **Download Profile JSON**

#### プログラマティック取得

```sql
-- システムテーブルから取得
SELECT query_id, query_text, query_start_time, total_time_ms
FROM system.query.history 
WHERE query_start_time >= current_timestamp() - INTERVAL 1 DAY
  AND total_time_ms > 30000  -- 30秒以上のクエリ
ORDER BY total_time_ms DESC
LIMIT 10;
```

#### ファイルアップロード方法

```python
# 方法1: Databricks UI
# Data → Create Table → Upload File → JSONファイルをドラッグ&ドロップ

# 方法2: dbutils
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# 方法3: Databricks CLI
# databricks fs cp profiler.json dbfs:/FileStore/profiler.json
```

## 📊 出力ファイル詳細

### 📄 extracted_metrics_YYYYMMDD-HHMISS.json

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
      "customer": {
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

### 📄 bottleneck_analysis_YYYYMMDD-HHMISS.txt

```text
🔍 Databricks SQL プロファイラー分析結果

📊 【クエリ基本情報】
🆔 クエリID: 01f0565c-48f6-1283-a782-14ed6494eee0
⏱️ 実行時間: 84,224 ms (84.2秒)
💾 読み込みデータ: 115.4 GB
📈 出力行数: 2,753 行

🚨 【特定されたボトルネック】

1. 🔥 **大量スピル発生 (HIGH PRIORITY)**
   - スピル量: 1.0 GB
   - 原因: メモリ不足による中間結果のディスク書き込み
   - 影響: 実行時間の30-50%増加

2. ⚡ **シャッフル操作ボトルネック (MEDIUM PRIORITY)**  
   - シャッフル回数: 3回
   - 最大シャッフル時間: 15,234 ms
   - 影響: 全体実行時間の18%

3. 📊 **データ選択性の問題 (MEDIUM PRIORITY)**
   - 選択性: 0.0022% (非常に低い)
   - 読み込み115.4GB → 出力2,753行
   - 改善余地: フィルター条件の最適化

🚀 【推奨改善策】

1. **メモリ設定の最適化**
   - spark.sql.adaptive.coalescePartitions.enabled = true
   - クラスターメモリ増強 (32GB → 64GB推奨)

2. **Liquid Clusteringの適用**
   - customer テーブル: customer_id, region でクラスタリング
   - 期待効果: スキャン時間50-70%削減

3. **クエリ構造の最適化**
   - WHERE句をJOINより前に配置
   - 不要なカラムの除外
   - パーティション剪定の活用

📈 【期待される改善効果】
- 実行時間: 84.2秒 → 35-45秒 (約50%削減)
- コスト削減: 約60%
- スピル解消: 100%削減見込み
```

### 📄 optimized_query_YYYYMMDD-HHMISS.sql

```sql
-- 最適化されたSQLクエリ
-- 元クエリID: 01f0565c-48f6-1283-a782-14ed6494eee0
-- 最適化日時: 2024-01-15 14:30:22
-- ベースクエリ: original_query_20240115-143022.sql

-- PHOTONエンジン最適化とLiquid Clustering対応
WITH customer_filtered AS (
  SELECT customer_id, region, signup_date
  FROM customer 
  WHERE region IN ('US', 'EU')  -- 早期フィルタリング
    AND signup_date >= '2023-01-01'
),
orders_summary AS (
  SELECT 
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as order_count
  FROM orders 
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

## 🔍 高度な機能

### 📊 カスタム分析プロンプト

```python
# analyze_bottlenecks_with_llm関数をカスタマイズ
custom_prompt = f"""
あなたは金融業界のSQLパフォーマンス専門家です。
以下の観点で分析してください：
- リアルタイム取引処理への影響
- 規制要件への適合性
- 災害復旧時の性能
- コンプライアンス監査への対応

{standard_analysis_content}

業界特有の推奨事項:
- SOX法対応のためのパフォーマンス要件
- Basel III規制下でのリスク計算最適化
- GDPR対応のデータアクセス最適化
"""
```

### 🔄 一括分析・比較

```python
# 複数クエリの性能比較
profiler_files = [
    '/path/to/query1_profile.json',
    '/path/to/query2_profile.json', 
    '/path/to/query3_profile.json'
]

comparison_results = []
for file_path in profiler_files:
    profiler_data = load_profiler_json(file_path)
    metrics = extract_performance_metrics(profiler_data)
    comparison_results.append({
        'query_id': metrics['query_info']['query_id'],
        'execution_time': metrics['overall_metrics']['total_time_ms'],
        'data_processed_gb': metrics['overall_metrics']['read_bytes'] / 1024**3,
        'spill_detected': metrics['bottleneck_indicators']['has_spill'],
        'cache_efficiency': metrics['bottleneck_indicators']['cache_hit_ratio']
    })

# DataFrame化して比較
comparison_df = spark.createDataFrame(comparison_results)
comparison_df.show()
```

### 📈 自動監視・アラート

```python
# パフォーマンス劣化の自動検出
def monitor_query_performance():
    """定期実行でのパフォーマンス監視"""
    recent_queries = spark.sql("""
        SELECT query_id, query_text, total_time_ms, read_bytes
        FROM system.query.history 
        WHERE query_start_time >= current_timestamp() - INTERVAL 1 HOUR
        AND total_time_ms > 60000  -- 1分以上のクエリ
    """)
    
    for row in recent_queries.collect():
        # 閾値チェック
        if row.total_time_ms > 300000:  # 5分以上
            send_performance_alert(row.query_id, "Long execution time detected")
        
        if row.read_bytes > 100 * 1024**3:  # 100GB以上
            send_performance_alert(row.query_id, "Large data scan detected")

# Slack/Teams通知
def send_performance_alert(query_id, message):
    webhook_url = "YOUR_SLACK_WEBHOOK_URL"
    payload = {
        "text": f"🚨 Performance Alert: {message}\nQuery ID: {query_id}"
    }
    requests.post(webhook_url, json=payload)
```

## 🛠️ トラブルシューティング

### ❌ よくあるエラーと解決方法

#### 1. LLMエンドポイントエラー

```bash
# エラー例
APIエラー: ステータスコード 404
HTTP 404: Model serving endpoint 'databricks-claude-3-7-sonnet' not found

# 解決方法
1. エンドポイント存在確認:
   databricks serving-endpoints list

2. エンドポイント状態確認: 
   databricks serving-endpoints get databricks-claude-3-7-sonnet

3. 権限確認:
   - Model Servingへのアクセス権限
   - Personal Access Tokenの有効性
```

#### 2. メモリ不足エラー

```python
# エラー例  
OutOfMemoryError: Java heap space

# 解決方法
# 1. クラスターメモリ増強
spark.conf.set("spark.driver.memory", "16g")
spark.conf.set("spark.executor.memory", "32g")

# 2. 大きなJSONファイルの分割処理
def process_large_json(file_path, chunk_size=1000000):
    """大きなJSONファイルの分割処理"""
    with open(file_path, 'r') as f:
        # ストリーミング処理で部分的に読み込み
        pass

# 3. 不要なデータのフィルタリング
def extract_essential_metrics_only(profiler_data):
    """必要最小限のメトリクスのみ抽出"""
    essential_data = {
        'query': profiler_data.get('query', {}),
        'graphs': profiler_data.get('graphs', [])[:1]  # 最初のグラフのみ
    }
    return essential_data
```

#### 3. ファイルアクセスエラー

```python
# エラー例
FileNotFoundError: [Errno 2] No such file or directory

# 解決方法とデバッグ
# 1. ファイル存在確認
def debug_file_access(file_path):
    """ファイルアクセスのデバッグ"""
    try:
        # DBFS確認
        if file_path.startswith('/dbfs/') or file_path.startswith('dbfs:/'):
            dbfs_files = dbutils.fs.ls(file_path.replace('/dbfs', '').replace('dbfs:', ''))
            print(f"DBFS files found: {len(dbfs_files)}")
        
        # ローカル確認
        import os
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"Local file found, size: {size} bytes")
        else:
            print(f"File not found: {file_path}")
            
    except Exception as e:
        print(f"Debug error: {e}")

# 使用例
debug_file_access('/dbfs/FileStore/shared_uploads/user/profiler.json')
```

### 🔧 パフォーマンス最適化

```python
# 大規模データ処理の最適化
def optimize_large_scale_analysis():
    """大規模データ分析の最適化設定"""
    
    # Spark設定の最適化
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") 
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    
    # メモリ設定
    spark.conf.set("spark.driver.maxResultSize", "8g")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    # 並列処理の最適化
    num_cores = spark.sparkContext.defaultParallelism
    optimal_partitions = num_cores * 2
    spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
    
    print(f"✅ 最適化設定完了: {num_cores} cores, {optimal_partitions} partitions")

# 実行前に最適化設定を適用
optimize_large_scale_analysis()
```

## 📚 アドバンス活用

### 🎯 業界特化カスタマイズ

```python
# 金融業界向けカスタマイズ例
class FinancialSQLAnalyzer:
    def __init__(self):
        self.regulatory_keywords = ['risk', 'basel', 'var', 'stress_test']
        self.performance_thresholds = {
            'trading_queries': 1000,  # 1秒以内
            'risk_calculations': 30000,  # 30秒以内
            'reporting_queries': 300000  # 5分以内
        }
    
    def analyze_regulatory_compliance(self, metrics):
        """規制要件への適合性分析"""
        query_text = metrics['query_info']['query_text'].lower()
        execution_time = metrics['overall_metrics']['total_time_ms']
        
        # クエリタイプの判定
        query_type = 'general'
        for keyword in self.regulatory_keywords:
            if keyword in query_text:
                if keyword in ['risk', 'var']:
                    query_type = 'risk_calculations'
                elif keyword == 'stress_test':
                    query_type = 'trading_queries'
                break
        
        # 性能要件チェック
        threshold = self.performance_thresholds.get(query_type, 60000)
        compliance_status = "COMPLIANT" if execution_time <= threshold else "NON_COMPLIANT"
        
        return {
            'query_type': query_type,
            'compliance_status': compliance_status,
            'threshold_ms': threshold,
            'actual_ms': execution_time,
            'deviation_pct': ((execution_time - threshold) / threshold) * 100
        }

# 使用例
financial_analyzer = FinancialSQLAnalyzer()
compliance_result = financial_analyzer.analyze_regulatory_compliance(extracted_metrics)
print(f"Compliance Status: {compliance_result['compliance_status']}")
```

### 🔄 CI/CD統合

```yaml
# .github/workflows/sql-performance-check.yml
name: SQL Performance Analysis

on:
  pull_request:
    paths:
      - 'sql/**'
      - 'queries/**'

jobs:
  performance-analysis:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Databricks CLI
        run: |
          pip install databricks-cli
          echo "${{ secrets.DATABRICKS_TOKEN }}" | databricks configure --token
          
      - name: Run SQL Performance Analysis
        run: |
          # 変更されたSQLファイルに対してプロファイル実行
          for sql_file in $(git diff --name-only ${{ github.event.before }} HEAD | grep '\.sql$'); do
            echo "Analyzing $sql_file"
            
            # SQLを実行してプロファイル取得
            databricks sql-exec --file $sql_file --profile-output profile_$sql_file.json
            
            # 分析ツール実行
            python sql_profiler_analysis.py --input profile_$sql_file.json --output analysis_$sql_file.txt
            
            # 性能劣化チェック
            if [ $(grep "CRITICAL" analysis_$sql_file.txt | wc -l) -gt 0 ]; then
              echo "❌ Performance issues detected in $sql_file"
              exit 1
            fi
          done
          
      - name: Comment PR
        uses: actions/github-script@v6
        if: failure()
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '🚨 SQL performance issues detected. Please review the analysis results.'
            })
```

## 📄 ライセンス・免責事項

### 📜 利用条件

- **用途**: 教育・研究・内部分析目的での使用を想定
- **プロダクション環境**: 事前の十分なテストと検証が必要
- **データプライバシー**: 機密性の高いクエリログの取り扱いに注意
- **コスト管理**: LLM API使用料とコンピュートリソース利用料に注意

### ⚠️ 免責事項

- 本ツールの分析結果は参考情報であり、実際のパフォーマンス改善を保証するものではありません
- AI生成の最適化SQLは本番環境での実行前に必ず検証してください
- 大量データや複雑なクエリの分析には実行時間とコストが増大する可能性があります
- Databricks Claude 3.7 Sonnetの利用には適切なライセンスと権限が必要です

## 🤝 サポート・コミュニティ

### 📞 技術サポート

**問題報告時の情報:**
```
1. 環境情報
   - Databricks Runtime version: __________
   - Cluster configuration: _______________
   - Python version: _____________________

2. エラー詳細
   - エラーメッセージ: ___________________
   - 発生セル番号: _______________________
   - スタックトレース: __________________

3. 使用状況
   - JSONファイルサイズ: ________________
   - LLMプロバイダー: ___________________
   - カスタマイズ内容: __________________
```

### 🚀 機能リクエスト・改善提案

**歓迎する貢献:**
- 新しいLLMプロバイダーの対応
- 追加の分析メトリクス
- 業界特化の分析テンプレート
- 可視化機能の強化
- パフォーマンス最適化

### 📈 ロードマップ

**近日実装予定:**
- 🔄 リアルタイム分析機能
- 📊 Databricks SQLダッシュボード統合
- 🤖 自動SQL最適化の精度向上
- 🎯 A/Bテスト機能
- 📱 モバイル対応レポート

---

**🎉 このツールを使用して、SQLパフォーマンス改善の旅を始めましょう！**

📧 フィードバック・質問: [GitHub Issues](https://github.com/your-repo/databricks-sql-profiler-analysis/issues)  
📖 詳細ドキュメント: [Wiki](https://github.com/your-repo/databricks-sql-profiler-analysis/wiki)  
💬 コミュニティ: [Discussions](https://github.com/your-repo/databricks-sql-profiler-analysis/discussions)
