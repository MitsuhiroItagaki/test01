# Databricks SQL Profiler Analysis Tool

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-orange.svg)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> An AI-powered tool for analyzing Databricks SQL profiler log files to identify bottlenecks and provide specific optimization recommendations.

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Latest Enhancements](#latest-enhancements)
- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration Options](#configuration-options)
- [Output Examples](#output-examples)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## 🎯 Overview

This tool analyzes JSON log files output by Databricks SQL profiler and provides the following capabilities:

- **Performance Metrics Extraction**: Detailed analysis of execution time, data volume, cache efficiency, and more
- **AI-Powered Bottleneck Analysis**: Intelligent analysis using multiple LLM providers
- **Liquid Clustering Recommendations**: Concrete implementation code generation for table optimization
- **BROADCAST Analysis**: Table size estimation from execution plans and JOIN optimization
- **SQL Query Optimization**: Automatic generation of improved versions of original queries
- **Automatic Report Refinement**: Automatic readability improvement of generated reports

## ✨ Key Features

### 🔍 Comprehensive Analysis Capabilities
- **Execution Plan Analysis**: Detailed analysis of Spark execution plans
- **Photon Engine Analysis**: Photon utilization status and optimization recommendations (target 90%+)
- **Parallelism & Shuffle Analysis**: Detailed evaluation of processing efficiency
- **Memory Spill Detection**: Identification of memory usage issues with GB-level quantification

### 🤖 AI-Driven Analysis
- **Multi-LLM Support**: Compatible with Databricks, OpenAI, Azure OpenAI, and Anthropic
- **Multilingual Support**: Analysis results in Japanese and English
- **Contextual Analysis**: Optimization recommendations considering execution environment

### 📊 Advanced Optimization Features
- **Liquid Clustering**: Implementation using correct Databricks SQL syntax
- **BROADCAST Optimization**: Recommendations considering existing optimization status
- **Query Optimization**: Generation of improved versions of original SQL queries
- **NULL Literal Optimization**: Functionality to appropriately CAST NULL literals in SELECT clauses
- **Filter Rate Calculation**: Detailed analysis of processing efficiency for each node

## 🚀 Latest Enhancements

### 🐛 DEBUG_ENABLE Flag Addition
- **Debug Mode Control**: `DEBUG_ENABLE = 'Y'` retains intermediate files, `'N'` auto-deletes them
- **File Management Optimization**: Only retains final outputs (`output_optimization_report_*.md`, `output_optimized_query_*.sql`)
- **Development & Production Support**: Detailed file retention for debugging, efficient file management for production

### 🔍 EXPLAIN Statement Execution and Enhanced CTAS Support
- **Automatic EXPLAIN Execution**: `EXPLAIN_ENABLED = 'Y'` automates execution plan analysis
- **Comprehensive CTAS Support**: Accurately extracts SELECT portions from complex CREATE TABLE AS SELECT statements
- **Catalog & Database Configuration**: Flexible execution environment setup with `CATALOG` and `DATABASE` variables
- **Complex Pattern Support**: Handles complex syntax including WITH clauses, CREATE OR REPLACE, PARTITIONED BY, etc.

### 📈 Cell 47: Comprehensive Bottleneck Analysis
- **Integrated Data Analysis**: Combines TOP10 time-consuming processes, Liquid Clustering analysis, and SQL optimization execution
- **Prioritized Reporting**: HIGH/MEDIUM/LOW priority action classification
- **Quantitative Improvement Predictions**: Quantitative predictions up to 80% execution time reduction
- **PHOTON Engine Optimization**: Specific recommendations for achieving 90%+ utilization target

### 🎯 Cell 48: Automatic Report Refinement
- **Automatic Report Detection**: Auto-detection of latest `output_optimization_report_*.md` files
- **LLM Refinement**: Improvement using "Make this report more readable and concise" prompt
- **Automatic File Management**: Deletion of original files and automatic renaming of refined versions
- **Error Handling**: Comprehensive error handling and preview functionality

### 🔧 Liquid Clustering Enhancements
- **WHERE Condition Rewriting**: Implementation includes optimization of filtering conditions
- **Clustering Key Extraction**: Optimal key selection based on JOIN, GROUP BY, and WHERE conditions
- **Current Clustering Key Display**: Side-by-side display of existing and recommended clustering keys for each table
- **Automatic Table-Key Mapping**: Auto-extraction of current clustering keys (SCAN_CLUSTERS) from scan nodes
- **Comparative Analysis**: Clear identification of differences between current and recommended settings to determine optimization needs
- **Prioritized Recommendations**: Clear implementation order through HIGH/MEDIUM/LOW priorities
- **SQL Implementation Examples**: Concrete implementation code generation with CLUSTER BY syntax

### 🚀 LLM-based SQL Optimization Enhancements
- **NULL Literal Processing**: Functionality to appropriately CAST `null` literals in SELECT clauses
- **Automatic Type Inference**: Type determination considering consistency with other columns
- **Syntax Improvements**: `SELECT null as col01` → `SELECT cast(null as String) as col01`
- **Diverse Type Support**: Appropriate type selection for String, Int, Long, Double, Date, Timestamp, etc.

## 📋 Requirements

### Basic Requirements
- Python 3.8 or higher
- Databricks Runtime 10.4 or higher
- Databricks SQL profiler JSON file

### Dependencies
```python
import json
import pandas as pd
import requests
from typing import Dict, List, Any
from datetime import datetime
```

### LLM Provider (Choose one)
- **Databricks Model Serving**: Recommended (fastest)
- **OpenAI API**: GPT-4o, GPT-4-turbo support
- **Azure OpenAI**: Enterprise usage
- **Anthropic API**: Claude-3.5-sonnet support

## 🚀 Setup

### 1. File Upload
Upload files to Databricks FileStore or Volumes:

```python
# Using FileStore (recommended)
dbutils.fs.cp("file:/local/path/profiler.json", "dbfs:/FileStore/profiler.json")

# Using Unity Catalog Volumes
dbutils.fs.cp("file:/local/path/profiler.json", "/Volumes/catalog/schema/volume/profiler.json")
```

### 2. LLM Endpoint Configuration

#### Databricks Model Serving (Recommended)
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

### 3. Basic Configuration
```python
# Analysis target file path configuration
JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/nophoton.json'

# Output language configuration
OUTPUT_LANGUAGE = 'en'  # 'ja' = Japanese, 'en' = English

# EXPLAIN statement execution configuration
EXPLAIN_ENABLED = 'Y'  # 'Y' = execute, 'N' = skip

# Debug mode configuration
DEBUG_ENABLE = 'N'  # 'Y' = retain intermediate files, 'N' = keep only final files

# Catalog and database configuration (used for EXPLAIN statement execution)
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'
```

## 📊 Usage

### Basic Analysis Flow

1. **Cells 1-32**: Basic configuration and analysis function definition
2. **Cell 33**: TOP10 time-consuming process analysis
3. **Cell 35**: Liquid Clustering opportunity analysis
4. **Cell 43**: Original query extraction
5. **EXPLAIN Execution Cell**: Execution plan analysis (when EXPLAIN_ENABLED='Y')
6. **Cell 45**: LLM-based SQL optimization (utilizing EXPLAIN results)
7. **Cell 47**: Comprehensive bottleneck analysis (integrated report generation)
8. **Cell 48**: Automatic report refinement and readability improvement

### Execution Example
```python
# Basic analysis execution
profiler_data = load_profiler_json(JSON_FILE_PATH)
extracted_metrics = extract_performance_metrics(profiler_data)

# TOP10 time-consuming process analysis
top10_report = generate_top10_time_consuming_processes_report(extracted_metrics)

# Liquid Clustering analysis
clustering_analysis = analyze_liquid_clustering_opportunities(profiler_data, extracted_metrics)

# Comprehensive bottleneck analysis
bottleneck_analysis = analyze_bottlenecks_with_llm(extracted_metrics)

# LLM-based SQL optimization including NULL literal processing
optimized_sql = generate_optimized_query_with_llm(original_query, bottleneck_analysis, extracted_metrics)

# Automatic report refinement
refined_report = refine_report_content_with_llm(bottleneck_analysis)
```

## 🎯 Output Examples

### Comprehensive Analysis Report
```markdown
# Databricks SQL Profiler Bottleneck Analysis Results

## 1. Performance Overview
- Execution Time: 45.67 seconds
- Data Read: 1.2GB
- Cache Efficiency: 78%
- Data Selectivity: 45%

## 2. Main Bottleneck Analysis (Focus on Photon, Parallelism, Shuffle)
- **Photon Engine**: 65% utilization → Optimization needed to achieve 90%+ target
- **Parallelism**: 128 tasks → Recommend increasing to 256 tasks
- **Shuffle**: 2.3GB detected → Optimization needed with BROADCAST JOIN

## 3. TOP5 Processing Time Bottlenecks
1. **CRITICAL**: FileScan processing (25.2 seconds)
2. **HIGH**: ShuffleExchange processing (12.4 seconds)
3. **MEDIUM**: HashAggregate processing (5.8 seconds)
```

### Liquid Clustering Recommendations (with Current Clustering Key Display)
```markdown
### Target Tables
1. `catalog.schema.user_transactions`
   - Current clustering keys: `user_id, status`
2. `catalog.schema.product_sales`
   - Current clustering keys: `Not configured`

### Implementation SQL Examples
```sql
-- Apply Liquid Clustering to user_transactions table
-- Current clustering keys: user_id, status
-- Recommendation: Change to more efficient key combination
ALTER TABLE catalog.schema.user_transactions
CLUSTER BY (user_id, transaction_date, category);

-- Apply Liquid Clustering to product_sales table
-- Current clustering keys: Not configured
-- Recommendation: New configuration
ALTER TABLE catalog.schema.product_sales
CLUSTER BY (product_id, sales_date);
```

### NULL Literal Optimization Examples
```sql
-- Before optimization
SELECT 
  user_id,
  null as discount_amount,
  null as coupon_code,
  purchase_amount
FROM user_transactions;

-- After optimization
SELECT 
  user_id,
  cast(null as Double) as discount_amount,
  cast(null as String) as coupon_code,
  purchase_amount
FROM user_transactions;
```

## 🔧 Configuration Options

### Basic Configuration Items
```python
# 🌐 Output language configuration
OUTPUT_LANGUAGE = 'en'  # 'ja' = Japanese, 'en' = English

# 🔍 EXPLAIN statement execution configuration
EXPLAIN_ENABLED = 'Y'  # 'Y' = execute, 'N' = skip

# 🐛 Debug mode configuration
DEBUG_ENABLE = 'N'  # 'Y' = retain intermediate files, 'N' = keep only final files

# 🗂️ Catalog and database configuration
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'
```

### File Management Behavior
- **DEBUG_ENABLE='N' (Default)**: 
  - Retained files: `output_optimization_report_*.md`, `output_optimized_query_*.sql`
  - Deleted files: `output_explain_plan_*.txt` and other intermediate files

- **DEBUG_ENABLE='Y' (Debug Mode)**: 
  - Retains all intermediate files
  - Enables detailed analysis process review

### EXPLAIN Statement Execution and CTAS Support
- **Supported Query Patterns**:
  - `CREATE TABLE table_name AS SELECT ...`
  - `CREATE OR REPLACE TABLE schema.table_name AS SELECT ...`
  - `CREATE TABLE table_name AS WITH ... SELECT ...`
  - `CREATE TABLE ... USING DELTA AS SELECT ...`
  - `CREATE TABLE ... PARTITIONED BY (...) AS SELECT ...`

### Advanced Configuration
```python
# Photon optimization configuration
PHOTON_CONFIG = {
    "target_utilization": 0.9,  # Target 90% utilization
    "enable_vectorized_execution": True,
    "optimize_shuffle_partitions": True
}

# Liquid Clustering configuration
CLUSTERING_CONFIG = {
    "analyze_where_conditions": True,
    "include_join_keys": True,
    "priority_threshold": 0.8
}

# Report refinement configuration
REFINEMENT_CONFIG = {
    "auto_cleanup": True,
    "preserve_original": False,
    "max_refinement_attempts": 3
}
```

## 📈 Performance Improvement Examples

### Before and After Comparison
- **Execution Time**: 45.67 seconds → 12.34 seconds (73% improvement)
- **Photon Utilization**: 65% → 92% (target achieved)
- **Memory Spill**: 2.3GB → 0GB (completely resolved)
- **Shuffle Amount**: 1.8GB → 0.5GB (72% reduction)

## 🛠️ Troubleshooting

### Common Issues

1. **LLM Endpoint Errors**
   - Check endpoint name and API key
   - Verify network connectivity

2. **Memory Shortage Errors**
   - Test with smaller datasets
   - Review cluster configuration

3. **Report Generation Errors**
   - Check input data format
   - Check log files for error details

## 📄 License

MIT License

## 🤝 Contributing

Pull requests and issue reports are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## 📞 Support

For questions or support, please report at [Issues](https://github.com/your-repo/issues). 