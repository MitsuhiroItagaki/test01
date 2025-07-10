# Databricks SQL Profiler Analysis Tool

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-orange.svg)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> An AI-powered tool for analyzing Databricks SQL profiler log files to identify bottlenecks and provide specific optimization recommendations.

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
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

## ✨ Key Features

### 🔍 Comprehensive Analysis Capabilities
- **Execution Plan Analysis**: Detailed analysis of Spark execution plans
- **Photon Engine Analysis**: Photon utilization status and optimization recommendations
- **Parallelism & Shuffle Analysis**: Detailed evaluation of processing efficiency
- **Memory Spill Detection**: Identification of memory usage issues

### 🤖 AI-Driven Analysis
- **Multi-LLM Support**: Compatible with Databricks, OpenAI, Azure OpenAI, and Anthropic
- **Multilingual Support**: Analysis results in Japanese and English
- **Contextual Analysis**: Optimization recommendations considering execution environment

### 📊 Advanced Optimization Features
- **Liquid Clustering**: Implementation using correct Databricks SQL syntax
- **BROADCAST Optimization**: Recommendations considering existing optimization status
- **Query Optimization**: Generation of improved versions of original SQL queries

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
        "api_key": "your-openai-api-key",
        "model": "gpt-4o",
        "max_tokens": 16000,
        "temperature": 0.0
    }
}
```

### 3. Basic Configuration
```python
# Target file path
JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler.json'

# Output language setting
OUTPUT_LANGUAGE = 'en'  # 'ja' = Japanese, 'en' = English
```

## 💻 Usage

### Basic Usage

1. **Load JSON File**
```python
profiler_data = load_profiler_json(JSON_FILE_PATH)
```

2. **Extract Metrics**
```python
metrics = extract_performance_metrics(profiler_data)
```

3. **AI-Powered Analysis**
```python
analysis_result = analyze_bottlenecks_with_llm(metrics)
```

4. **Generate Optimized Query**
```python
original_query = extract_original_query_from_profiler_data(profiler_data)
optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, metrics)
```

### Complete Analysis Flow

```python
# 1. Data Loading
profiler_data = load_profiler_json(JSON_FILE_PATH)
extracted_metrics = extract_performance_metrics(profiler_data)

# 2. Bottleneck Analysis
analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)

# 3. Liquid Clustering Analysis
clustering_analysis = analyze_liquid_clustering_opportunities(profiler_data, extracted_metrics)

# 4. BROADCAST Analysis
original_query = extract_original_query_from_profiler_data(profiler_data)
plan_info = extract_execution_plan_info(profiler_data)
broadcast_analysis = analyze_broadcast_feasibility(extracted_metrics, original_query, plan_info)

# 5. Optimized Query Generation
optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, extracted_metrics)

# 6. Report Generation
save_optimized_sql_files(original_query, optimized_query, extracted_metrics)
```

## ⚙️ Configuration Options

### LLM Provider Configuration

| Provider | Configuration Key | Description |
|----------|------------------|-------------|
| Databricks | `endpoint_name` | Model Serving endpoint name |
| OpenAI | `api_key`, `model` | API key and model name |
| Azure OpenAI | `api_key`, `endpoint`, `deployment_name` | Azure-specific configuration |
| Anthropic | `api_key`, `model` | Anthropic API key and model |

### Analysis Options

| Option | Default | Description |
|--------|---------|-------------|
| `OUTPUT_LANGUAGE` | 'ja' | Output language ('ja' or 'en') |
| `max_tokens` | 131072 | Maximum LLM tokens |
| `temperature` | 0.0 | LLM output randomness |

## 📊 Output Examples

### Bottleneck Analysis Results
```
🔧 **Databricks SQL Profiler Bottleneck Analysis Results**

## 📊 Performance Overview
- **Execution Time**: 45.2 seconds
- **Data Read**: 2.1GB
- **Cache Efficiency**: 85.3%
- **Data Selectivity**: 12.4%

## ⚡ Photon Engine Analysis
- **Photon Enabled**: Yes
- **Photon Utilization**: 92.1%
- **Recommendation**: Optimized

## 🗂️ Liquid Clustering Recommendations
**Target Tables**: 3 tables

**Recommended Implementation**:
- orders table: ALTER TABLE orders CLUSTER BY (customer_id, order_date)
- customers table: ALTER TABLE customers CLUSTER BY (region, customer_type)
```

### Optimized SQL Query
```sql
-- Before optimization
SELECT customer_id, SUM(amount) 
FROM orders 
WHERE order_date >= '2023-01-01' 
GROUP BY customer_id;

-- After optimization
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

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. LLM Endpoint Error
```
❌ Analysis Error: Connection timeout
```
**Solution**:
- Databricks: Check Model Serving endpoint status
- OpenAI/Azure/Anthropic: Verify API key and quota
- Check network connectivity

#### 2. File Loading Error
```
❌ File loading error: FileNotFoundError
```
**Solution**:
```python
# Check file existence
dbutils.fs.ls("/FileStore/shared_uploads/")

# Verify path format
# Correct: '/FileStore/shared_uploads/username/file.json'
# Correct: '/Volumes/catalog/schema/volume/file.json'
```

#### 3. Memory Error
```
❌ OutOfMemoryError: Java heap space
```
**Solution**:
- Increase cluster memory settings
- Use larger instance types
- Process multiple profiler files separately

#### 4. Multi-language Character Encoding
```
❌ UnicodeDecodeError
```
**Solution**:
```python
# Verify UTF-8 encoding
with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
```

## 📈 Performance Optimization Tips

### 1. LLM Provider Selection
- **Fast Processing**: Databricks Model Serving
- **High-Quality Analysis**: OpenAI GPT-4o
- **Enterprise**: Azure OpenAI
- **Cost-Effective**: Anthropic Claude

### 2. Large File Processing
```python
# Check file size
import os
file_size = os.path.getsize(JSON_FILE_PATH)
print(f"File size: {file_size / 1024 / 1024:.1f}MB")

# Consider split processing for large files
```

### 3. Parallel Processing
```python
# Parallel processing of multiple files
from concurrent.futures import ThreadPoolExecutor

profiler_files = [file1, file2, file3]
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(analyze_single_file, profiler_files))
```

## 🔧 Customization

### Adding Analysis Metrics
```python
def extract_performance_metrics(profiler_data):
    # Add custom metrics
    metrics["custom_indicators"] = {
        "custom_metric_1": calculate_custom_metric_1(profiler_data),
        "custom_metric_2": calculate_custom_metric_2(profiler_data)
    }
    return metrics
```

### Adding New LLM Providers
```python
def _call_custom_llm(prompt: str) -> str:
    # Custom LLM provider implementation
    pass

# Add to LLM_CONFIG
LLM_CONFIG["custom_provider"] = {
    "api_key": "your-api-key",
    "endpoint": "your-endpoint"
}
```

## 📝 License

MIT License

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request

## 📞 Support

- Issues: [GitHub Issues](https://github.com/your-username/databricks-sql-profiler-analysis/issues)
- Documentation: [Wiki](https://github.com/your-username/databricks-sql-profiler-analysis/wiki)
- Email: support@example.com

---

**Note**: This tool provides analysis and recommendations only. Always validate recommendations before executing in production environments. 