# Databricks SQL Profiler Analysis Tool

[English](#english) | [日本語](README_ja.md)

---

## English

**AI-Powered SQL Performance Analysis Tool for Databricks**

A comprehensive analysis tool that leverages AI (LLM) to analyze Databricks SQL Profiler JSON logs, identify bottlenecks, provide optimization recommendations, and generate optimized SQL queries with precise execution plan analysis.

## ✨ Key Features

### 📊 **Advanced Performance Analysis**
- Detailed metrics extraction from JSON profiles
- Automatic bottleneck detection (spill, shuffle, cache efficiency, etc.)
- TOP10 time-consuming processes analysis

### 🧠 **Multi-Provider AI Analysis**
- Multiple LLM provider support (Databricks, OpenAI, Azure OpenAI, Anthropic)
- Automatic bottleneck diagnosis and optimization recommendations
- Japanese/English output support

### 📄 **Execution Plan Detailed Analysis** (New Feature)
- Accurate table size estimation from execution plans (`estimatedSizeInBytes` utilization)
- Detailed analysis of BROADCAST, JOIN, shuffle, and aggregate nodes
- Plan structure visualization and Markdown report generation

### 🎯 **High-Precision BROADCAST Analysis** (Enhanced)
- Prioritizes Spark engine's actual estimated values
- Accurate judgment with 30MB threshold
- Automatic detection of existing BROADCAST applications
- Clear indication of size estimation confidence levels

### 🔧 **SQL Optimization**
- Automatic extraction of original queries
- LLM-based query optimization
- Executable SQL output

### 💾 **Comprehensive File Output**
- Performance analysis JSON files
- Execution plan analysis JSON files (New Feature)
- Detailed Markdown reports (including execution plan information)
- Optimized SQL files
- All with timestamp and `output_` prefix

## 📁 Output Files

| File | Format | Description |
|------|--------|-------------|
| `output_performance_analysis_YYYYMMDD-HHMMSS.json` | JSON | Performance metrics details |
| `output_execution_plan_analysis_YYYYMMDD-HHMMSS.json` | JSON | **Execution plan structure and size estimation** |
| `output_execution_plan_report_YYYYMMDD-HHMMSS.md` | Markdown | **Execution plan detailed report** |
| `output_bottleneck_analysis_YYYYMMDD-HHMMSS.md` | Markdown | LLM bottleneck analysis report |
| `output_original_query_YYYYMMDD-HHMMSS.sql` | SQL | Extracted original query |
| `output_optimized_query_YYYYMMDD-HHMMSS.sql` | SQL | LLM optimized query |
| `output_optimization_report_YYYYMMDD-HHMMSS.md` | Markdown | Optimization report (including BROADCAST analysis) |

## 🔬 New Feature Details

### 📏 **Table Size Estimation from Execution Plans**
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

**Benefits:**
- ✅ Direct utilization of Spark engine's actual estimated values (high accuracy)
- ✅ Reflects post-filtering sizes
- ✅ Simultaneous acquisition of file count and partition count

### 🎯 **High-Precision BROADCAST Analysis**

**Estimation Accuracy Improvement:**
- **Previous**: Metrics-based indirect estimation (confidence: medium)
- **New Feature**: Execution plan `estimatedSizeInBytes` (confidence: high)

**Analysis Example:**
```
🔹 BROADCAST(orders_table) - Uncompressed 15.2MB (≤ safe threshold 24.0MB) strongly recommended for BROADCAST
(execution_plan_estimate based, confidence: high)
```

### 📊 **Execution Plan Analysis Report**
- 📊 Execution plan summary (total nodes, JOIN strategies, etc.)
- 📡 BROADCAST node details
- 🔗 JOIN node details
- 📋 Table scan details (including size estimation)
- 📏 **Table Size Estimation Information** (new section)
- 💡 Plan-based optimization recommendations

## 🛠 Usage Guide

### 1. **LLM Provider Configuration**
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

# Azure OpenAI
LLM_CONFIG = {
    'provider': 'azure_openai',
    'azure_openai': {
        'api_key': 'your-azure-key',
        'endpoint': 'https://your-resource.openai.azure.com/',
        'deployment_name': 'gpt-4'
    }
}

# Anthropic
LLM_CONFIG = {
    'provider': 'anthropic',
    'anthropic': {
        'api_key': 'your-anthropic-key',
        'model': 'claude-3-sonnet-20240229'
    }
}
```

### 2. **Profile File Placement**
```bash
# File upload destination
/FileStore/shared_uploads/your-email/profiler_output.json
```

### 3. **Execute Analysis**
```python
# Run in Databricks Notebook
# Execute cells 1-22 sequentially
```

### 4. **Review Results**
- All generated files have `output_` prefix
- Timestamps distinguish multiple executions
- JSON, SQL, and Markdown files are automatically generated

## 📈 Analysis Report Examples

### **Execution Plan Analysis Report** (New Feature)
```markdown
## 📏 Table Size Estimation (Execution Plan Based)

- **Estimated Tables Count**: 3
- **Total Estimated Size**: 125.5MB

### orders
- **Estimated Size**: 15.2MB
- **Confidence**: high
- **File Count**: 5

### customers  
- **Estimated Size**: 45.8MB
- **Confidence**: high
- **File Count**: 12

## 💡 Execution Plan Based BROADCAST Recommendations

- Small tables ≤30MB detected: 1 table
  • orders: 15.2MB (BROADCAST candidate)
```

### **BROADCAST Analysis Results** (Enhanced)
```markdown
## BROADCAST Hint Analysis (30MB Threshold)

- **JOIN Query**: Yes
- **Spark BROADCAST Threshold**: 30.0MB (uncompressed)
- **BROADCAST Feasibility**: recommended
- **BROADCAST Candidates**: 1

### BROADCAST Candidate Tables (Detailed Analysis)

🔹 **orders**
  - **Uncompressed Size**: 15.2MB
  - **Compressed Size**: 3.8MB
  - **Compression Ratio**: 4.0x
  - **File Format**: delta
  - **Rows**: 50,000
  - **Confidence**: high
  - **Reasoning**: Uncompressed estimated size 15.2MB (≤ safe threshold 24.0MB) strongly recommended for BROADCAST (execution_plan_estimate based, confidence: high)
```

## 🔧 Technical Specifications

### **Supported LLM Providers**
- **Databricks**: Model Serving endpoints
- **OpenAI**: GPT-3.5/4 series
- **Azure OpenAI**: GPT-4 deployments
- **Anthropic**: Claude series

### **Supported File Formats**
- **Input**: Databricks SQL Profile JSON
- **Output**: JSON, SQL, Markdown

### **Analysis Target Metrics**
- Execution time and memory usage
- Spill and shuffle volumes
- Cache efficiency
- **Execution plan node details** (New Feature)
- **Table size estimation** (New Feature)

## 🎯 Optimization Targets

### **BROADCAST Optimization**
- Accurate judgment with 30MB threshold
- Detection of existing applications
- Memory impact evaluation

### **JOIN Optimization**
- JOIN strategy analysis
- Key distribution evaluation
- Nested loop avoidance

### **Partitioning**
- Liquid Clustering recommendations
- Data distribution optimization
- Skew avoidance

## 📋 System Requirements

- **Databricks Runtime**: 11.3 LTS or later
- **Python**: 3.8 or later
- **Required Libraries**: requests, json (standard libraries)
- **Memory**: Minimum 4GB recommended

## 🚨 Important Notes

- Always test in a development environment before applying to production
- Use LLM recommendations as reference guidance
- Processing time may be extended for large JSON files (>100MB)
- **Execution plan information is high-precision but may include pre-filtering sizes**

## 🚀 Quick Start

### Step 1: Create Notebook

1. Create a new **Notebook** in Databricks workspace
2. Set language to **Python**
3. Copy and paste content from `databricks_sql_profiler_analysis.py`

### Step 2: Basic Configuration

```python
# 📁 Analysis target file setting
JSON_FILE_PATH = '/FileStore/shared_uploads/your-email/profiler_output.json'

# 🌐 Output language setting
OUTPUT_LANGUAGE = 'en'  # 'en' = English, 'ja' = Japanese

# 🤖 LLM endpoint setting
LLM_CONFIG = {
    "provider": "databricks",  # "databricks", "openai", "azure_openai", "anthropic"
    "thinking_enabled": False,  # Thinking process display (default: disabled for fast execution)
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",
        "max_tokens": 131072,  # 128K tokens (Claude 3.7 Sonnet max limit)
        "temperature": 0.0,    # Deterministic output
        "thinking_budget_tokens": 65536  # 64K tokens (used only when thinking enabled)
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

## 📊 Output File Details

### Performance Analysis Report
- Query information and execution metrics
- Bottleneck indicators and performance analysis
- TOP10 time-consuming processes
- Cache efficiency and Photon utilization

### Execution Plan Analysis Report (New)
- Plan structure summary and node analysis
- Table size estimation with confidence levels
- BROADCAST, JOIN, shuffle, aggregate node details
- Plan-based optimization recommendations

### SQL Optimization Report
- Original and optimized SQL queries
- BROADCAST hint analysis with 30MB threshold
- Performance improvement estimations
- Detailed optimization rationale

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. LLM Timeout Errors
```
❌ ⏰ Timeout Error: LLM endpoint response did not complete within 300 seconds.
```

**Solution**:
- Timeout extended: 180s → **300s (5 minutes)**
- Retry attempts increased: 2 → **3 times**
- Prompt optimization: 60% size reduction
- Token limit optimization for large models

#### 2. Incomplete SQL Generation
```sql
-- Problem: Column names or table names are omitted
SELECT 
 r_uid,
 ref_domain
 FROM
 catalog.schema.
 -- [truncated]
```

**Solution**:
✅ **Enhanced Completeness Check**: Strict constraints added to prompts
- Complete prohibition of omissions and placeholders
- Explicit requirement to preserve all SELECT items
- Step-by-step construction with thinking functionality
- Retention of detailed analysis information

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
| **Size Estimation** | Metrics only | **Plan + Metrics** | High precision |
| **BROADCAST Detection** | Rule-based | **Plan-based** | 95% accuracy |
| **Confidence Levels** | Single level | **High/Medium** | Transparent reliability |
| **30MB Threshold** | Estimated | **Strict enforcement** | Precise compliance |
| **Language Support** | Limited | **EN/JA** | Global usage |
| **Execution Plan** | Not used | **Fully integrated** | Reality-based analysis |

### Expected Results
- ✅ Accurate size estimation using Spark's `estimatedSizeInBytes`
- ✅ Elimination of incorrect BROADCAST recommendations
- ✅ Transparent confidence levels for all estimations
- ✅ Reality-based optimization suggestions
- ✅ Enhanced execution plan analysis capabilities

## 📞 Support

- Bug reports and feedback welcome
- Feature requests accepted
- Community support available

---

**Version**: 2.1.0 (Execution Plan Analysis Edition)  
**Last Updated**: December 2024  
**Compatibility**: Databricks SQL Warehouse, Databricks Notebooks
