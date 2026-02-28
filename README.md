# PySpark Day 2 — DataFrame Basics (Parquet Pipeline)

This project demonstrates **Day 2 PySpark fundamentals** with a real ETL-style workflow:

* Read Kaggle CSV
* Infer schema
* DataFrame transformations
* Filtering
* Writing output as **Parquet**
* Windows Hadoop (`winutils.exe`) integration

---

## 📁 Project Structure

```text
pyspark-Day2/
│
├── data/
│   └── kaggle_dataset.csv
│
├── warehouse/
│   └── day2_parquet/
│
├── dataframes_basics.py
├── day2_parquet_pipeline.py
└── spark-env/
```

---

## ⚙️ Requirements

* Python 3.9+
* Java 11
* PySpark
* Hadoop binaries (Windows)

---

## 🔧 Environment Setup

### 1. Virtual Environment

```bash
python -m venv spark-env
spark-env\Scripts\activate
pip install pyspark==3.5.1
```

---

## 🪟 Windows Hadoop Setup (Mandatory for Parquet Write)

### Hadoop Path

```text
bin/winutils.exe
bin/hadoop.dll
```

---

## 🌱 Environment Variables

Set **System Variables**:

Add to **PATH**:

Restart system after setting variables.

---

## ✅ Verification

```bash
echo %HADOOP_HOME%
where winutils
winutils
```

Expected output: No errors.

---

## ▶️ Run Pipeline

```bash
spark-env\Scripts\activate
python day2_parquet_pipeline.py
```

---

## 📦 Output

```text
warehouse/day2_parquet/
├── part-00000-xxxx.snappy.parquet
├── part-00001-xxxx.snappy.parquet
└── _SUCCESS
```

---

## 🔍 Read Parquet Output

### Using Spark

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("warehouse/day2_parquet")
df.show(20)
```

---

## 🎯 Learning Objectives

* Distributed CSV ingestion
* Schema inference
* DataFrame API usage
* Column transformation
* Filtering
* Parquet storage format
* Windows Spark-Hadoop integration

---

## 🧠 Data Engineering Flow

```text
CSV → Spark Read → Schema → Transform → Filter → Parquet Write → Analytics / BI / ML
```

## 🚀 Professional Note

Parquet is the **industry-standard analytics format** used by:

* Spark
* Databricks
* Hive
* AWS Athena
* BigQuery
* Snowflake

