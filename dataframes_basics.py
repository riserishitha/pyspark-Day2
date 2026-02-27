# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper

# # -------------------------------
# # 1. Spark Session
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("Day2-DataFrame-Basics") \
#     .getOrCreate()

# print("\n=== Spark Session Started ===\n")

# # -------------------------------
# # 2. Read Kaggle CSV
# # -------------------------------
# DATA_PATH = "data/kaggle_data_set.csv"   # change this path

# df = spark.read.csv(
#     DATA_PATH,
#     header=True,
#     inferSchema=True
# )

# print("\n=== Raw Data Sample ===")
# df.show(10, truncate=False)

# # -------------------------------
# # 3. Schema Inspection
# # -------------------------------
# print("\n=== DataFrame Schema ===")
# df.printSchema()

# print("\n=== Column Names ===")
# print(df.columns)

# # -------------------------------
# # 4. Basic Select Operations
# # -------------------------------

# # Single column
# print("\n=== Single Column Select ===")
# df.select(df.columns[0]).show(5)

# # Multiple columns (first 3 columns dynamically)
# print("\n=== Multiple Column Select ===")
# df.select(df.columns[:3]).show(5)

# # -------------------------------
# # 5. Select with Alias
# # -------------------------------
# print("\n=== Select with Alias ===")
# df_alias = df.select(
#     col(df.columns[0]).alias("renamed_col1"),
#     col(df.columns[1]).alias("renamed_col2")
# )
# df_alias.show(5)

# # -------------------------------
# # 6. Select with Transformation
# # -------------------------------
# # Only if column is string-type
# string_cols = [c for c, t in df.dtypes if t == "string"]

# if string_cols:
#     print("\n=== Select with Transformation (upper) ===")
#     df.select(
#         col(string_cols[0]),
#         upper(col(string_cols[0])).alias(f"{string_cols[0]}_upper")
#     ).show(5)

# # -------------------------------
# # 7. Select + Filter
# # -------------------------------
# # Only if numeric column exists
# numeric_cols = [c for c, t in df.dtypes if t in ("int", "double", "float", "bigint")]

# if numeric_cols:
#     print("\n=== Select + Filter ===")
#     df.select(numeric_cols[0]) \
#       .filter(col(numeric_cols[0]) > 0) \
#       .show(5)

# # -------------------------------
# # 8. Final Clean Select (ETL style)
# # -------------------------------
# print("\n=== Final Clean Dataset (ETL-style output) ===")

# selected_cols = df.columns[:4]   # choose first 4 columns
# final_df = df.select(*selected_cols)

# final_df.show(10)

# print("\n=== Day-2 Pipeline Completed Successfully ===\n")

# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
import os

# -------------------------------
# Config
# -------------------------------
DATA_PATH = "data/kaggle_data_set.csv"     # input CSV
PARQUET_PATH = "warehouse/parquet_data"   # output parquet folder

# -------------------------------
# Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("Day2-Parquet-Pipeline") \
    .getOrCreate()

print("\n=== Spark Session Started ===\n")

# -------------------------------
# Read CSV
# -------------------------------
df = spark.read.csv(
    DATA_PATH,
    header=True,
    inferSchema=True
)

# -------------------------------
# Schema
# -------------------------------
print("\n=== Schema ===")
df.printSchema()

# -------------------------------
# Column classification
# -------------------------------
string_cols = [c for c, t in df.dtypes if t == "string"]
numeric_cols = [c for c, t in df.dtypes if t in ("int", "double", "float", "bigint")]

# -------------------------------
# Transformations
# -------------------------------
base_cols = df.columns[:4] if len(df.columns) >= 4 else df.columns
base_df = df.select(*base_cols)

# Uppercase transformation (string col)
if string_cols:
    transform_df = base_df.withColumn(
        f"{string_cols[0]}_upper",
        upper(col(string_cols[0]))
    )
else:
    transform_df = base_df

# Numeric filter
if numeric_cols:
    final_df = transform_df.filter(col(numeric_cols[0]) > 0)
else:
    final_df = transform_df

# -------------------------------
# Write Parquet Only
# -------------------------------
final_df.write.mode("overwrite").parquet(PARQUET_PATH)

# -------------------------------
# Preview
# -------------------------------
print("\n=== Sample Output ===")
final_df.show(10, truncate=False)

print("\n=== Parquet Written To ===")
print(PARQUET_PATH)

print("\n=== Pipeline Completed Successfully ===\n")

spark.stop()