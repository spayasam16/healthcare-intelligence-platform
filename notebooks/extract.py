# Databricks notebook source
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def FileToTable(fmt, header, inferSchema, file_path, table_name):
    try:
        print(f"📥 Starting ingestion for: {table_name}")
        # Try reading the file
        df_raw = spark.read.option("header", header).option("inferSchema", inferSchema)
        if fmt == "csv":
            df_raw = df_raw.csv(file_path)
        elif fmt == "json":
            df_raw = df_raw.option("multiline", True).json(file_path)
        else:
            raise ValueError(f"❌ Unsupported format: {fmt}")

        # Get common columns and cast
        target_cols = [f.name for f in spark.table(table_name).schema]
        common_cols = [c for c in df_raw.columns if c in target_cols]
        df_casted = df_raw.select([df_raw[c].cast("string").alias(c) for c in common_cols])

        # Truncate and load
        spark.sql(f"TRUNCATE TABLE {table_name}")
        df_casted.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"✅ Loaded: {table_name} ({len(common_cols)} columns)")

    except AnalysisException as e:
        print(f"⚠️ Skipped: {table_name} — File missing or unreadable ({file_path})")

# COMMAND ----------

configs = spark.sql("""SELECT * FROM workspace.healthintel360.pipeline_config WHERE layer = 'bronze' AND is_active = 'Y' ORDER BY seq_no""").collect()
total = len(configs)

# COMMAND ----------

print(f"📦 Starting Bronze Ingestion | Total Tables: {total}")
for idx, row in enumerate(configs, 1):
    print(f"\n🔹 [{idx}/{total}] Processing")
    FileToTable(row.file_format, row.has_header, row.infer_schema, row.file_path,
                f"{row.catalog_name}.{row.schema_name}.{row.table_name}")