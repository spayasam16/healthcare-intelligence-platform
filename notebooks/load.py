# Databricks notebook source
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window


# COMMAND ----------

def load_to_gold(stage_table, gold_table, key_columns):
   
    # Step 1: Get max update_dt from gold
    gold_df = spark.table(gold_table)
    if gold_df.isEmpty():
        max_update_dt = '1900-01-01'
    else:
        max_update_dt = gold_df.agg({"update_dt": "max"}).collect()[0][0]

    # Step 2: Read incremental records from stage
    stage_df = spark.table(stage_table).filter(col("update_dt") > max_update_dt)
    stage_df = stage_df \
        .withColumn("create_dt", current_timestamp()) \
        .withColumn("update_dt", current_timestamp())

    stage_df.createOrReplaceTempView("source_view")

    if stage_df.isEmpty():
        print(f"✅ No new records to load for {stage_table}.")
        return

      # Build merge condition dynamically
    cond = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])

    update_set = ", ".join([f"t.{col} = s.{col}" for col in stage_df.columns if col != "create_dt"])
    insert_cols = ", ".join(stage_df.columns)
    insert_vals = ", ".join([f"s.{col}" for col in stage_df.columns])

    merge_sql = f"""
        MERGE INTO {gold_table} t
        USING source_view s
        ON {cond}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)
    print(f"✅ Gold Table Updated: {gold_table}")

# COMMAND ----------

def deduplicate_gold_table(table_name, match_columns):
 
    # Load table
    df = spark.table(table_name)

    # Convert comma-separated to list
    match_cols = match_columns

    # Add ranking window
    window_spec = Window.partitionBy(*match_cols).orderBy(col("update_dt").desc())
    df = df.withColumn("rnk", row_number().over(window_spec))

    # Keep only rnk = 1
    deduped_df = df.filter(col("rnk") == 1).drop("rnk")

    # Overwrite back to same table
    deduped_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"✅ De-duplication complete: {table_name}")

# COMMAND ----------

def publish_to_bi(table_name, sql_txt):
    del_query = f"DELETE FROM {table_name}"
    spark.sql(del_query)
    spark.sql(sql_txt.format(table_name=table_name))

    print(f"✅ Publish to BI complete: {table_name}")

# COMMAND ----------

def job_run(job):
    configs = spark.table("workspace.healthintel360.main_pipeline_config").filter("layer = 'gold'").filter(f"job_typ = '{job}'").filter("is_active = 'Y'").orderBy('seq_no').collect()

    cnt = len(configs)
    print(f"Started job {job} with Tables: {cnt}")

    for row in configs:
        key_cols = [col.strip() for col in row['ref_table'].split(",")]
        source_name = f"{row.catalog_name}.{row.schema_name}.{row.source}"
        target_name = f"{row.catalog_name}.{row.schema_name}.{row.target}"
        if job == 'ingest':
            print(f"🧽 Inserting to Gold Tables: [{cnt}]/[{len(configs)}]")
            load_to_gold(source_name, target_name, key_cols)
            cnt = cnt - 1
        elif job == 'dedup':
            print(f"🧽 Deduplicating Tables: [{cnt}]/[{len(configs)}]")
            deduplicate_gold_table(target_name, key_cols)
            cnt = cnt - 1
        else:
            print(f"Invalid Job: {job}")

# COMMAND ----------

def load_bi(job):
    configs = spark.table("workspace.healthintel360.sql_repos").filter(f"name = '{job}'").filter("is_active = 'Y'").orderBy('seq_no').collect()

    cnt = len(configs)
    print(f"Started job {job} with Tables: {cnt}")

    for row in configs:
        table_name = row.table_name
        sql_txt = row.sql_txt
        print(f"🧽 Updating BI Tables: [{cnt}]/[{len(configs)}]")
        publish_to_bi(table_name, sql_txt)
        cnt = cnt - 1


# COMMAND ----------

job = ['ingest', 'dedup', 'Load BI Table']
for i in job:
    if i == 'Load BI Table':
        load_bi(i)
    else:
        job_run(i)