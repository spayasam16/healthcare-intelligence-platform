# Databricks notebook source
from pyspark.sql.functions import col, trim, lower, upper, initcap, when, regexp_replace, to_timestamp, current_timestamp, concat_ws, collect_list, struct, length, expr, lit

# COMMAND ----------

def app_cleanse(source_name, ref_table, source, target_name):
    df_cln = spark.table(source_name)

    rules_df = spark.table(ref_table).filter(f"table_name = '{source}'").filter("is_active = 'Y'").orderBy('seq_no')

    for row in rules_df.collect():
        col_nm = row.column_name
        cln_typ = (row.cleanse_type).lower()
        if col_nm not in df_cln.columns:
            continue
        if cln_typ == 'trim':
            df_cln = df_cln.withColumn(col_nm, trim(col(col_nm)))
        elif cln_typ == 'lower':
            df_cln = df_cln.withColumn(col_nm, lower(col(col_nm)))
        elif cln_typ == 'upper':
            df_cln = df_cln.withColumn(col_nm, upper(col(col_nm)))   
        elif cln_typ == 'initcap':
            df_cln = df_cln.withColumn(col_nm, initcap(col(col_nm))) 
        elif cln_typ == 'nullify':
            df_cln = df_cln.withColumn(col_nm, when((col(col_nm) == "") | (col(col_nm).isNull()), None).otherwise(col(col_nm)))
        elif cln_typ == 'remove_spcl_charcs':
            df_cln = df_cln.withColumn(col_nm, regexp_replace(col(col_nm), r'[^a-zA-Z0-9\s]', '')) 
        elif cln_typ.startswith("date_format:"):
            date_fmt = cln_typ.split(":")[1]
            df_cln = df_cln.withColumn(col_nm, to_timestamp(col(col_nm), date_fmt))
        else:
            print(f"Invalid cleansing type: {cln_typ}")
    df_cln.write.mode("overwrite").saveAsTable(target_name)
    print(f"✅ Cleaned: {source}")

# COMMAND ----------

def app_standardization(source_name, source, ref_table, target_name):
    df_std = spark.table(source_name)

    # Load standardization mappings from metadata
    meta_df = spark.table(ref_table).filter(
        (col("table_name") == source) & 
        (col("is_active") == "Y")
    ).select("column_name", "actual_value", "std_value").distinct()

    # Group mappings by column
    mappings = meta_df.groupBy("column_name").agg(
        collect_list(struct("actual_value", "std_value")).alias("map_list")
    ).collect()

    for row in mappings:
        col_name = row["column_name"]
        map_list = row["map_list"]

        # Special case: phone formatting
        if col_name == "phone" and any(m["actual_value"] == "__TRIGGER__" for m in map_list):
            df_std = df_std.withColumn(col_name, expr("right(regexp_replace(phone, '[^0-9]', ''), 10)"))
            continue

        condition = None
        for mapping in map_list:
            actual = mapping["actual_value"]
            standard = mapping["std_value"]

            if actual == "__NEGATIVE__":
                new_condition = when(col(col_name) < 0, lit(standard))
            else:
                new_condition = when(col(col_name) == actual, lit(standard))

            if condition is None:
                condition = new_condition
            else:
                condition = condition.when(col(col_name) == actual, lit(standard)) if actual != "__NEGATIVE__" else condition.when(col(col_name) < 0, lit(standard))

        if condition is not None:
            df_std = df_std.withColumn(col_name, condition.otherwise(col(col_name)))

    # Write result to target table
    df_std.write.mode("overwrite").saveAsTable(target_name)
    print(f"✅ Standardized: {source}")

# COMMAND ----------

def apply_dq_rules(source_table, rule_table, target_name, dq_column="dq_chk"):
    # Load source data and rules
    df = spark.table(source_table)
    rules_df = spark.table(rule_table).filter(col("table_name") == source_table.split(".")[-1])

    # Initialize DQ column
    df = df.withColumn(dq_column, lit(""))

    # Apply each rule
    for row in rules_df.collect():
        column = row["column_name"]
        rule_type = row["rule_type"]
        condition = row["rule_condition"]
        rule_msg = row["rule_desc"]

        if rule_type == "NOT_NULL":
            df = df.withColumn(
                dq_column,
                when((col(column).isNull()) | (col(column) == ""),
                     concat_ws(" | ", col(dq_column), lit(rule_msg))
                ).otherwise(col(dq_column))
            )

        elif rule_type == "VALID_SET":
            valid_values = [v.strip() for v in condition.split(",")]
            df = df.withColumn(
                dq_column,
                when(~col(column).isin(valid_values),
                     concat_ws(" | ", col(dq_column), lit(rule_msg))
                ).otherwise(col(dq_column))
            )

        elif rule_type == "REGEX":
            df = df.withColumn(
                dq_column,
                when(~col(column).rlike(condition),
                     concat_ws(" | ", col(dq_column), lit(rule_msg))
                ).otherwise(col(dq_column))
            )

        elif rule_type == "CUSTOM_EXPR":
            expr_condition = f"NOT({condition})"
            df = df.withColumn(
                dq_column,
                when(expr(expr_condition),
                     concat_ws(" | ", col(dq_column), lit(rule_msg))
                ).otherwise(col(dq_column))
            )
    # Write result to target table
    df.write.mode("overwrite").saveAsTable(target_name)
    print(f"✅ DQ Done: {target_name}")

# COMMAND ----------

def merge_to_stage(std_table, stage_table, key_cols):
    df_std = spark.table(std_table).filter(col("dq_chk") == '')

    # Drop dq_chk if exists
    if "dq_chk" in df_std.columns:
        df_std = df_std.drop("dq_chk")
    
    # Add audit columns
    df_std = df_std \
        .withColumn("create_dt", current_timestamp()) \
        .withColumn("update_dt", current_timestamp())
    
    # Create temp view
    df_std.createOrReplaceTempView("source_view")

    # Build ON condition dynamically for merge
    on_condition = " AND ".join([f"tgt.{col} = src.{col}" for col in key_cols])
    

    # Build SET for UPDATE and INSERT
    update_set = ", ".join([f"tgt.{col} = src.{col}" for col in df_std.columns if col != "create_dt"])
    insert_cols = ", ".join(df_std.columns)
    insert_vals = ", ".join([f"src.{col}" for col in df_std.columns])

   

    merge_sql = f"""
        MERGE INTO {stage_table} AS tgt
        USING source_view AS src
        ON {on_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    print(f"✅ Staged: {stage_table}")

# COMMAND ----------

def job_run(job):
    configs = spark.table("workspace.healthintel360.main_pipeline_config").filter("layer = 'silver'").filter(f"job_typ = '{job}'").filter("is_active = 'Y'").orderBy('seq_no').collect()
    
    #configs = spark.sql(f"SELECT * from workspace.healthintel360.main_pipeline_config where layer = 'silver' and job_typ = {job} and is_active = 'Y' order by seq_no asc").collect()

    cnt = len(configs)
    print(f"Started job {job} with Tables: {cnt}")

    for row in configs:
        source = row.source
        ref_table = f"{row.catalog_name}.{row.schema_name}.{row.ref_table}"
        source_name = f"{row.catalog_name}.{row.schema_name}.{row.source}"
        target_name = f"{row.catalog_name}.{row.schema_name}.{row.target}"
        key_cols = [col.strip() for col in row['ref_table'].split(",")]

        if job == 'cleanse':
            print(f"🧽 Cleansing Tables: [{cnt}]/[{len(configs)}]")
            app_cleanse(source_name, ref_table, source, target_name)
            cnt = cnt - 1
        elif job == 'std':
            print(f"🔄 Standardization Tables: [{cnt}]/[{len(configs)}]")
            app_standardization(source_name, source, ref_table, target_name)
            cnt = cnt - 1
        elif job == 'dq':
            print(f"🧪 DQ Check Tables: [{cnt}]/[{len(configs)}]")
            apply_dq_rules(source_name, ref_table, target_name)
            cnt = cnt - 1
        elif job == 'stage':
            print(f"📥 Stage Tables: [{cnt}]/[{len(configs)}]")
            merge_to_stage(source_name, target_name, key_cols)
            cnt = cnt - 1
        else:
            print(f"Invalid Job: {job}")

# COMMAND ----------

jobs = ['cleanse', 'std', 'dq', 'stage']
for i in jobs:
    print(i)
    job_run(job=i)