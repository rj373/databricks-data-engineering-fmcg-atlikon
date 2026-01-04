# Databricks notebook source
# 1️⃣ INITIALIZATION
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportbar-rakeshjain/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# 2️⃣ BRONZE & STAGING
# Ingesting ONLY the newly arrived daily file [31, 32]
df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv") \
    .withColumn("read_timestamp", F.current_timestamp()) \
    .select("*", "_metadata.file_name", "_metadata.file_size")

# Casting for schema consistency [32]
df = (df.withColumn("product_id", F.col("product_id").cast("string"))
      .withColumn("order_qty", F.col("order_qty").cast("string")))

# Appending to main Bronze table and OVERWRITING a staging table for isolated processing [30, 32, 33]
df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("append").saveAsTable(bronze_table)
df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# Moving file to processed to clean landing zone [30, 34, 35]
files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path, f"{processed_path}/{file_info.name}", True)

# 3️⃣ SILVER LAYER (INCREMENTAL TRANSFORMATIONS)
# Processing only the 350-400 records in the staging table to save compute [35, 36]
df_silver = spark.sql(f'select * from {catalog}.{bronze_schema}.staging_{data_source};')

# Applying standard cleansing (identifying nulls, customer_id fixes, date normalization) [35, 37, 38]
df_silver = df_silver.filter(F.col("order_qty").isNotNull())
df_silver = df_silver.withColumn('customer_id', F.when(F.col('customer_id').rlike(r'^\d+$'), F.col('customer_id')).otherwise(F.lit('999999').cast('string')))
df_silver = df_silver.withColumn("order_placement_date", F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", ""))
df_silver = df_silver.withColumn("order_placement_date", F.coalesce(
    F.try_to_date("order_placement_date", "yyyy/MM/dd"),
    F.try_to_date("order_placement_date", "dd-MM-yyyy"),
    F.try_to_date("order_placement_date", "dd/MM/yyyy"),
    F.try_to_date("order_placement_date", "MMMM dd, yyyy")
))

df_silver = df_silver.dropDuplicates(['order_id','order_placement_date','customer_id','product_id','order_qty'])
df_silver = df_silver.withColumn('product_id', F.col('product_id').cast('string'))

# Join with product master for surrogate keys [39]
df_products = spark.table("fmcg.silver.products")
df_joined = df_silver.join(df_products, on="product_id", how="inner").select(df_silver["*"], df_products["product_code"])

# Upsert into main Silver table and save to Silver Staging [39, 40]
silver_delta = DeltaTable.forName(spark, silver_table)
silver_delta.alias("silver").merge(df_joined.alias("bronze"), 
    "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

df_joined.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# 4️⃣ GOLD LAYER (INCREMENTAL)
df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

# Upsert into Child Gold table [41]
gold_delta = DeltaTable.forName(spark, gold_table)
gold_delta.alias("source").merge(df_gold.alias("gold"), 
    "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# 5️⃣ RECALCULATED PARENT MERGE
# To update the monthly total in the parent, we must pull ALL daily records for the current month from child Gold [42-44]
df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")
incremental_month_df = df_child.select(F.trunc("date", "MM").alias("start_month")).distinct()
incremental_month_df.createOrReplaceTempView("incremental_months")

# Fetching all records for the impacted month (e.g., all Dec records) to get an accurate monthly sum [43, 44]
monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m ON trunc(sbf.date, 'MM') = m.start_month
""")

# Re-aggregating daily records into a new monthly total [44-46]
df_monthly_recalc = (monthly_table.withColumn("month_start", F.trunc("date", "MM"))
                    .groupBy("month_start", "product_code", "customer_code")
                    .agg(F.sum("sold_quantity").alias("sold_quantity"))
                    .withColumnRenamed("month_start", "date"))

# Upserting the recalculated monthly total into parent Gold [46, 47]
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), 
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# CLEANUP: Dropping temporary staging tables [48]
spark.sql(f"DROP TABLE {catalog}.{bronze_schema}.staging_orders")
spark.sql(f"DROP TABLE {catalog}.{silver_schema}.staging_orders")