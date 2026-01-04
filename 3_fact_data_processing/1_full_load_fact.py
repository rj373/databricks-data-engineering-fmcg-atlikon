# Databricks notebook source
# 1️⃣ INITIALIZATION & CONFIGURATION
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Importing environment variables (schemas) from the utilities notebook [3, 4]
# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# Parameterizing catalog and data source via widgets for environment flexibility [3, 5]
dbutils.widgets.text("catalog","fmcg", "CATALOG") 
dbutils.widgets.text("data_source","orders" ,"DATA SOURCE")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

# Defining storage paths: landing for new files, processed for archiving [2, 6]
base_path = f's3://sportbar-rakeshjain/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

# Defining target table names following Medallion architecture [6, 7]
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# 2️⃣ BRONZE LAYER: RAW INGESTION
# Reading all CSVs in the landing path and enriching with metadata for lineage [7, 8]
df = spark.read.options(header=True).csv(f"{landing_path}/*.csv") \
    .withColumn("read_timestamp", F.current_timestamp()) \
    .select("*", "_metadata.file_name", "_metadata.file_size")

# Appending raw data into the Bronze Delta table with Change Data Feed enabled [7, 9]
df.write.format("delta").mode("append").option("delta.enableChangeDataFeed", "true").saveAsTable(bronze_table)

# ARCHIVING: Moving ingested files to 'processed' to avoid reprocessing in batch [10, 11]
files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path, f"{processed_path}/{file_info.name}", True)

# 3️⃣ SILVER LAYER: CLEANSING & TRANSFORMATIONS
df_silver = spark.sql(f"Select * from {bronze_table}")

# Handling "data chaos": filtering null quantities and sanitizing customer IDs [10, 12]
df_silver = df_silver.filter(F.col("order_qty").isNotNull())
df_silver = df_silver.withColumn('customer_id', 
    F.when(F.col('customer_id').rlike(r'^\d+$'), F.col('customer_id'))
    .otherwise(F.lit('999999').cast('string')))

# Normalizing Dates: Removing weekday names (e.g., "Tuesday") and parsing formats [13-15]
df_silver = df_silver.withColumn("order_placement_date", 
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", ""))

df_silver = df_silver.withColumn("order_placement_date", 
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy")
    ))

# Deduplication and schema alignment for product IDs [16, 17]
df_silver = df_silver.dropDuplicates(['order_id','order_placement_date','customer_id','product_id','order_qty'])
df_silver = df_silver.withColumn('product_id', F.col('product_id').cast('string'))

# ENRICHMENT: Joining with products to attach the unified 'product_code' surrogate key [18, 19]
df_products = spark.table('fmcg.silver.products')
df_joined = df_silver.join(df_products, on='product_id', how='inner') \
    .select(df_silver["*"], df_products["product_code"])

# Upsert (Merge) into Silver table to maintain data integrity [18, 20, 21]
if not(spark.catalog.tableExists(silver_table)):
    df_joined.write.format('delta').option('delta.enableChangeDataFeed', 'true') \
        .option('mergeschema', 'true').mode('overwrite').saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), 
        "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# 4️⃣ GOLD LAYER: CHILD CONSOLIDATION
df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

# Saving to child-specific fact table [22, 23]
if not (spark.catalog.tableExists(gold_table)):
    df_gold.write.format("delta").option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), 
        "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# 5️⃣ PARENT CONSOLIDATION: MONTHLY AGGREGATION
# Parent table requires monthly granularity; child has daily. Aggregating here [24-26]
df_gold_child = (df_gold.withColumn("month_start", F.trunc("date", "MM"))
                .groupBy("month_start", "product_code", "customer_code")
                .agg(F.sum("sold_quantity").alias("sold_quantity"))
                .withColumnRenamed("month_start", "date"))

# Merging child monthly totals into the Atlikon parent fact_orders [24, 27, 28]
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_gold_child.alias("child_gold"), 
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()