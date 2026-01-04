# Databricks notebook source
# 1️⃣ SETUP & LIBRARIES
# Importing core Spark SQL functions, Delta Table for Medallion architecture, and Window functions for ranking.
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------
# Importing global schema variables (bronze, silver, gold) from the central utilities module.
# This ensures that the Atlikon and Sports Bar schemas remain consistent across the environment.
# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------
# Verifying that schemas are correctly imported for use in table paths.
print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------
# Initializing widgets to parameterize the Catalog and Data Source.
# This modular approach allows the script to be reused for different datasets.
dbutils.widgets.text("catalog", "fmcg", "CATALOG")
dbutils.widgets.text("data_source", "gross_price", "DATA SOURCE")

# COMMAND ----------
# Retrieving widget values into variables for directory and table naming.
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
print(catalog, data_source)

# COMMAND ----------
# 2️⃣ BRONZE LAYER (RAW INGESTION)
# Defining the landing path in S3 for Sports Bar's gross pricing CSV files.
base_path = f's3://sportbar-rakeshjain/{data_source}/*.csv'
print(base_path)

# COMMAND ----------
# Reading raw CSV pricing data from S3. 
# We add metadata (timestamp, filename, file size) to maintain full data lineage and auditability [1].
df = (spark.read.format('csv')
      .option('header', True)
      .option('inferSchema', True)
      .load(base_path)
      .withColumn('read_timestamp', F.current_timestamp())
      .select('*', '_metadata.file_name', '_metadata.file_size')
     )

display(df.limit(10))

# COMMAND ----------
# Writing raw pricing data to the Bronze table in Delta format with Change Data Feed (CDF) enabled [2].
df.write\
  .format("delta") \
  .option("delta.enableChangeDataFeed", "true") \
  .mode("overwrite") \
  .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------
# 3️⃣ SILVER LAYER (CLEANSING & NORMALIZATION)
# Loading raw data from the Bronze layer to address non-uniformity in the Sports Bar dataset [2].
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")

# COMMAND ----------
# TRANSFORMATION: Temporal Normalization
# Sports Bar's dates are inconsistent. This logic uses 'coalesce' and 'try_to_date' to 
# iteratively test different date formats until a uniform 'yyyy-MM-dd' is achieved [2, 3].
df_silver = df_bronze.withColumn("month", 
    F.coalesce(
        F.try_to_date(F.col("month"), "yyyy/MM/dd"),
        F.try_to_date(F.col("month"), "MM/dd/yyyy"),
        F.try_to_date(F.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------
# TRANSFORMATION: Pricing Logic & Data Quality
# 1. Negative Prices: Multiplied by -1 to correct "fat-fingered" input errors.
# 2. Unknowns: Replaces non-numeric strings with 0 as per business manager confirmation [4].
df_silver = df_silver.withColumn("gross_price", 
    F.when(F.col("gross_price").cast("double").isNotNull(),
           F.when(F.col("gross_price") < 0, F.col("gross_price") * -1)
           .otherwise(F.col("gross_price")))
    .otherwise(F.lit(0))
)

# COMMAND ----------
# TRANSFORMATION: Key Enrichment
# Joining with the products table to retrieve the high-quality SHA-2 'product_code'.
# This replaces the unreliable 'product_id' as the primary key for the Gold layer [5].
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_silver = df_silver.join(df_products, "product_id", "left").select(
    "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size"
)

# Saving the cleansed pricing data to the Silver layer.
df_silver.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------
# 4️⃣ GOLD LAYER (AGGREGATION & RANKING)
# Atlikon reports pricing yearly, while Sports Bar reports monthly.
# We must extract the latest month's price to represent the yearly value [6].
df_gold = spark.table(f"{catalog}.{silver_schema}.{data_source}")

# Saving to the child-specific Gold table first [7].
df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------
# 5️⃣ UPSERT INTO ATLIKON MASTER PRICING
# Loading the child gold pricing and adding analytical flags.
df_gold_price = spark.table(f"{catalog}.{gold_schema}.sb_dim_gross_price")

# Deriving the 'Year' and flagging zero-price records for special handling [7].
df_gold_price = (df_gold_price.withColumn('year', F.year('month'))
                 .withColumn('is_zero', F.when(F.col('gross_price') == 0, 1).otherwise(0)))

# COMMAND ----------
# TRANSFORMATION: Window Ranking
# For each product/year combination, we rank rows by month to find the most recent price [6, 8].
window_spec = Window.partitionBy("product_code", "year").orderBy(F.col("month").desc())
df_ranked = df_gold_price.withColumn("rank", F.rank().over(window_spec))

# Filter for rank 1 (the latest month in that year) and cast 'year' to string to match Atlikon's schema [9].
df_gold_latest_price = (df_ranked.filter(F.col("rank") == 1)
                        .select(F.col("product_code"), 
                                F.col("gross_price").alias("price_inr"), 
                                F.col("year").cast("string")))

# COMMAND ----------
# CONSOLIDATION: Delta Merge
# Merging the latest Sports Bar prices into Atlikon's master dim_gross_price table [9].
delta_table = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_gross_price")

delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()
