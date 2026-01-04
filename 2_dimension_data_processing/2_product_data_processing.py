# Databricks notebook source
# 1️⃣ SETUP & LIBRARIES
# Importing core Spark SQL functions and Delta Table for Medallion architecture processing [3].
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------
# Importing global schema variables (bronze, silver, gold) from the central utilities module.
# This ensures that all notebooks in the Atlikon pipeline use consistent schema references [3, 4].
# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------
# Debugging check to confirm utilities have been loaded successfully [3].
print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------
# Establishing interactive widgets for dynamic catalog and data source selection.
# This makes the pipeline scalable across different environments and datasets [3, 5].
dbutils.widgets.text("catalog", "fmcg", "CATALOG") 
dbutils.widgets.text("data_source", "products", "DATA SOURCE")

# COMMAND ----------
# Retrieving values from the widgets for use in S3 paths and Delta table names [6].
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
print(catalog, data_source)

# COMMAND ----------
# 2️⃣ BRONZE LAYER (RAW INGESTION)
# Defining the S3 landing path where Sports Bar's product CSVs are stored [6].
base_path = f's3://sportbar-rakeshjain/{data_source}/*.csv'
print(base_path)

# COMMAND ----------
# Reading raw CSV files from S3 with schema inference.
# Adding metadata (current timestamp, filename, and file size) to provide full data lineage [6, 7].
df = (spark.read.format('csv')
      .option('header', True)
      .option('inferSchema', True)
      .load(base_path)
      .withColumn('read_timestamp', F.current_timestamp())
      .select('*', '_metadata.file_name', '_metadata.file_size')
     )

display(df.limit(10))

# COMMAND ----------
# Persisting raw data to the Bronze table in Delta format. 
# Change Data Feed (CDF) is enabled to track historical changes for auditing [8, 9].
df.write\
  .format('delta')\
  .option('delta.enableChangeDataFeed', True)\
  .mode('overwrite')\
  .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------
# 3️⃣ SILVER LAYER (CLEANSING & NORMALIZATION)
# Loading the raw data from the Bronze layer for transformation [9].
df_bronze = spark.sql(f'select * from {catalog}.{bronze_schema}.{data_source}')
display(df_bronze.limit(10))

# COMMAND ----------
# TRANSFORMATION: Deduplication
# Dropping duplicate records based on 'product_id' to maintain a unique product dimension [9, 10].
print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------
# TRANSFORMATION: Standardization of Category Names
# Applying 'initcap' to normalize casing (e.g., converting "ENERGY BAR" or "energy bar" to "Energy Bar") [10, 11].
df_silver = df_silver.withColumn('category', F.initcap(F.col('category')))

# COMMAND ----------
# TRANSFORMATION: Typos & Regex Corrections
# Using regular expressions to fix common "fat-fingering" errors like misspelling "Protein" as "Protien" [10, 12].
df_silver = df_silver.withColumn('category', F.regexp_replace(F.col('category'), "(?i)Protien", "Protein")) \
                     .withColumn('product_name', F.regexp_replace(F.col('product_name'), "(?i)Protien", "Protein"))

# COMMAND ----------
# 4️⃣ SCHEMA ENRICHMENT & MAPPING
# Mapping Sports Bar's categories to Atlikon's higher-level 'division' structure.
# This is a critical step for unified analytics across the merged company [13, 14].
df_silver = (
    df_silver
    .withColumn(
        "division",
        F.when(F.col("category") == "Energy Bars", "Nutrition Bars")
        .when(F.col("category") == "Protein Bars", "Nutrition Bars")
        .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
        .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery")
        .when(F.col("category") == "Healthy Snacks", "Healthy Snacks")
        .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
        .otherwise("Other") # [15]
    )
)

# COMMAND ----------
# TRANSFORMATION: Extracting Variants
# Parsing 'variant' information (like size or weight) from the 'product_name' using regex [13, 15].
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)

# COMMAND ----------
# TRANSFORMATION: Deterministic Keys & Surrogate IDs
# 1. Generating a consistent 'product_code' using SHA-2 hashing on the product name [15, 16].
# 2. Cleansing 'product_id': replacing invalid/non-numeric IDs with a fallback value (999999) [17, 18].
# 3. Renaming columns to align with the parent company's (Atlikon) standard data model [18, 19].
df_silver = (
    df_silver
    .withColumn("product_code", F.sha2(F.col("product_name").cast("string"), 256))
    .withColumn("product_id", 
                F.when(F.col("product_id").cast("string").rlike("^[1, 2, 20-26]+$"), 
                       F.col("product_id").cast("string"))
                .otherwise(F.lit(999999).cast("string")))
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------
# Persisting the cleansed, high-quality data to the Silver layer [27].
df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")
df_silver.write\
  .format("delta") \
  .option("delta.enableChangeDataFeed", "true") \
  .option("mergeSchema", "true") \
  .mode("overwrite") \
  .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------
# 5️⃣ GOLD LAYER (CURATION & BI PREPARATION)
# Selecting target analytical columns for the child company's Gold dimension table [27].
df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")

# Saving the finalized table as 'sb_dim_products' [28].
df_gold.write\
  .format("delta") \
  .option("delta.enableChangeDataFeed", "true") \
  .mode("overwrite") \
  .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------
# 6️⃣ CONSOLIDATION (UPSERT INTO ATLIKON MASTER)
# Merging the new Sports Bar products into the existing Atlikon master product table.
# This provides a single source of truth for the entire company [24, 28].
delta_table = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_products")
df_child_products = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}").select('product_code', 'division', 'category', 'product', 'variant')

# Using Delta Merge logic: Update records if product_code matches, otherwise Insert new ones [29, 30].
delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={"division": "source.division", "category": "source.category", "product": "source.product", "variant": "source.variant"}
).whenNotMatchedInsert(
    values={"product_code": "source.product_code", "division": "source.division", "category": "source.category", "product": "source.product", "variant": "source.variant"}
).execute()
