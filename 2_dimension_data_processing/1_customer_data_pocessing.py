# Databricks notebook source
# 1️⃣ SETUP & CONFIGURATION
# Importing Spark SQL functions and Delta Table for the Medallion architecture processing [1].
from pyspark.sql import functions as F
from delta.tables import *

# COMMAND ----------
# Importing global variables (bronze_schema, silver_schema, gold_schema) from the utilities notebook
# This ensures consistency and scalability across the Atlikon project [1, 2].
# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------
# Debugging check to verify schemas are correctly loaded from utilities [1].
print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------
# Creating Databricks Widgets to parameterize the catalog and data source names.
# This allows the same code to be reused for different datasets or environments (dev/prod) [1, 3].
dbutils.widgets.text("catalog", "fmcg", "CATALOG")
dbutils.widgets.text("data_source", "customers", "DATA SOURCE")

# COMMAND ----------
# Retrieving the widget values into variables for the S3 path and table names [4, 5].
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
print(catalog, data_source)

# COMMAND ----------
# 2️⃣ BRONZE LAYER (RAW INGESTION)
# Defining the landing zone path in S3. Using a wildcard (*) to ingest all CSV files in the folder [4, 5].
base_path = f's3://sportbar-rakeshjain/{data_source}/*.csv'
print(base_path)

# COMMAND ----------
# Reading the raw CSV data with schema inference enabled. 
# We add metadata columns (_metadata.file_name and _metadata.file_size) for data lineage and auditing [5, 6].
df = (spark.read.format('csv')
      .option('header', True)
      .option('inferSchema', True)
      .load(base_path)
      .withColumn('read_timestamp', F.current_timestamp())
      .select('*', '_metadata.file_name', '_metadata.file_size')
     )

display(df.limit(10))

# COMMAND ----------
# Writing the raw data to the Bronze table in Delta format.
# 'delta.enableChangeDataFeed' is enabled to track row-level changes for future auditing [7, 8].
df.write\
  .format('delta')\
  .option('delta.enableChangeDataFeed', True)\
  .mode('overwrite')\
  .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------
# 3️⃣ SILVER LAYER (DATA CLEANING)
# Loading the Bronze data into a data frame to begin cleansing the "messy" Sports Bar data [8, 9].
df_bronze = spark.sql(f'select * from {catalog}.{bronze_schema}.{data_source};')
display(df_bronze.limit(10))

# COMMAND ----------
# Identifying duplicate customers to ensure the "reliability" of the pipeline [8, 10].
duplicate_customers = (
    df_bronze.groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)
display(duplicate_customers)

# COMMAND ----------
# Removing duplicate records by customer_id to ensure a clean dimension table [11, 12].
print('Rows before deduplication: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after deduplication: ', df_silver.count())

# COMMAND ----------
# Fixing "fat-fingering" errors where leading/trailing spaces exist in names [12, 13].
df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name")))

# COMMAND ----------
# 4️⃣ GEOGRAPHIC NORMALIZATION
# Addressing city typos (e.g., "Hyderabadd") by mapping them to standardized names [14, 15].
city_map = {
    "NewDelhee": "New Delhi", "NewDelhi": "New Delhi", "NewDheli": "New Delhi",
    "Bengaluruu": "Bengaluru", "Bangalore": "Bengaluru", "Bengalore": "Bengaluru",
    "Hyderabadd": "Hyderabad", "Hyderbad": "Hyderabad"
}
allowed = ["New Delhi", "Bengaluru", "Hyderabad"]

# Replacing mapped cities and setting non-allowed/garbage cities to Null for manual fixing [15, 16].
df_silver = (df_silver
             .replace(city_map, "city")
             .withColumn("city",
                         F.when(F.col("city").isNull(), None)
                         .when(F.col("city").isin(allowed), F.col("city"))
                         .otherwise(None)))

# COMMAND ----------
# Applying initcap to standardize casing (e.g., "nutrition" becomes "Nutrition") [17, 18].
df_silver = df_silver.withColumn("customer_name", F.initcap(F.col("customer_name")))

# COMMAND ----------
# Solving missing data: Creating a "fix" dataframe for known customers with missing cities, 
# verified through business collaboration with Sports Bar managers [19, 20].
customer_city_fix = {
    789403: "New Delhi", 789420: "Bengaluru", 
    789521: "Hyderabad", 789603: "Hyderabad"
}
df_fix = spark.createDataFrame([(k, v) for k, v in customer_city_fix.items()], ["customer_id", "fixed_city"])

# Joining the fix data and using coalesce to fill Nulls in the original city column [20, 21].
df_silver = (df_silver
             .join(df_fix, "customer_id", "left")
             .withColumn("city", F.coalesce("city", "fixed_city"))
             .drop("fixed_city"))

# COMMAND ----------
# 5️⃣ SCHEMA ALIGNMENT WITH ATLIKON
# Casting customer_id to String. Atlikon treats IDs as categorical data (not arithmetic) [22-24].
df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))

# COMMAND ----------
# Constructing a unified 'customer' column and adding static attributes (market, platform, channel)
# to match the parent company's (Atlikon) data model for unified analytics [25-27].
df_silver = (df_silver
             .withColumn("customer", F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown"))))
             .withColumn("market", F.lit("India"))
             .withColumn("platform", F.lit("Sports Bar"))
             .withColumn("channel", F.lit("Acquisition"))
            )

# COMMAND ----------
# Saving the cleansed data to the Silver layer [27, 28].
df_silver.write\
  .format('delta')\
  .option("mergeSchema", "true")\
  .option('delta.enableChangeDataFeed', 'true')\
  .mode("overwrite")\
  .saveAsTable(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------
# 6️⃣ GOLD LAYER (CONSOLIDATION)
# Selecting target columns and saving to the child-specific Gold table [28, 29].
df_silver = spark.read.table(f'{catalog}.{silver_schema}.{data_source}')
df_gold = df_silver.select('customer_id', 'customer_name', 'city', 'customer', 'market', 'platform', 'channel')

df_gold.write\
  .format('delta')\
  .option('delta.enableChangeDataFeed', 'true')\
  .mode("overwrite")\
  .saveAsTable(f'{catalog}.{gold_schema}.sb_dim_{data_source}')

# COMMAND ----------
# 7️⃣ UPSERT INTO CONSOLIDATED PARENT TABLE
# Merging Sports Bar data into Atlikon's main dim_customers table.
# If customer_code exists, it updates; if not, it inserts new records (SCD Type 1 logic) [30, 31].
delta_table = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.dim_customers')
df_child_customers = (spark.table(f'{catalog}.{gold_schema}.sb_dim_{data_source}')
                      .select(F.col("customer_id").alias("customer_code"), 'customer', 'market', 'platform', 'channel'))

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
