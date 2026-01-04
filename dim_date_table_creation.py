# Databricks notebook source
from pyspark.sql import functions as F

# Define the project's data timeline (Historical backfill through incremental updates)
# The project range covers the merger period from 2024 through the end of 2025 [3]
start_date = '2024-01-01'
end_date = '2025-12-01'

# COMMAND ----------

# 1️⃣ Generate a time-series sequence
# We use 'sequence' to create an array of dates at 1-month intervals and 'explode' 
# to flatten that array into individual rows for the data frame [3].
df = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 month
            )
        ) AS month_start_date
    """)
)

# COMMAND ----------

# Preview the initial monthly sequence
display(df)

# COMMAND ----------

# 2️⃣ Derive Analytical Attributes
# These columns allow the BI layer to aggregate metrics like revenue and quantity 
# by different time grains (Year, Quarter, Month) [4], [5].
df = (
    # Create a primary key (yyyyMMdd format) to link with fact tables [4]
    df.withColumn('date_key', F.date_format('month_start_date', 'yyyyMMdd').cast('int'))
      .withColumn('month', F.month('month_start_date'))
      .withColumn('year', F.year('month_start_date'))
      # Format dates into human-readable strings (e.g., "January") for dashboarding [4]
      .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
      .withColumn('month_short_name', F.date_format('month_start_date', 'MMM'))
      # Generate fiscal/quarterly markers (e.g., "Q1", "2024-Q1") for high-level reporting [4]
      .withColumn('quarter', F.quarter('month_start_date'))
      .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
      .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
      
)

# Preview the enriched dimension table
display(df)

# COMMAND ----------

# 3️⃣ Persistence to Gold Layer
# We save this as a Delta table in the Gold schema to provide a BI-ready 
# source for tools like Databricks Dashboards and Genie AI [1], [4].
df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("fmcg.gold.dim_date")