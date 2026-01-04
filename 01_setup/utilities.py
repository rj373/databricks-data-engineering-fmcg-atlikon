# Databricks notebook source

# These variables define the Medallion Architecture layers used throughout the pipeline.
# By centralizing these, we ensure that all notebooks reference the same schema names [1, 2].

# BRONZE: The Raw Ingestion Layer. 
# Stores the initial state of data from S3 before any cleaning [3, 4].
bronze_schema = 'bronze'

# SILVER: The Cleansed & Transformed Layer. 
# Stores data that has been filtered for duplicates, typos, and null values [3, 5].
silver_schema = 'silver'

# GOLD: The Curated BI Layer. 
# Stores final, aggregated tables ready for Dashboards and Genie AI [6, 7].
gold_schema = 'gold'