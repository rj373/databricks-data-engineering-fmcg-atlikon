📂 Dimension Data Processing: Pipeline Overview
The dimension pipelines ensure that Atlikon leadership can view aggregated analytics across both companies in a single dashboard. Each pipeline follows a standardized flow: ingesting from S3, applying business rules, and performing an Upsert (Merge) into the master Atlikon tables.
👥 1. Customer Data Pipeline (customer_data_processing)
This pipeline standardizes customer information to resolve inconsistencies in reporting and formatting.
• Bronze Layer: Ingests raw CSV files from the customers S3 path, adding metadata such as read_timestamp and file_name for auditability.
• Silver Layer:
    ◦ Deduplication: Removes duplicate records based on customer_id.
    ◦ Data Cleaning: Trims leading/trailing spaces from customer_name and applies initcap for consistent casing.
    ◦ Geographic Normalization: Maps various typos (e.g., "NewDelhee", "Hyderabadd") to standardized city names (New Delhi, Bengaluru, Hyderabad).
    ◦ Business Fixes: Manually applies city names to specific customer IDs confirmed by the business team.
• Gold Layer:
    ◦ Schema Alignment: Casts customer_id to a string to match the Atlikon model.
    ◦ Unified Attributes: Creates a customer column (concatenating Name and City) and adds static attributes: Market (India), Platform (Sports Bar), and Channel (Acquisition).
• Final Consolidation: Merges the processed data into the master dim_customers table using a merge condition on customer_code.
📦 2. Product Data Pipeline (product_data_processing)
This script bridges the gap between different product naming conventions and category structures.
• Bronze Layer: Raw ingestion of product CSVs with Change Data Feed (CDF) enabled to track row-level changes.
• Silver Layer:
    ◦ Correction Logic: Fixes "fat-fingering" typos like "Protien" to "Protein" in both categories and product names.
    ◦ Division Mapping: Maps Sports Bar categories (e.g., "Energy Bars") to Atlikon’s high-level divisions (e.g., "Nutrition Bars").
    ◦ Variant Extraction: Uses regex to extract product weight/size (e.g., "60g") from the product name string.
    ◦ Deterministic Key: Generates a SHA-2 hashed product_code from the product name to serve as a reliable surrogate key.
• Gold Layer: Selects specific columns (product_code, division, category, product, variant) and saves them as a child-specific dimension table.
• Final Consolidation: Upserts the new product codes into Atlikon’s dim_products master table.
💰 3. Pricing Data Pipeline (gross_price_data_processing)
This pipeline reconciles the different pricing cycles: Atlikon’s yearly prices versus Sports Bar’s monthly updates.
• Bronze Layer: Captures raw pricing snapshots from S3.
• Silver Layer:
    ◦ Temporal Normalization: Uses coalesce and try_to_date to fix inconsistent date formats (e.g., yyyy/MM/dd and dd-MM-yyyy) into a uniform yyyy-MM-dd.
    ◦ Price Cleaning: Converts negative prices to absolute values and replaces "Unknown" strings with zero.
    ◦ Key Enrichment: Joins with the Silver Product table to append the unified product_code.
• Gold Layer (Monthly to Yearly Logic):
    ◦ Because Atlikon reports pricing by Year, the pipeline uses a Window Function to partition data by product_code and Year.
    ◦ It ranks records by the latest month and extracts the most recent price to represent that year’s value in the master table.
• Final Consolidation: Merges these yearly representations into the master dim_gross_price table
