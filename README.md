🚀 Atlikon & Sports Bar Data Integration Project

🏗️ Technical Architecture

The project follows the Medallion Architecture, moving data through three distinct stages to ensure quality and reliability:

• Bronze (Raw): Ingests data exactly as it arrives from AWS S3 with added metadata (file names, timestamps) for auditing.

• Silver (Cleansed): The "transformation" zone where data is deduplicated, typos are fixed, and inconsistent formats are standardized.

• Gold (Curated): The "BI-ready" layer where data is aggregated and merged into the final consolidated tables used by leadership.

--------------------------------------------------------------------------------
🛠️ Implementation Steps

1. Setup & Infrastructure
• Catalog & Schemas: Established an FMCG catalog with Bronze, Silver, and Gold schemas to organize the data.
• S3 Connection: Linked Databricks to an AWS S3 bucket, creating a "data lake" to store raw CSV files from Sports Bar.
• Utilities: Created a centralized configuration notebook to manage schema names across all pipelines, ensuring the code is easy to maintain.

2. Dimension Data Processing (The "Ingredients")
Dimension tables provide the context (Who, What, Where) for business transactions.
• Customer Pipeline:
    ◦ Fixed "fat-fingering" errors in city names (e.g., mapping "Hyderabadd" to "Hyderabad").
    ◦ Standardized customer names using initcap and trimmed leading spaces.
    ◦ Merged Sports Bar records into the master dim_customers table using an Upsert (Merge) operation.
   
• Product Pipeline:
    ◦ Corrected common typos like "Protien" to "Protein" using Regular Expressions.
    ◦ Generated a unique product_code using SHA-2 hashing to replace unreliable IDs.
    ◦ Extracted product "variants" (e.g., 60g) from product names into a separate column.
   
• Pricing Pipeline:
    ◦ Normalized various date formats (e.g., dd/MM/yyyy, yyyy-MM-dd) into a uniform standard.
    ◦ Handled negative prices and "Unknown" values by converting them to absolute numbers or zero.
    ◦ Used a Window Function to extract the latest monthly price for products to match Atlikon’s yearly reporting model.
   
3. Fact Data Processing (The "Logbook")
This pipeline handles high-volume sales transactions across two phases:
• Historical Full Load: Ingested five months of historical data (July–November). Once ingested, raw files were moved from a landing/ folder to a processed/ folder to prevent reprocessing.
• Incremental Load: Implemented a Staging Table pattern to process new daily files (December).
• Granularity Alignment: Since Sports Bar provides daily data and Atlikon requires monthly reporting, the pipeline aggregates daily sales into a single "Month Start" record before merging into the master fact table.

--------------------------------------------------------------------------------
📊 Business Intelligence & Orchestration
• Orchestration: All pipelines were organized into a Databricks Job, scheduled to run nightly at 11:00 PM to ensure data is updated automatically as soon as the business day concludes.
• Unified View: Created a "denormalized" view—one massive table containing all information—to make dashboarding fast and simple for analysts.
• AI Analytics (Genie): Leveraged Databricks Genie AI to allow executives to ask natural language questions (e.g., "What are the top 5 products by revenue?") and receive instant visualizations.

--------------------------------------------------------------------------------
Analogy for the Project
Think of this project like a Pizza Store merger. Atlikon is a world-class restaurant with high standards, while Sports Bar is a small shop providing ingredients in messy boxes. As Data Engineers, our job is to act as the prep crew: we wash the vegetables (Cleansing), standardize the labels (Normalization), and provide the Chef (AI Engineers and Analysts) with high-quality ingredients so they can make the perfect pizza (Insights)
