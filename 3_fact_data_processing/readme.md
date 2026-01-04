📊 Fact Data Processing: Orders Pipeline
This folder contains the core transaction engine for the Atlikon and Sports Bar merger. It is designed to take high-volume daily order data and transform it into a monthly consolidated format for executive reporting.

--------------------------------------------------------------------------------
🏗️ Architecture Overview
The pipeline follows a Medallion Architecture to move transaction data from raw ingestion to a "Single Source of Truth".
1. Bronze (Raw Ingestion): Ingests daily CSV files from the S3 landing zone, adding metadata columns (file_name, read_timestamp) for full auditability.
2. Silver (Transformation): Standardizes "messy" data formats and performs high-integrity joins.
3. Gold (Consolidation): Curates BI-ready daily records for the child company and monthly aggregates for the parent company.

--------------------------------------------------------------------------------
🛠️ Data Cleansing & Transformation Logic
Both the Full and Incremental scripts apply the following business rules to resolve "data chaos":
• Quantity Validation: Filters out records where order_qty is null.
• Customer ID Sanitization: Uses Regular Expressions to ensure customer_id is numeric; invalid entries are defaulted to 999999 to maintain referential integrity.
• Temporal Normalization:
    ◦ Weekday Removal: Strips day names (e.g., "Tuesday, ") from date strings.
    ◦ Multi-Format Parsing: Leverages coalesce and try_to_date to standardize various formats (yyyy/MM/dd, dd-MM-yyyy, MMMM dd, yyyy) into a uniform yyyy-MM-dd.
• Key Enrichment: Joins raw orders with the Silver Product Dimension to attach the deterministic product_code (SHA-256).

--------------------------------------------------------------------------------
🔄 Ingestion Strategies
1. Full Load (full_load_fact_processing)
• Purpose: Processes the 5-month historical backfill (July to November).
• File Management: Once records are appended to the Bronze table, the script programmatically moves raw CSVs from the landing/ folder to the processed/ folder to prevent reprocessing.
• Batch Merge: Performs a DeltaTable.merge into the master Gold table to ensure no duplicates exist in the historical data.
2. Incremental Load (incremental_load_fact_processing)
• Purpose: Handles daily December updates and ongoing transactions.
• Staging Pattern: To optimize compute, the script creates a Staging Table containing only the newly arrived daily data.
• Monthly Re-aggregation:
    ◦ The script retrieves all records for the current month from the child Gold table.
    ◦ It re-calculates the sum of sold_quantity at a Monthly Granularity (first of the month) to match the Atlikon parent schema.
    ◦ It performs an Upsert (Merge) into the parent fact_orders table.

--------------------------------------------------------------------------------
⚙️ Technical Highlights
• Delta Lake Features: Utilizes Change Data Feed (CDF) and Merge Schema options to allow the pipeline to evolve alongside the business.
• Granularity Alignment: Resolves the conflict between Sports Bar's daily tracking and Atlikon's monthly reporting cycles.
• Orchestration: Integrated into a scheduled Databricks Job to run nightly once the business day is concluded.

--------------------------------------------------------------------------------
Analogy for GitHub Portfolio: Think of the Fact Pipeline as a Commercial Sorter at a post office.
• Bronze is the Loading Dock, where all packages (daily CSVs) are dumped regardless of their condition.
• Silver is the Sorting Room, where we fix torn labels (sanitizing IDs) and translate different address formats into a standard zip code (parsing dates).
• Gold is the Master Logbook. Instead of listing every individual package, the sorter writes down the Monthly Totals (aggregation) for each department, providing the CEO with a single, reliable summary of all activity
