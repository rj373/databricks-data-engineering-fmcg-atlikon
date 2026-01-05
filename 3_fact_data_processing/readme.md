# 📊 Fact Data Processing
## Orders Pipeline

This folder contains the core transaction engine for the Atlikon and Sports Bar merger.

The pipeline processes high-volume daily order data and transforms it into a consolidated monthly format used for executive reporting.

---

## 🏗️ Architecture Overview

The pipeline follows the Medallion Architecture to move transaction data from raw ingestion to a single source of truth.

### Bronze Layer (Raw Ingestion)

- Ingests daily CSV files from the S3 landing zone
- Appends ingestion metadata such as file_name and read_timestamp
- Preserves raw data for auditing and replayability

### Silver Layer (Transformation)

- Standardizes inconsistent and messy data formats
- Applies business validation rules
- Performs high-integrity joins across dimensions

### Gold Layer (Consolidation)

- Produces BI-ready daily records for the child company
- Aggregates daily data into monthly records for the parent company
- Serves as the authoritative reporting layer

---

## 🛠️ Data Cleansing and Transformation Logic

Both full-load and incremental scripts apply the following business rules to resolve data inconsistencies.

- Filters out records where order quantity is null
- Validates customer_id values using regular expressions
- Replaces invalid or non-numeric customer_id values with a default value to maintain referential integrity
- Removes weekday names from date strings
- Parses multiple date formats into a single yyyy-MM-dd standard using coalesce and try_to_date
- Joins order data with the Silver Product Dimension to attach the deterministic product_code

---

## 🔄 Ingestion Strategies

### Full Load Processing

- Processes five months of historical data from July through November
- Appends cleaned records to the Bronze layer
- Moves processed CSV files from the landing folder to the processed folder to prevent reprocessing
- Executes a Delta Lake merge into the Gold fact table to eliminate duplicates

---

### Incremental Load Processing

- Handles daily updates and ongoing transactions
- Creates a staging table containing only newly arrived records
- Retrieves all records for the current month from the child Gold table
- Recalculates monthly aggregates using the first day of the month as the grain
- Performs an upsert into the parent fact_orders table

---

## ⚙️ Technical Highlights

- Utilizes Delta Lake features such as Change Data Feed and Merge Schema
- Resolves granularity mismatches between daily and monthly reporting models
- Integrated into a Databricks Job scheduled to run nightly after business hours
- Designed to scale as transaction volume grows

---

## 🧠 Portfolio Analogy

Think of the Fact Pipeline as a commercial mail sorter at a post office.

- Bronze is the loading dock where all packages arrive in any condition
- Silver is the sorting room where labels are repaired and addresses standardized
- Gold is the master logbook where only monthly totals are recorded

This approach provides leadership with a single, reliable summary of all business activity.
