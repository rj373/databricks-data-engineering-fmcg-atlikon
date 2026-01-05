# 🚀 Atlikon & Sports Bar Data Integration Project

## 🏗️ Technical Architecture

The project follows the Medallion Architecture to ensure scalable, reliable, and high-quality data processing.

- **Bronze Layer (Raw)**
  - Ingests raw CSV files directly from AWS S3
  - Adds metadata such as file name and ingestion timestamp
  - Used for auditing and traceability

- **Silver Layer (Cleansed)**
  - Removes duplicates
  - Fixes typos and inconsistent values
  - Standardizes formats across datasets

- **Gold Layer (Curated)**
  - Aggregates and joins cleansed data
  - Produces BI-ready fact and dimension tables
  - Used by leadership, analysts, and AI tools

---

## 🛠️ Implementation Steps

## 1. Setup and Infrastructure

- Created an FMCG catalog with Bronze, Silver, and Gold schemas
- Connected Databricks to AWS S3 as the data lake
- Built a centralized configuration notebook for schema management

---

## 2. Dimension Data Processing (The Ingredients)

Dimension tables provide business context such as who, what, and where.

### Customer Pipeline

- Corrected city name spelling errors (Hyderabadd → Hyderabad)
- Standardized customer names using initcap and trimming
- Upserted records into the `dim_customers` table

### Product Pipeline

- Fixed product name typos using regular expressions
- Generated unique product codes using SHA-2 hashing
- Extracted product variants such as weight or size

### Pricing Pipeline

- Normalized multiple date formats into a single standard
- Converted negative and unknown prices to valid values
- Selected the latest monthly price using window functions

---

## 3. Fact Data Processing (The Logbook)

- Loaded five months of historical sales data
- Moved processed files to prevent duplicate ingestion
- Implemented incremental daily ingestion using staging tables
- Aggregated daily sales into monthly records

---

## 📊 Business Intelligence and Orchestration

- Scheduled all pipelines using Databricks Jobs at 11:00 PM daily
- Built a denormalized analytics table for fast dashboarding
- Enabled natural language analytics using Databricks Genie AI

---

## 🍕 Project Analogy

Atlikon is a premium restaurant.
Sports Bar supplies messy ingredients.
Data Engineers clean, standardize, and organize the data.
Analysts and AI systems turn it into insights.

<img width="2660" height="1496" alt="Sports Bar to Atlikon Integration_ Project Architecture - visual selection (1)" src="https://github.com/user-attachments/assets/b9271e33-f867-4eb0-8298-a5957095fafa" />

