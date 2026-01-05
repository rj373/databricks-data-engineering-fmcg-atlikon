# 📂 Dimension Data Processing
## Data Cleansing and Transformation

This document outlines the cleansing and transformation steps applied to the three core dimension tables: Customers, Products, and Pricing.  
The goal is to integrate Sports Bar data cleanly into the Atlikon data ecosystem.

---

## Customer Data Pipeline

The primary goal of this pipeline is to resolve inconsistencies in customer reporting and geographic data.

### Data Cleansing Steps

- Removed duplicate records based on customer_id
- Trimmed leading and trailing whitespace from customer names
- Standardized customer name casing using initcap
- Normalized city name typos such as NewDelhee, Bengalore, and Hyderabadd into approved values including New Delhi, Bengaluru, and Hyderabad
- Filled missing city values for specific business accounts using business-confirmed identifiers
- Cast customer_id from integer to string to align with the parent company’s categorical data model
- Created a unified customer attribute by concatenating name and city
- Appended static attributes for Market, Platform, and Channel

---

## Product Data Pipeline

This pipeline aligns Sports Bar’s nutrition product catalog with Atlikon’s standardized master product list.

### Data Cleansing Steps

- Removed duplicate records using product_id
- Standardized product category casing using initcap
- Corrected common typographical errors such as Protien to Protein using regular expressions
- Mapped Sports Bar product categories to parent business divisions
- Extracted product variants such as size or weight from product names into a dedicated column
- Generated a deterministic surrogate key called product_code using SHA-2 hashing
- Validated product_id values to ensure numeric consistency
- Replaced invalid or non-numeric product_id values with a fallback value to preserve data integrity

---

## Pricing Data Pipeline

This pipeline reconciles Sports Bar’s monthly pricing snapshots with Atlikon’s annual reporting model.

### Data Cleansing Steps

- Parsed multiple inconsistent date formats into a single yyyy-MM-dd standard using coalesce and try_to_date
- Converted negative price values to positive values
- Replaced non-numeric price values such as Unknown with zero
- Joined pricing data with the Silver Product dimension to attach the hashed product_code
- Applied a window function to rank monthly prices within each year
- Selected the latest monthly price as the official annual price

---

## 🛠️ Shared Technical Standards

- All transformations are performed in the Silver layer following the Medallion Architecture
- Each record is enriched with ingestion metadata including read_timestamp, file_name, and file_size
- All pipelines conclude with Delta Lake merge operations to upsert records into master dimension tables
- Duplicate records are prevented while allowing updates to existing data

---

## 🧠 Dimension Cleansing Analogy

This process functions like a quality control center during a factory merger.

- The Customer pipeline ensures every client’s address is spelled correctly on their badge
- The Product pipeline ensures every item has the correct barcode and shelf placement
- The Pricing pipeline reviews fluctuating prices to determine the final official price for the annual catalog
