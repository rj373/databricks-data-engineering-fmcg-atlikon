📂 Dimension Data Processing: Data Cleansing & Transformation
This document outlines the specific cleansing and transformation steps performed for the three core dimension tables—Customers, Products, and Pricing—to integrate Sports Bar's data into the Atlikon ecosystem.

--------------------------------------------------------------------------------
1️⃣ Customer Data Pipeline
The primary goal of this pipeline is to resolve inconsistencies in customer reporting and geographic data.
Data Cleansing Steps:
• Deduplication: Identified and removed duplicate records based on the customer_id.
• Name Standardization: Removed leading and trailing white spaces using trim and standardized name casing (e.g., "NUTRITION" to "Nutrition") using initcap.
• Geographic Normalization: Standardized city typos such as "NewDelhee", "Bengalore", and "Hyderabadd" into a uniform list of allowed cities (New Delhi, Bengaluru, Hyderabad) using a mapping dictionary.
• Business Logic Correction: Manually filled null city values for specific accounts—Sprintx Nutrition, Zenathlete Foods, Primefuel Nutrition, and Recovery Lane—using business-confirmed IDs.
• Schema Alignment: Cast customer_id from an integer to a string to match the categorical data model of the parent company.
• Attribute Enrichment: Created a unified customer column by concatenating the name and city, and appended static attributes for Market (India), Platform (Sports Bar), and Channel (Acquisition).

--------------------------------------------------------------------------------
2️⃣ Product Data Pipeline
This pipeline bridges the gap between Sports Bar’s nutrition product catalog and Atlikon’s standardized master list.
Data Cleansing Steps:
• Deduplication: Dropped duplicate records using the product_id to ensure each product is unique in the dimension table.
• Category Normalization: Standardized the casing of product categories using initcap.
• Typo Correction: Utilized Regular Expressions (Regex) to fix common "fat-fingering" errors, specifically correcting "Protien" to "Protein" across all category and product name columns.
• Division Mapping: Mapped Sports Bar categories to parent business divisions (e.g., mapping "Energy Bars" and "Protein Bars" to "Nutrition Bars").
• Variant Extraction: Extracted size or weight information (e.g., "60g") from the product name string and moved it into a dedicated variant column.
• Surrogate Key Generation: Created a deterministic product_code using a SHA-2 hashing algorithm on the product name to act as a reliable identifier.
• ID Sanitization: Validated product_id strings to ensure they are numeric; invalid or non-numeric IDs were replaced with a fallback value of 999999 to maintain data integrity.

--------------------------------------------------------------------------------
3️⃣ Pricing Data Pipeline
This pipeline reconciles Sports Bar's monthly pricing snapshots with Atlikon’s yearly reporting model.
Data Cleansing Steps:
• Temporal Normalization: Used coalesce and try_to_date to parse multiple inconsistent date formats into a single, uniform yyyy-MM-dd standard.
• Price Cleaning: Corrected "fat-fingering" errors by converting negative price values to their absolute (positive) value and replaced non-numeric "Unknown" price strings with 0.
• Relationship Enrichment: Joined the pricing table with the Silver Product dimension to attach the hashed product_code, ensuring a consistent join key for the Gold layer.
• Yearly Aggregation Logic: Because the parent company reports prices annually, a Window Function was used to rank monthly prices within each year. The latest month's price was extracted to serve as the official yearly price.

--------------------------------------------------------------------------------
🛠️ Shared Technical Standards
• Medallion Flow: All transformations occur in the Silver Layer, moving data from raw (Bronze) to curated (Gold) states.
• Lineage & Auditing: Every record is enriched with read_timestamp, file_name, and file_size during ingestion for tracking purposes.
• Delta Merge: Each pipeline concludes with an Upsert (Merge) into the Atlikon master tables, ensuring existing records are updated and new records are inserted without duplication.
Analogy for Dimension Cleansing: Think of this process as a Quality Control Center at a factory merger. The Customer Pipeline ensures every client's address is spelled correctly on their badge; the Product Pipeline ensures every item has the right barcode and is placed in the correct aisle; and the Pricing Pipeline looks at the fluctuating daily price tags to decide on the final, official price for the yearly catalog.
