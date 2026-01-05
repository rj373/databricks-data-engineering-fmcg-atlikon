# 📂 setup/

This folder contains the initialization scripts required to establish the data infrastructure for the Atlikon and Sports Bar unified data pipeline.

All scripts in this folder must be executed before running any ETL processes from Bronze to Gold.

---

## 📄 Included Files

### setup_catalog.py

**Purpose**  
Initializes the Unity Catalog and defines the Medallion Architecture layers.

**Functionality**
- Creates the fmcg catalog as the top-level container
- Establishes the Bronze (raw), Silver (cleansed), and Gold (curated) schemas

**Why This Is Needed**
- Provides a governed data environment
- Ensures clear data lineage and separation of concerns
- Guarantees that only validated data reaches the Gold layer for executive reporting

---

### utilities.py

**Purpose**  
Centralized configuration module for environment variables.

**Functionality**
- Defines schema names (bronze, silver, gold) as reusable Python variables
- Allows notebooks to import variables using the %run command

**Why This Is Needed**
- Eliminates hardcoded values across notebooks
- Improves scalability and maintainability
- Enables fast redirection of pipelines if new business units are added

---

### dim_date_table_creation.py

**Purpose**  
Generates the Date Dimension table programmatically.

**Functionality**
- Uses Spark SQL sequence and explode functions
- Generates a continuous monthly timeline from January 2024 to December 2025
- Derives analytical attributes such as year_quarter, month_name, and date_key

**Why This Is Needed**
- Aligns reporting calendars between Atlikon and Sports Bar
- Creates a unified temporal backbone for analytics
- Ensures accurate aggregation across months and quarters

---

## 🚀 Execution Order

Run the scripts in the following order:

- setup_catalog.py  
  Creates catalogs, schemas, and physical containers

- utilities.py  
  Initializes configuration variables for the session

- dim_date_table_creation.py  
  Populates the Gold layer with the master Date Dimension table

---

## 🎯 Business Impact

- Establishes a clean and governed data foundation
- Reduces onboarding time for new data engineers
- Enables consistent and trusted reporting across sources
- Converts messy source data into analytics-ready datasets
