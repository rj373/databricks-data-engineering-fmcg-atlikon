📂 Folder: setup/
This folder contains the initialization scripts required to establish the data infrastructure for the Atlikon and Sports Bar unified data pipeline. These scripts must be executed before running any ETL (Bronze-to-Gold) processes.
📄 Included Files
1. setup_catalog.sql
• Purpose: Initializes the Unity Catalog and defines the Medallion Architecture layers.
• Functionality:
    ◦ Creates the fmcg catalog as the top-level container.
    ◦ Establishes the three maturity layers: Bronze (raw), Silver (cleansed), and Gold (curated).
• Why we need it: To move away from "data chaos," we require a governed environment where data lineage is clear. By isolating data into specific schemas, we ensure that only high-quality, transformed data reaches the Gold layer for executive reporting.
2. utilities.py
• Purpose: A configuration module that centralizes environment variables.
• Functionality:
    ◦ Defines the schema names (bronze, silver, gold) as Python variables.
    ◦ Allows other notebooks to import these variables using the %run command.
• Why we need it: This satisfies the Scalability success criterion. Instead of hardcoding schema names in every notebook, we centralize them here. If Atlikon acquires another company tomorrow, we can point the entire pipeline to a new catalog just by updating this one file.
3. dim_date_table_creation.py
• Purpose: Programmatically generates the Date Dimension table.
• Functionality:
    ◦ Uses Spark SQL’s sequence and explode functions to generate a continuous monthly timeline from January 2024 to December 2025.
    ◦ Derives analytical attributes like year_quarter, month_name, and date_key.
• Why we need it: Atlikon and Sports Bar originally had reporting cycles that "didn't align". This script creates a Unified Temporal Backbone, ensuring that when we aggregate revenue by quarter, both companies’ data is joined onto the exact same calendar.
🚀 Execution Order
1. setup_catalog.sql: Run first to create the physical folders and database containers in Databricks.
2. utilities.py: Run to initialize the variables for the session.
3. dim_date_table_creation.py: Run to populate the Gold layer with the master date dimension.
🎯 Business Impact
By establishing this setup folder, we ensure a "low learning curve" for any new data engineers joining the project. The infrastructure is clear, modular, and designed to transform messy spreadsheets and inconsistent APIs into a reliable data layer that the COO can trust for aggregated analytics
