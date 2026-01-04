-- Databricks notebook source
-- 1️⃣ Catalog Creation
-- In Unity Catalog, a Catalog is the top-level container for your data assets.
-- We create 'fmcg' to isolate the unified data of Atlon and Sports Bar [3].
CREATE CATALOG IF NOT EXISTS 'fmcg';
USE CATALOG fmcg;

-- COMMAND ----------

-- 2️⃣ Medallion Architecture Schema Setup
-- These schemas represent the three stages of data maturity in the pipeline [4, 5].

-- GOLD: Final BI-ready layer. Contains consolidated, aggregated tables used 
-- for Dashboards and the Genie AI assistant [5, 6].
CREATE SCHEMA IF NOT EXISTS fmcg.gold;

-- SILVER: The transformation layer. Data here is cleaned, duplicates are removed, 
-- and formats are normalized (e.g., fixing city names and date formats) [4, 5, 7].
CREATE SCHEMA IF NOT EXISTS fmcg.silver;

-- BRONZE: The raw ingestion layer. This stores the initial data dumped from 
-- OLTP systems or AWS S3 buckets without any transformations [4, 5, 8].
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
