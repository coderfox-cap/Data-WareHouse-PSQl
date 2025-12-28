📊 Sales Data Warehouse using PostgreSQL
📌 Project Overview

This project implements an end-to-end Sales Data Warehouse using PostgreSQL, designed to consolidate raw sales data from ERP and CRM systems into a structured, analytics-ready format.

The warehouse follows a Medallion Architecture (Bronze → Silver → Gold) to ensure data traceability, quality, and business usability.
The final output supports reporting, analytics, and informed decision-making.

🎯 Objectives

Consolidate ERP and CRM sales data into a single source of truth

Improve data quality through systematic cleaning and validation

Design scalable and maintainable warehouse architecture

Provide business-friendly datasets for analytics and reporting

🛠️ Tech Stack & Tools

Database: PostgreSQL

Version Control: GitHub

Project Tracking: Notion

Data Modeling & Visualization: Draw.io

Data Sources: CSV files (ERP & CRM exports)

🏗️ Architecture Overview (Medallion Model)
🥉 Bronze Layer – Raw Data

Purpose: Store unprocessed source data

Bulk-loaded raw CSV files using TRUNCATE + INSERT

Tables closely mirror source structure

Used for traceability, debugging, and data replay

No transformations applied

🥈 Silver Layer – Cleaned & Standardized Data

Purpose: Improve data quality and consistency

Removed duplicate records using primary keys

Trimmed unwanted spaces from string fields

Standardized inconsistent and abbreviated values

Added metadata columns for auditing and tracking

Applied data type corrections and validations

🥇 Gold Layer – Business-Ready Data

Purpose: Enable analytics and reporting

Built facts and dimensions based on business requirements

Renamed columns to business-friendly names

Integrated datasets across domains

Exposed curated views instead of tables for flexibility and security

📈 Key Learnings

Real-world data warehouse design and architecture

Importance of data quality and traceability

Challenges in maintaining and automating data pipelines

Strong improvement in SQL skills and logical thinking

Practical experience in data modeling and system visualization

🚀 Future Enhancements

Slowly Changing Dimensions (SCD)

Incremental data loading

Performance optimization (indexes, partitioning)

BI tool integration (Power BI / Tableau)

👤 Author
Naren
Aspiring Data Engineer | SQL | Data Warehousing
