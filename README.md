# SQL-data-warehouse-project1
Building a modern data warehouse with SQL Server, including ETL processes. data modeling and analytics.

## Acknowledgements
This project was created based on a tutorial by DataWithBaraa.
Special thanks for sharing valuable knowledge for free.
Tutorial link: https://youtu.be/SSKVgrwhzus?si=a9yrJDsT8LbA53aK

Welcome to the Data Warehouse and Analytics Project repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

### 🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![model](docs/DataArchitecture.png)
**Bronze Layer:** Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
**Silver Layer:** This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
**Gold Layer:** Houses business-ready data modeled into a star schema required for reporting and analytics.

### 📖 Project Overview
This project involves:

**Data Architecture:** Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
**ETL Pipelines:** Extracting, transforming, and loading data from source systems into the warehouse.
**Data Modeling:** Developing fact and dimension tables optimized for analytical queries.
**Analytics & Reporting:** Creating SQL-based reports and dashboards for actionable insights.

### 🛠️ Important Links & Tools:
Everything is for Free!

**Datasets:** Access to the project dataset (csv files).
**SQL Server Express:** Lightweight server for hosting your SQL database.
**SQL Server Management Studio (SSMS):** GUI for managing and interacting with databases.
**Git Repository:** Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
**DrawIO:** Design data architecture, models, flows, and diagrams.
**Notion:** Get the Project Template from Notion
**Notion Project Steps:** Access to All Project Phases and Tasks.

### 🚀 Project Requirements
Building the Data Warehouse (Data Engineering)
**Objective**
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
**Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.
**Data Quality:** Cleanse and resolve data quality issues prior to analysis.
**Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.
**Scope:** Focus on the latest dataset only; historization of data is not required.
**Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams.
### BI: Analytics & Reporting (Data Analysis)
Objective
Develop SQL-based analytics to deliver detailed insights into:

**Customer Behavior**
**Product Performance**
**Sales Trends**
These insights empower stakeholders with key business metrics, enabling strategic decision-making.
