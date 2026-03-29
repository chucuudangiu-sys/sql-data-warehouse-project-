Modern Data Warehouse & Analytics Project (MySQL)
Welcome to the Modern Data Warehouse and Analytics Project repository! 

This project demonstrates a complete end-to-end data warehousing solution using MySQL. It follows the industry-standard Medallion Architecture to transform raw datasets into actionable business insights.

Data Architecture
The architecture follows the Medallion pattern, divided into three distinct layers to ensure data quality and traceability:

Bronze Layer: Acts as the landing zone for raw data. Data is ingested from CSV files (ERP and CRM systems) into MySQL tables using LOAD DATA LOCAL INFILE scripts.

Silver Layer: Focuses on data cleansing, standardization, and normalization. This layer handles missing values, inconsistent formats, and adds crucial metadata columns (e.g., src_insertion_timestamp) for auditing.

Gold Layer: The final layer where data is modeled into a Star Schema (Fact and Dimension tables), optimized for high-performance analytical reporting.

Project Overview
This project highlights expertise in:

Data Architecture: Implementing Medallion Architecture using MySQL schemas.

ETL Pipelines: Developing robust scripts for Extraction, Transformation, and Loading (ETL).

Data Modeling: Designing Star Schemas with Fact and Dimension tables.

Technical Troubleshooting: Resolving environment-specific challenges such as MySQL local_infile restrictions on macOS.

Tooling & Environment
Database: MySQL (hosted via DBngin).

GUI / Management: TablePlus for SQL development and database administration.

Modeling: Draw.io for designing data flow and ER diagrams.

Version Control: Git & GitHub for code management.

Project Requirements & Implementation
Data Engineering (Building the Warehouse)
Source Systems: Integration of data from CRM and ERP systems provided as CSV files.

Data Quality: Implementation of logic to resolve data quality issues during the transition from Bronze to Silver.

Integration: Consolidating diverse sources into a unified, user-friendly analytical model.

Data Analysis (Analytics & Reporting)
Customer Insights: Analyzing behavior and segmentation.

Product Performance: Identifying top-performing products and categories.

Sales Trends: Monitoring business health through key performance metrics.

Repository Structure
Plaintext
sql-data-warehouse-project/
│
├── datasets/                 # Raw CSV files (ERP and CRM data)
│
├── docs/                     # Architecture diagrams and documentation
│   ├── data_architecture.png # Medallion architecture flow
│   └── data_models.drawio    # Star Schema design
│
├── scripts/                  # SQL scripts for data transformation
│   ├── bronze/               # DDL and ingestion scripts (LOAD DATA)
│   ├── silver/               # Cleansing and standardization logic
│   └── gold/                 # Fact and Dimension table creation
│
├── README.md                 # Project documentation
└── .gitignore                # Files ignored by Git

License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.
