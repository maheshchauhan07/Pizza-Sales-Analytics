🍕 Pizza Sales Analytics — End-to-End Data Engineering & Analytics Project


📌 Project Overview

This project demonstrates a complete end-to-end data pipeline and analytics workflow built using Pizza Sales data from 2016.

The goal is to simulate a real-world analytics engineering environment, where raw data is ingested, transformed, validated, and visualized to generate actionable business insights.

The project covers the full analytics lifecycle:

Raw data ingestion from CSV files

Data warehouse modeling using a Star Schema

Pipeline orchestration with Apache Airflow

Data transformation and validation using SQL

Interactive Power BI dashboards

Business insight generation and reporting

This project demonstrates both Data Engineering and Data Analytics skills, including ETL pipelines, warehouse design, data validation, and BI visualization.

🛠️ Tech Stack
Technology	Purpose
Snowflake	Cloud Data Warehouse
Apache Airflow	Workflow orchestration
Docker	Containerized Airflow environment
Python	DAG creation & pipeline automation
SQL	Data transformation & analysis
Power BI	Business intelligence dashboard
Lucidchart	Data modeling & schema design
🏗️ Data Pipeline Architecture

The project follows a layered modern data architecture used in production analytics systems.

CSV Files
    ↓
Airflow (orchestrates everything)
    ↓
Snowflake Staging
    ↓
Snowflake Mart (Star Schema)
    ↓
SQL Analysis (snowflake)
    ↓
Power BI Dashboard



Pipeline Flow

1️⃣ Raw CSV sales data is ingested into Snowflake staging tables
2️⃣ Airflow orchestrates transformation workflows
3️⃣ Cleaned data is transformed into a Star Schema warehouse
4️⃣ SQL queries generate business metrics and insights
5️⃣ Power BI visualizes performance and trends

📁 Project Structure
pizza-sales-analytics
│
├── airflow_dags
│   └── pizza_dag.py          # Airflow pipeline DAG
│
├── sql
│   └── pizza_analysis.sql    # Analytical SQL queries
│
├── dashboard
│   └── screenshots           # Power BI dashboard images
│
├── report
│   └── Pizza_Sales_Report.pdf
│
└── README.md
📊 Data Warehouse Model

The warehouse follows a Star Schema, which is optimized for analytical workloads and BI tools.

⭐ Fact Table

fact_table
Column	Description
sales_key	Primary Key
date_key	FK → dim_date
pizza_key	FK → dim_pizza
order_id	Order identifier
quantity	Number of pizzas sold
price	Pizza price
revenue	quantity × price
full_time	Order time

📅 Dimension Table — Date

dim_date
Column	Description
date_key	Primary Key
full_date	Calendar date
year	Year
month	Month number
month_name	Month name
day	Day number
day_name	Name of day
quarter	Quarter
is_weekend	Weekend indicator

🍕 Dimension Table — Pizza
dim_pizza
Column	Description
pizza_key	Primary Key
pizza_id	Pizza identifier
pizza_name	Pizza name
category	Pizza category
size	Pizza size
ingredients	Pizza ingredients

✅ Data Quality Checks

To ensure high-quality analytical data, multiple validation checks were implemented:

✔ Row count validation between staging and fact tables
✔ Null value checks on critical fields
✔ Revenue calculation consistency checks
✔ Negative value validation for quantity and price
✔ Referential integrity checks between fact and dimension tables

These checks help maintain accurate and trustworthy analytics outputs.

🔍 Key Business Insights
📊 Business Metrics
Metric	Value
Total Revenue	$817,860
Total Orders	21,350
Total Pizzas Sold	49,574
Average Pizzas per Order	~2
Average Order Value	~$38
📈 Sales Trends

Key trends observed in the data:

Friday generates the highest revenue

July is the best-performing month

Quarter 2 produces the strongest revenue performance

🍕 Product Performance

Key product insights include:

Large pizzas contribute ~46% of total revenue

Thai Chicken Pizza is the highest revenue generating pizza

Revenue distribution is relatively balanced across products (no strict 80/20 Pareto rule)

📊 Power BI Dashboard

The Power BI dashboard contains two analytical views.

Page 1 — Sales Overview

Features:

KPI summary cards

Revenue by month

Revenue by quarter

Revenue by day of week

Orders by hour

Page 2 — Product Performance

Features:

Revenue by pizza category

Revenue by pizza size

Top 10 pizzas by revenue

Bottom 10 pizzas by revenue

💡 Business Recommendations

Based on the analysis, the following strategies could improve performance:

📌 Introduce weekend promotions to boost non-weekday sales
📌 Expand chicken pizza menu offerings due to strong demand
📌 Evaluate removal of low-performing XL/XXL sizes
📌 Launch Q4 holiday marketing campaigns
📌 Promote lunchtime deals around 12 PM
📌 Develop a signature "hero" pizza product

📄 Business Report

A detailed business report summarizing insights and recommendations is included in the repository.

📁 Pizza_Sales_Performance_Report_2016.pdf

👤 Author

Mahesh Chauhan

Data Analyst | Data Engineering Enthusiast

📍 Berlin, Germany

🔗 LinkedIn
