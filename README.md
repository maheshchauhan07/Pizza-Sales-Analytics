🍕 Pizza Sales Analytics — End-to-End Data Project
📌 Overview

This project demonstrates an end-to-end data pipeline and analytics workflow using pizza sales data from 2016.
The goal is to simulate a real-world data engineering + analytics pipeline — transforming raw CSV data into business insights through a structured data warehouse and interactive dashboard.

The project covers:

Data ingestion from raw CSV files

Data transformation using a star schema data model

Pipeline orchestration with Apache Airflow

SQL-based data quality checks and exploratory analysis

Business intelligence visualization using Power BI

🛠️ Tools & Technologies
Tool	Purpose
Snowflake	Cloud Data Warehouse
Apache Airflow	Pipeline Orchestration
Docker	Containerized Airflow environment
SQL	Data transformation & analysis
Power BI	Dashboard & business intelligence
Python	DAG development
Lucidchart	Data modeling (ERD / Star Schema)
🏗️ Data Pipeline Architecture

The project follows a layered architecture commonly used in modern analytics systems.

Raw CSV Data
      ↓
Snowflake Staging Layer
      ↓
Snowflake Mart Layer (Star Schema)
      ↓
SQL Analysis (snowflake)
      ↓
Power BI Dashboard
📁 Project Structure
pizza-sales-analytics
│
├── airflow_dags
│   └── pizza_dag.py
│
├── sql
│   └── pizza_analysis.sql
│
├── dashboard
│   └── screenshots
│
├── report
│   └── Pizza_Sales_Report.pdf
│
└── README.md
📊 Data Model — Star Schema

The warehouse uses a star schema design optimized for analytical queries.

Fact Table

fact_table

Column	Description
sales_key	Primary key
date_key	FK → dim_date
pizza_key	FK → dim_pizza
order_id	Order identifier
quantity	Number of pizzas sold
price	Pizza price
revenue	quantity × price
Full_Time Time column

Dimension Tables

dim_date

Column	Description
date_key	Primary key
full_date	Calendar date
year	Year
month	Month
month_name name of month
day     day number
day_name  name of day
quarter	Quarter
is_weekend	Weekend indicator

dim_pizza

Column	Description

pizza_key	Primary key
PIZZA_ID    ID
pizza_name	Pizza name
category	Pizza category
size	Pizza size
Ingredients INGREDIENTS

✅ Data Quality Checks

To ensure reliable analytics, the following validation checks were implemented:

Row count validation between staging and fact tables

Null value checks on critical fields

Revenue consistency validation

Negative value checks on quantity and price

Referential integrity checks between fact and dimension tables

🔍 Key Business Insights

Total Revenue: $817,860

Total Orders: 21,350

Total Pizzas Sold: 49,574

Average Pizzas per Order: ~2

Average Order Value: ~$38

Sales Trends

Friday is the highest revenue generating day

July is the best performing month

Q2 generates the strongest quarterly revenue

Product Performance

Large pizzas contribute ~46% of total revenue

Thai Chicken Pizza is the top revenue driver

Revenue is distributed across many pizzas (no strict 80/20 rule)

📈 Power BI Dashboard

The Power BI dashboard contains two analytical views.

Page 1 — Sales Overview

KPI summary cards

Revenue by month

Revenue by quarter

Revenue by day of week

Orders by hour

Page 2 — Product Performance

Revenue by category

Revenue by pizza size

Top 10 pizzas by revenue

Bottom 10 pizzas by revenue

💡 Business Recommendations

Based on the analysis, several strategies could improve business performance:

Introduce weekend promotions to increase non-weekday sales

Expand chicken pizza offerings due to strong demand

Consider removing low performing XL/XXL sizes

Launch Q4 holiday marketing campaigns

Target lunch-time promotions around 12 PM

Develop a signature "hero" pizza to drive brand recognition

📄 Business Report

A detailed business report summarizing insights and recommendations is available by the following name:

Pizza_Sales_Performance_Report_2016_.pdf

👤 Author

Mahesh Chauhan
Data Analyst | Data Engineering Enthusiast

🔗 LinkedIn
https://www.linkedin.com/in/mahesh-chauhan-98154a247/

📍 Berlin, Germany
