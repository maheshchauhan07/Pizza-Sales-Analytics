🍕 Pizza Sales Analytics
End-to-End Data Engineering & Analytics Project
📌 Project Overview

This project demonstrates a complete end-to-end data pipeline and analytics workflow built using Pizza Sales data from 2016.

The objective of the project is to simulate a real-world analytics engineering environment, where raw data is ingested, transformed, validated, and visualized to generate actionable business insights.

The project covers the full analytics lifecycle:

Raw data ingestion from CSV files

Data warehouse modeling using a Star Schema

Pipeline orchestration with Apache Airflow

Data transformation and validation using SQL

Interactive Power BI dashboards

Business insight generation and reporting

This project demonstrates both Data Engineering and Data Analytics skills, including:

ETL pipelines

Data warehouse design

Data quality validation

Business intelligence visualization

🛠️ Tech Stack
Technology	Purpose
Snowflake	Cloud Data Warehouse
Apache Airflow	Workflow orchestration
Docker	Containerized Airflow environment
Python	DAG creation & pipeline automation
SQL	Data transformation & analysis
Power BI	Business intelligence dashboards
Lucidchart	Data modeling & schema design
🏗️ Data Pipeline Architecture

The project follows a layered modern data architecture commonly used in production analytics systems.

CSV Files
⬇
Airflow (Orchestration Layer)
⬇
Snowflake Staging Layer
⬇
Snowflake Data Mart (Star Schema)
⬇
SQL Analysis / EDA
⬇
Power BI Dashboard

⚙️ Pipeline Flow
Step 1 — Data Ingestion

Raw pizza sales data is ingested from CSV files into the Snowflake staging layer.

Step 2 — Pipeline Orchestration

Apache Airflow DAGs automate the data pipeline including ingestion, transformations, and validations.

Step 3 — Data Transformation

SQL transformations convert raw staging data into structured warehouse tables.

Step 4 — Data Warehouse

A Star Schema data mart is created containing fact and dimension tables.

Step 5 — Exploratory Data Analysis

SQL queries are used to analyze patterns, validate metrics, and explore business trends.

Step 6 — Business Intelligence

Power BI dashboards visualize key metrics and insights.

📁 Project Structure

pizza-sales-analytics
│
├── airflow_dags
│ └── pizza_dag.py
│
├── sql
│ └── pizza_analysis.sql
│
├── dashboard
│ └── screenshots
│
├── report
│ └── Pizza_Sales_Report.pdf
│
└── README.md

📊 Data Warehouse Model

The warehouse follows a Star Schema, optimized for analytical workloads and BI tools.

⭐ Fact Table — fact_table
Column	Description
sales_key	Primary Key
date_key	Foreign Key → dim_date
pizza_key	Foreign Key → dim_pizza
order_id	Order identifier
quantity	Number of pizzas sold
price	Pizza price
revenue	quantity × price
full_time	Order time
📅 Dimension Table — Date (dim_date)
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
🍕 Dimension Table — Pizza (dim_pizza)
Column	Description
pizza_key	Primary Key
pizza_id	Pizza identifier
pizza_name	Pizza name
category	Pizza category
size	Pizza size
ingredients	Pizza ingredients
✅ Data Quality Checks

To ensure high-quality analytical data, several validation checks were implemented.

✔ Row count validation between staging and fact tables
✔ Null value checks on critical columns
✔ Revenue calculation consistency validation
✔ Negative value checks for quantity and price
✔ Referential integrity validation between fact and dimension tables

🔍 Key Business Insights
📊 Business Metrics
Metric	Value
Total Revenue	$817,860
Total Orders	21,350
Total Pizzas Sold	49,574
Average Pizzas per Order	~2
Average Order Value	~$38
📈 Sales Trends

Important patterns observed in the data:

Friday generates the highest revenue

July is the best performing month

Quarter 2 produces the strongest revenue performance

🍕 Product Performance

Large pizzas contribute ~46% of total revenue

Thai Chicken Pizza is the top revenue generating pizza

Revenue distribution is relatively balanced across products

📊 Power BI Dashboard

The Power BI dashboard contains two analytical pages.

Page 1 — Sales Overview

KPI summary cards

Revenue by month

Revenue by quarter

Revenue by day of week

Orders by hour

Page 2 — Product Performance

Revenue by pizza category

Revenue by pizza size

Top 10 pizzas by revenue

Bottom 10 pizzas by revenue

💡 Business Recommendations

📌 Introduce weekend promotions
📌 Expand chicken pizza offerings
📌 Evaluate removal of low-performing XL / XXL pizza sizes
📌 Launch Q4 holiday marketing campaigns
📌 Promote lunchtime deals around 12 PM
📌 Develop a signature hero pizza product

📄 Business Report

Detailed report included in the repository:

Pizza_Sales_Performance_Report_2016.pdf

👤 Author

Mahesh Chauhan
Data Analyst | Data Engineering Enthusiast

📍 Berlin, Germany

🔗 LinkedIn
https://www.linkedin.com/in/mahesh-chauhan-98154a247/
