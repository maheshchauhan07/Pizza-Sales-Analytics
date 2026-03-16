# 🍕 Pizza Sales Analytics
## End-to-End Data Engineering & Analytics Project

---

# 📌 Project Overview

This project demonstrates a **fully implemented end-to-end data pipeline and analytics workflow** using **Pizza Sales data from 2016**.

The objective is to simulate a **real-world analytics engineering environment**, where data is **ingested, processed across multiple layers, validated, and visualized**, generating actionable business insights.

The project covers the full analytics lifecycle:

- Data ingestion from CSV files into a **raw layer**  
- Transformation and staging in **Snowflake staging layer**  
- Modeling into a **Star Schema data mart**  
- Pipeline orchestration with **Apache Airflow**  
- Data validation and exploratory analysis using **SQL**  
- Interactive **Power BI dashboards** for business insights  

This project demonstrates **hands-on Data Engineering and Analytics skills**, including:

- ETL pipeline development  
- Data warehouse design and modeling  
- SQL-based data quality validation  
- Business intelligence visualization  

---

# 🛠️ Tech Stack

| Technology | Purpose |
|-----------|--------|
| Snowflake | Cloud Data Warehouse |
| Apache Airflow | Workflow orchestration of raw → staging → mart layers |
| Docker | Containerized Airflow environment |
| Python | DAG creation & pipeline automation |
| SQL | Data transformation, EDA, and validation |
| Power BI | Business intelligence dashboards |
| Lucidchart | Data modeling & schema design |

---

# 🏗️ Data Pipeline Architecture

The pipeline is implemented across **three layers** and orchestrated via Airflow:


- CSV Files  
  ↓  
- Airflow (Orchestration Layer)  
  ↓  
- Snowflake Staging Layer  
  ↓  
- Snowflake Data Mart (Star Schema)  
  ↓  
- SQL Analysis / EDA  
  ↓  
- Power BI Dashboard


---

# ⚙️ Pipeline Flow

## Step 1 — Data Ingestion
Raw pizza sales data is ingested from **CSV files** into the **Snowflake staging layer** using Airflow DAGs.

## Step 2 — Pipeline Orchestration
**Apache Airflow DAGs** orchestrate the full pipeline from **raw → staging → data mart**, including **data validation, logging, and error handling**. Each step ensures the workflow is **robust and maintainable**.

## Step 3 — Data Transformation, Cleaning, and Validation

SQL transformations convert the staging data into structured **fact and dimension tables** in the data mart (Star Schema). After transformation, **SQL** is used to clean, validate, and explore the data.

🔹 Highlighted Skills:

Airflow for orchestrating ETL pipelines

SQL for data transformations and quality checks

Data cleaning and validation 

Exploratory Data Analysis (EDA) for business insights

**Using YAML for Scalable Pipelines:**  
To make the pipeline **scalable and maintainable**, table creation and insert statements are **abstracted into YAML configuration files**. The pipeline reads table definitions, column specifications, and insert logic from YAML files, allowing **easy schema changes, addition of new tables, or data sources** without modifying DAG code. This ensures the ETL workflow is **robust, reusable, and production-ready**.

## Step 4 — Data Warehouse
A **Star Schema data mart** is implemented containing **fact and dimension tables**, optimized for analytics and BI workloads.

## Step 5 — Exploratory Data Analysis & Validation
After loading the data mart, **SQL queries were used for EDA and validation**, including:

- Row count validation between layers  
- Null value checks on critical columns  
- Revenue calculation consistency  
- Negative value checks for quantity and price  
- Referential integrity validation between fact and dimension tables  

## Step 6 — Business Intelligence
Power BI dashboards visualize key metrics and insights, enabling **interactive exploration of trends, product performance, and revenue analytics**.

 

# 📊 Data Warehouse Model

The warehouse follows a **Star Schema**, optimized for analytics and BI.

## ⭐ Fact Table — fact_table

| Column | Description |
|------|-------------|
| sales_key | Primary Key |
| date_key | Foreign Key → dim_date |
| pizza_key | Foreign Key → dim_pizza |
| order_id | Order identifier |
| quantity | Number of pizzas sold |
| price | Pizza price |
| revenue | quantity × price |
| full_time | Order time |

---

## 📅 Dimension Table — Date (dim_date)

| Column | Description |
|------|-------------|
| date_key | Primary Key |
| full_date | Calendar date |
| year | Year |
| month | Month number |
| month_name | Month name |
| day | Day number |
| day_name | Name of day |
| quarter | Quarter |
| is_weekend | Weekend indicator |

---

## 🍕 Dimension Table — Pizza (dim_pizza)

| Column | Description |
|------|-------------|
| pizza_key | Primary Key |
| pizza_id | Pizza identifier |
| pizza_name | Pizza name |
| category | Pizza category |
| size | Pizza size |
| ingredients | Pizza ingredients |

---

# ✅ Data Quality Checks

- Row count validation between raw, staging, and mart layers  
- Null checks on critical columns  
- Revenue calculation consistency  
- Negative value checks for quantity and price  
- Referential integrity validation  

These ensure **high-quality, reliable analytics outputs**.

---

# 🔍 Key Business Insights

## 📊 Business Metrics

| Metric | Value |
|------|------|
| Total Revenue | $817,860 |
| Total Orders | 21,350 |
| Total Pizzas Sold | 49,574 |
| Average Pizzas per Order | ~2 |
| Average Order Value | ~$38 |

---

## 📈 Sales Trends

- Friday generates the **highest revenue**  
- July is the **best performing month**  
- Quarter 2 produces the **strongest revenue performance**

---

## 🍕 Product Performance

- Large pizzas contribute **~46% of total revenue**  
- **Thai Chicken Pizza** is the top revenue generating pizza  
- Revenue distribution is **balanced across products**  

---

# 📊 Power BI Dashboard

## Page 1 — Sales Overview
- KPI summary cards  
- Revenue by month, quarter, day of week  
- Orders by hour  

## Page 2 — Product Performance
- Revenue by pizza category and size  
- Top 10 pizzas by revenue  
- Bottom 10 pizzas by revenue  

---

# 💡 Business Recommendations

- 📌 Introduce **weekend promotions**  
- 📌 Expand **chicken pizza offerings**  
- 📌 Remove **low-performing XL / XXL pizza sizes**  
- 📌 Launch **Q4 holiday marketing campaigns**  
- 📌 Promote **lunchtime deals around 12 PM**  
- 📌 Develop a **signature hero pizza product**

---

# 📄 Business Report

Detailed report included:

**Pizza_Sales_Performance_Report_2016.pdf**

---

# 👤 Author

**Mahesh Chauhan**  
Data Analyst | Data Engineering Enthusiast  

📍 Berlin, Germany  

🔗 LinkedIn  
https://www.linkedin.com/in/mahesh-chauhan-98154a247/
