# 🍕 Pizza Sales Analytics
## End-to-End Data Engineering & Analytics Project

---

# 📌 Project Overview

This project demonstrates a **fully implemented end-to-end data pipeline and analytics workflow** using **Pizza Sales data from 2015**.

The objective is to simulate a **real-world analytics engineering environment**, where data is **ingested, processed across multiple layers, validated with automated tests, and visualized**, generating actionable business insights.

The project covers the full analytics lifecycle:

- Data ingestion from CSV files into a **raw layer**
- Transformation and staging via **dbt**, orchestrated by **Apache Airflow**
- Modeling into a **Star Schema data mart**
- Automated data quality testing with **dbt tests**
- Pipeline orchestration with **Apache Airflow**, containerized via a custom **Docker** image
- Interactive **Power BI dashboards** for business insights

This project demonstrates **hands-on Data Engineering skills**, including:

- ETL/ELT pipeline development
- Data warehouse design and modeling (Kimball star schema)
- SQL and dbt-based data transformation
- Automated, test-driven data quality validation
- Containerized orchestration with custom Docker images
- Business intelligence visualization

---

# 🛠️ Tech Stack

| Technology     | Purpose                                                        |
| -------------- | --------------------------------------------------------------- |
| Snowflake      | Cloud Data Warehouse                                            |
| Apache Airflow | Workflow orchestration (ingestion + triggers dbt)                |
| dbt            | SQL transformation layer (staging → mart) with automated tests   |
| Docker         | Containerized Airflow + dbt environment (custom image)           |
| Python         | DAG creation & pipeline automation                               |
| SQL            | Data modeling, transformation, and validation                    |
| Power BI       | Business intelligence dashboards                                 |
| Lucidchart     | Data modeling & schema design                                    |

---

# 🏗️ Data Pipeline Architecture

```
CSV Files
   ↓
Airflow (Orchestration Layer — raw ingestion)
   ↓
Snowflake RAW Layer
   ↓
dbt (triggered by Airflow)
   └── Staging models (clean, cast, trim)
   ↓
Snowflake STAGING Layer
   ↓
dbt
   ├── Dimension models (DIM_DATE, DIM_PIZZA)
   ├── Fact model (FACT_table)
   └── 13 automated dbt tests
   ↓
Snowflake MART Layer (Star Schema)
   ↓
Power BI Dashboard
```

Airflow and dbt run inside Docker containers built from a custom image, so the entire pipeline is reproducible with a single `docker-compose up`.

---

# ⚙️ Pipeline Flow

## Step 1 — Data Ingestion
Raw pizza sales data is ingested from **CSV files** into the **Snowflake RAW layer** using an Airflow DAG. Table creation is dynamically generated from a YAML config (`config/tables.yml`), so adding a new table or column requires no DAG code changes.

## Step 2 — Pipeline Orchestration
The **Apache Airflow DAG** handles session setup, stage creation, dynamic RAW table creation, and loading CSVs via `PUT` + `COPY INTO`. Loads use `ON_ERROR = ABORT_STATEMENT`, so a malformed CSV row fails the task loudly instead of being silently skipped. Once ingestion completes, Airflow triggers a single `run_dbt` task that installs dbt's package dependencies (`dbt deps`), then hands off all transformation work to dbt (`dbt run` + `dbt test`).

## Step 3 — Transformation via dbt
dbt builds the **STAGING** layer (type casting, trimming, cleaning) on top of the RAW tables, then builds the **MART** layer (star schema) on top of staging — all defined as version-controlled `.sql` model files instead of inline SQL strings. A custom `generate_schema_name` macro keeps STAGING and MART as genuinely separate Snowflake schemas.

**Why dbt instead of raw SQL in the DAG:**
- Transformation logic is modular, testable, and lives in its own layer
- Every model's lineage and dependencies are explicit (`ref()`/`source()`)
- Data quality is enforced automatically on every run, not just checked manually after the fact

## Step 4 — Data Warehouse
A **Star Schema data mart** is implemented containing one fact table and two dimension tables, optimized for analytics and BI workloads.

## Step 5 — Automated Data Quality Testing
Instead of manual, after-the-fact SQL validation queries, the pipeline runs **13 automated dbt tests** on every execution:

| Category | Tests |
|---|---|
| Uniqueness | `date_key`, `pizza_key`, `sales_key` are each unique |
| Completeness | Critical columns (`date_key`, `pizza_key`, `sales_key`) are never null |
| Referential integrity | Every `FACT_table` row correctly references an existing `DIM_DATE` and `DIM_PIZZA` row |
| Row-count completeness | `FACT_table` has exactly as many rows as `stg_order_details`, so no order lines are silently dropped by joins |
| Value sanity | `quantity` and `price` are never negative |

**A concrete example of why this matters:** during development, dbt's `unique` test on `DIM_DATE.date_key` caught 358 duplicate rows — a bug inherited from the original hand-written SQL, where `DISTINCT` was applied across a column (`time`) that varies per order, silently breaking the one-row-per-date guarantee. The original raw-SQL version of this pipeline had this same bug with no test to catch it. Fixing the model and re-running the test suite confirmed the fix immediately.

A separate bug — `DAYOFWEEK(date) IN (1,7)`, which under Snowflake's default week settings flagged only Mondays as weekend — silently corrupted the weekday/weekend revenue split in earlier versions of the report and dashboard (the old figures were computed before this was fixed and were never refreshed). The current numbers in this README, the report, and the dashboard reflect the corrected data: weekends earn close to their proportional share of the week, not the dramatic shortfall originally reported — a good illustration of why re-validating downstream outputs after a data fix matters just as much as the fix itself.

## Step 6 — Business Intelligence
Power BI dashboards visualize key metrics and insights, enabling **interactive exploration of trends, product performance, and revenue analytics**.

---

# 📊 Data Warehouse Model

The warehouse follows a **Star Schema**, optimized for analytics and BI.

## ⭐ Fact Table — fact_table

| Column     | Description              |
| ---------- | ------------------------ |
| sales_key  | Primary Key              |
| date_key   | Foreign Key → dim_date   |
| pizza_key  | Foreign Key → dim_pizza  |
| order_id   | Order identifier         |
| quantity   | Number of pizzas sold    |
| price      | Pizza price              |
| revenue    | quantity × price         |
| full_time  | Order time               |

## 📅 Dimension Table — Date (dim_date)

| Column      | Description       |
| ----------- | ----------------- |
| date_key    | Primary Key       |
| full_date   | Calendar date     |
| year        | Year              |
| month       | Month number      |
| month_name  | Month name        |
| day         | Day number        |
| day_name    | Name of day       |
| quarter     | Quarter           |
| is_weekend  | Weekend indicator |

## 🍕 Dimension Table — Pizza (dim_pizza)

| Column      | Description       |
| ----------- | ----------------- |
| pizza_key   | Primary Key       |
| pizza_id    | Pizza identifier  |
| pizza_name  | Pizza name        |
| category    | Pizza category    |
| size        | Pizza size        |
| ingredients | Pizza ingredients |

---

# ✅ Data Quality Checks

13 automated dbt tests run on every pipeline execution:
- Uniqueness checks on all primary keys
- Not-null checks on critical columns
- Referential integrity between fact and dimension tables
- Row-count check between staging and the fact table
- Non-negative value checks on quantity and price

These replace manual, point-in-time SQL validation with checks that run automatically, every time, and fail the pipeline loudly if data quality regresses.

---

# 🔍 Key Business Insights

## 📊 Business Metrics

| Metric                   | Value            |
| ------------------------ | ---------------- |
| Total Revenue            | $817,860         |
| Total Orders             | 21,350           |
| Total Pizzas Sold        | 49,574           |
| Average Pizzas per Order | ~2               |
| Average Order Value      | ~$38             |
| Weekday Revenue          | $595,474 (72.8%) |
| Weekend Revenue          | $222,386 (27.2%) |

## 📈 Sales Trends
- Friday generates the **highest revenue**
- July is the **best performing month**
- Quarter 2 produces the **strongest revenue performance**
- Weekends earn close to their proportional share of the week (2 of 7 days ≈ 28.6% expected, actual 27.2%) — Saturday is the 3rd-best day overall, while Sunday is the weakest day

## 🍕 Product Performance
- Large pizzas contribute **~46% of total revenue**
- **Thai Chicken Pizza** is the top revenue generating pizza
- Revenue distribution is **balanced across products**

---

# 📊 Power BI Dashboard

See [`pizza_dashboard_pictures.pdf`](./pizza_dashboard_pictures.pdf) for dashboard screenshots, or open [`pizza_dashboard.pbix`](./pizza_dashboard.pbix) directly in Power BI.

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

- 📌 Introduce a **Sunday-specific promotion** (Sunday is the weakest day; the weekend overall is close to its fair share of weekly revenue)
- 📌 Expand **chicken pizza offerings**
- 📌 Remove **low-performing XL / XXL pizza sizes**
- 📌 Launch **Q4 holiday marketing campaigns**
- 📌 Promote **lunchtime deals around 12 PM**
- 📌 Develop a **signature hero pizza product**

---

# 🚀 Running This Project

```bash
# 1. Clone the repo
git clone https://github.com/maheshchauhan07/Pizza-Sales-Analytics.git
cd Pizza-Sales-Analytics

# 2. Create .env with your AIRFLOW_UID
echo AIRFLOW_UID=50000 > .env

# 3. Build the custom image (dbt baked in)
docker-compose build

# 4. Initialize Airflow (one-time)
docker-compose up airflow-init

# 5. Start everything
docker-compose up -d

# 6. Set up dbt credentials
# Copy dbt/pizza_sales_dbt/profiles.yml.example to
# dbt/pizza_sales_dbt/profiles.yml and fill in your own Snowflake credentials.
# This file is gitignored and never committed.

# 7. In Airflow's UI (localhost:8080), set the snowflake_default connection
# with your account, username, password/token, warehouse, and role.

# 8. Trigger the raw_to_staging_to_mart DAG
# (this runs ingestion, then dbt deps / dbt run / dbt test automatically)
```

---

# 📄 Additional Documentation

- [`Pizza_Sales_Performance_Report_2015_.pdf`](./Pizza_Sales_Performance_Report_2015_.pdf) — full business report
- [`Pizza_ERD.pdf`](./Pizza_ERD.pdf) — entity relationship diagram
- [`Pizza_data_model.pdf`](./Pizza_data_model.pdf) — data model documentation
- [`pizza_dashboard_pictures.pdf`](./pizza_dashboard_pictures.pdf) — Power BI dashboard screenshots

---

# 👤 Author

**Mahesh Chauhan**
Data Analyst | Data Engineering Enthusiast

📍 Berlin, Germany

🔗 [LinkedIn](https://www.linkedin.com/in/mahesh-chauhan-98154a247/)
