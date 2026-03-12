from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
import yaml

# -------------------------------
# CONFIG PATHS
# -------------------------------
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")
CONFIG_PATH = "/opt/airflow/config/tables.yml"  # contains column names + types for staging

# Snowflake settings
DB_NAME = "pizza"
RAW_SCHEMA = "PUBLIC"
STAGING_SCHEMA = "STAGING"
STAGE_NAME = "pizza"
WAREHOUSE_NAME = "my_load_wh"

# -------------------------------
# LOAD METADATA YAML
# -------------------------------
with open(CONFIG_PATH, "r") as f:
    table_config = yaml.safe_load(f)

tables = table_config["tables"]

# -------------------------------
# DEFINE DAG
# -------------------------------
with DAG(
    dag_id="raw_to_staging_to_mart",
    start_date=datetime(2026, 1, 16),
    schedule=None,
    catchup=False,
    tags=["snowflake", "staging", "etl"],
    description="Push RAW → STAGING with type casting and finally mart",
) as dag:

    # -------------------------------
    # 1 Create session, DB, schema, warehouse
    # -------------------------------
    set_session = SQLExecuteQueryOperator(
        task_id="setting_up_session",
        conn_id="snowflake_default",
        sql=f"""
            CREATE DATABASE IF NOT EXISTS {DB_NAME};
            USE DATABASE {DB_NAME};

            CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};
            USE SCHEMA {RAW_SCHEMA};

            CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE_NAME}
            WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE;

            USE WAREHOUSE {WAREHOUSE_NAME};
        """,
        autocommit=True,
        execution_timeout=timedelta(minutes=2),
    )

    # -------------------------------
    # 2 Create Stage
    # -------------------------------
    create_stage = SQLExecuteQueryOperator(
        task_id="creating_stage",
        conn_id="snowflake_default",
        sql=f"CREATE OR REPLACE STAGE {DB_NAME}.{RAW_SCHEMA}.{STAGE_NAME};",
        autocommit=True,
    )

    set_session >> create_stage

    # -------------------------------
    # 3 Dynamically create RAW tables
    # -------------------------------
    create_raw_tasks = []

    for table in tables:
        table_name = table["raw_table"]
        columns = table["columns"].keys()  # we still create RAW as STRING

        columns_sql = ",\n".join([f"{col} STRING" for col in columns])
        create_sql = f"""
        CREATE OR REPLACE TABLE {DB_NAME}.{RAW_SCHEMA}.{table_name} (
            {columns_sql}
        );
        """

        create_task = SQLExecuteQueryOperator(
            task_id=f"create_{table_name}",
            conn_id="snowflake_default",
            sql=create_sql,
            autocommit=True,
        )

        create_stage >> create_task
        create_raw_tasks.append(create_task)

    # -------------------------------
    # 4 PUT CSVs into Snowflake Stage
    # -------------------------------
    copy_tasks = []

    for table in tables:
        file_path = os.path.join(DATA_FOLDER, table["file"])
        put_sql = f"""
        PUT file://{file_path} @{DB_NAME}.{RAW_SCHEMA}.{STAGE_NAME} OVERWRITE = TRUE;
        """

        copy_task = SQLExecuteQueryOperator(
            task_id=f"put_{table['raw_table']}",
            conn_id="snowflake_default",
            sql=put_sql,
            autocommit=True,
        )

        # Make copy dependent on RAW table creation
        create_task = [t for t in create_raw_tasks if t.task_id == f"create_{table['raw_table']}"][0]
        create_task >> copy_task
        copy_tasks.append(copy_task)

    # -------------------------------
    # 5 COPY INTO RAW TABLES
    # -------------------------------
    load_tasks = []

    for table, copy_task in zip(tables, copy_tasks):
        load_sql = f"""
        COPY INTO {DB_NAME}.{RAW_SCHEMA}.{table['raw_table']}
        FROM @{DB_NAME}.{RAW_SCHEMA}.{STAGE_NAME}/{table['file']}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ENCODING = 'ISO-8859-1'
        )
        ON_ERROR = CONTINUE;
        """

        load_task = SQLExecuteQueryOperator(
            task_id=f"load_{table['raw_table']}",
            conn_id="snowflake_default",
            sql=load_sql,
            autocommit=True,
        )

        copy_task >> load_task
        load_tasks.append(load_task)

    # -------------------------------
    # 6 Create STAGING Tables with Type Casting
    # -------------------------------
    create_staging_schema = SQLExecuteQueryOperator(
        task_id="create_staging_schema",
        conn_id="snowflake_default",
        sql=f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{STAGING_SCHEMA};",
        autocommit=True,
    )

    # All load tasks must finish before creating staging schema
    for task in load_tasks:
        task >> create_staging_schema

    staging_tasks = []

    for table in tables:
        table_name = table["raw_table"]
        column_types = table["columns"]

        staging_cols_sql = []

        for col_name, col_type in column_types.items():
            if col_type.upper() == "STRING":
                staging_cols_sql.append(f"TRIM({col_name}) AS {col_name}")
            elif col_type.upper() == "INT":
                staging_cols_sql.append(f"CAST({col_name} AS INT) AS {col_name}")
            elif col_type.upper() == "FLOAT":
                staging_cols_sql.append(f"CAST({col_name} AS FLOAT) AS {col_name}")
            elif col_type.upper() == "DATE":
                staging_cols_sql.append(f"TO_DATE({col_name}) AS {col_name}")
            else:
                staging_cols_sql.append(col_name)  # fallback

        cols_sql = ",\n".join(staging_cols_sql)

        staging_sql = f"""
        CREATE OR REPLACE TABLE {DB_NAME}.{STAGING_SCHEMA}.{table_name}_stg AS
        SELECT {cols_sql}
        FROM {DB_NAME}.{RAW_SCHEMA}.{table_name};
        """

        staging_task = SQLExecuteQueryOperator(
            task_id=f"stg_{table_name}",
            conn_id="snowflake_default",
            sql=staging_sql,
            autocommit=True,
        )

        create_staging_schema >> staging_task
        staging_tasks.append(staging_task)




# creating data mart final step 

    create_mart_schema = SQLExecuteQueryOperator(
    task_id="create_mart_schema",
    conn_id="snowflake_default",
    sql=f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.MART;",
    autocommit=True,
    )

    for task in staging_tasks:
        task >> create_mart_schema


    create_dim_date = SQLExecuteQueryOperator(
    task_id="create_dim_date",
    conn_id="snowflake_default",
    sql=f"""
    CREATE OR REPLACE TABLE {DB_NAME}.MART.DIM_DATE AS
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS date_key,
        date AS full_date,
        time as full_time,
        YEAR(date) AS year,
        MONTH(date) AS month,
        TO_CHAR(date, 'MMMM') AS month_name,
        DAY(date) AS day,
        TO_CHAR(date, 'DY') AS day_name,
        QUARTER(date) AS quarter,
        CASE WHEN DAYOFWEEK(date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM {DB_NAME}.{STAGING_SCHEMA}.orders_raw_stg;
    """,
    autocommit=True,
    )

    create_mart_schema >> create_dim_date


    create_dim_pizza = SQLExecuteQueryOperator(
    task_id="create_dim_pizza",
    conn_id="snowflake_default",
    sql=f"""
    CREATE OR REPLACE TABLE {DB_NAME}.MART.DIM_PIZZA AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY p.pizza_id) AS pizza_key,
        p.pizza_id,
        pt.name AS pizza_name,
        pt.category,
        p.size,
        pt.ingredients
    FROM {DB_NAME}.{STAGING_SCHEMA}.pizza_raw_stg p
    JOIN {DB_NAME}.{STAGING_SCHEMA}.pizza_types_raw_stg pt
        ON p.pizza_type_id = pt.pizza_type_id;
    """,
    autocommit=True,
    )

    create_mart_schema >> create_dim_pizza


    create_fact_sales = SQLExecuteQueryOperator(
    task_id="create_fact_sales",
    conn_id="snowflake_default",
    sql=f"""
    CREATE OR REPLACE TABLE {DB_NAME}.MART.FACT_table AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY od.order_details_id) AS sales_key,
        TO_NUMBER(TO_CHAR(o.date, 'YYYYMMDD')) AS date_key,
        dp.pizza_key,
        od.order_id,
        od.quantity,
        pz.price,
        od.quantity * pz.price AS revenue
    FROM {DB_NAME}.{STAGING_SCHEMA}.order_details_raw_stg od
    JOIN {DB_NAME}.{STAGING_SCHEMA}.orders_raw_stg o
        ON od.order_id = o.order_id
    JOIN {DB_NAME}.{STAGING_SCHEMA}.pizza_raw_stg pz   
    ON od.pizza_id = pz.pizza_id
    JOIN {DB_NAME}.MART.DIM_PIZZA dp
        ON od.pizza_id = dp.pizza_id;
    """,
    autocommit=True,
    )

    create_dim_date >> create_fact_sales
    create_dim_pizza >> create_fact_sales




    