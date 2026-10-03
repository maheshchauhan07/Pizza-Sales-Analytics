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

# ------------------------
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
    # 6 Run dbt to build STAGING + MART
    # -------------------------------
    from airflow.operators.bash import BashOperator

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt/pizza_sales_dbt && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .",    )

    for task in load_tasks:
        task >> run_dbt
