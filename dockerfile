FROM apache/airflow:3.1.5
RUN pip install --no-cache-dir dbt-snowflake==1.12.*