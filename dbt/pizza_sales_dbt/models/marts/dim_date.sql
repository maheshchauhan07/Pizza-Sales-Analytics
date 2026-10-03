SELECT DISTINCT
    TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS date_key,
    date AS full_date,
    YEAR(date) AS year,
    MONTH(date) AS month,
    TO_CHAR(date, 'MMMM') AS month_name,
    DAY(date) AS day,
    TO_CHAR(date, 'DY') AS day_name,
    QUARTER(date) AS quarter,
    CASE WHEN DAYOFWEEKISO(date) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM {{ ref('stg_orders') }}