SELECT
    CAST(order_id AS INT) AS order_id,
    TO_DATE(date) AS date,
    time AS time
FROM {{ source('raw', 'orders_raw') }}