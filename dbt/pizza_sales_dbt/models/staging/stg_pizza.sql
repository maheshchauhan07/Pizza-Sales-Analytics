SELECT
    TRIM(pizza_id) AS pizza_id,
    TRIM(pizza_type_id) AS pizza_type_id,
    TRIM(size) AS size,
    CAST(price AS FLOAT) AS price
FROM {{ source('raw', 'pizza_raw') }}