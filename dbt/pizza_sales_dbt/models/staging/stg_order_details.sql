SELECT
    CAST(order_details_id AS INT) AS order_details_id,
    CAST(order_id AS INT) AS order_id,
    TRIM(pizza_id) AS pizza_id,
    CAST(quantity AS INT) AS quantity
FROM {{ source('raw', 'order_details_raw') }}