SELECT
    ROW_NUMBER() OVER (ORDER BY od.order_details_id) AS sales_key,
    TO_NUMBER(TO_CHAR(o.date, 'YYYYMMDD')) AS date_key,
    dp.pizza_key,
    od.order_id,
    od.quantity,
    pz.price,
    od.quantity * pz.price AS revenue
FROM {{ ref('stg_order_details') }} od
JOIN {{ ref('stg_orders') }} o
    ON od.order_id = o.order_id
JOIN {{ ref('stg_pizza') }} pz
    ON od.pizza_id = pz.pizza_id
JOIN {{ ref('dim_pizza') }} dp
    ON od.pizza_id = dp.pizza_id