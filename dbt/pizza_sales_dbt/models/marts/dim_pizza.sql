SELECT
    ROW_NUMBER() OVER (ORDER BY p.pizza_id) AS pizza_key,
    p.pizza_id,
    pt.name AS pizza_name,
    pt.category,
    p.size,
    pt.ingredients
FROM {{ ref('stg_pizza') }} p
JOIN {{ ref('stg_pizza_types') }} pt
    ON p.pizza_type_id = pt.pizza_type_id