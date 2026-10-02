SELECT
    TRIM(pizza_type_id) AS pizza_type_id,
    TRIM(name) AS name,
    TRIM(category) AS category,
    TRIM(ingredients) AS ingredients
FROM {{ source('raw', 'pizza_types_raw') }}