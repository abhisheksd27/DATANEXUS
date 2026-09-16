WITH fact AS (
    SELECT
        order_date,
        city,
        total_amount,
        status,
        order_id
    FROM {{ ref('fact_orders') }}
)

SELECT
    order_date,
    city,
    SUM(total_amount)             AS total_revenue,
    AVG(total_amount)             AS avg_order_value,
    COUNT(order_id)               AS total_orders,
    COUNTIF(status = 'COMPLETED') AS completed_orders
FROM fact
GROUP BY
    order_date,
    city
