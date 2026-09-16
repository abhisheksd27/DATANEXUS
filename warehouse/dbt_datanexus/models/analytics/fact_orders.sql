WITH orders AS (
    SELECT
        order_surrogate_key,
        order_id,
        user_id,
        total_amount,
        status,
        city,
        created_at
    FROM {{ ref('stg_orders') }}
),

users AS (
    SELECT
        user_id,
        full_name,
        email_normalized
    FROM {{ ref('stg_users') }}
)

SELECT
    o.order_id,
    o.order_surrogate_key,
    o.user_id,
    u.full_name,
    o.city,
    u.email_normalized,
    o.total_amount,
    o.status,
    o.created_at,
    EXTRACT(DATE FROM o.created_at) AS order_date
FROM orders AS o
LEFT JOIN users AS u
    ON o.user_id = u.user_id
