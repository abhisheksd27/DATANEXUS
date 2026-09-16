WITH source AS (
    SELECT
        CAST(order_id       AS STRING)    AS order_id,
        CAST(user_id        AS STRING)    AS user_id,
        CAST(total_amount   AS FLOAT64)   AS total_amount,
        CAST(status         AS STRING)    AS status,
        CAST(city           AS STRING)    AS city,
        CAST(created_at     AS TIMESTAMP) AS created_at
    FROM {{ source('datanexus_raw', 'orders') }}
    WHERE order_id IS NOT NULL
      AND total_amount > 0
)

SELECT
    TO_HEX(MD5(order_id)) AS order_surrogate_key,
    order_id,
    user_id,
    total_amount,
    status,
    city,
    created_at
FROM source
