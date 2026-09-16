WITH source AS (
    SELECT
        CAST(user_id      AS STRING)    AS user_id,
        CAST(full_name    AS STRING)    AS full_name,
        CAST(email        AS STRING)    AS email,
        CAST(city         AS STRING)    AS city,
        CAST(signup_date  AS TIMESTAMP) AS signup_date
    FROM {{ source('datanexus_raw', 'users') }}
    WHERE user_id IS NOT NULL
      AND email IS NOT NULL
)

SELECT
    user_id,
    full_name,
    email,
    LOWER(email) AS email_normalized,
    city,
    signup_date
FROM source
