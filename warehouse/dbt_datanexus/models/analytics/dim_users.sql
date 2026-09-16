WITH users AS (
    SELECT
        user_id,
        full_name,
        email_normalized,
        city,
        signup_date
    FROM {{ ref('stg_users') }}
)

SELECT
    user_id,
    full_name,
    email_normalized,
    city,
    signup_date,
    DATE_DIFF(CURRENT_DATE(), DATE(signup_date), DAY) AS days_since_signup
FROM users
