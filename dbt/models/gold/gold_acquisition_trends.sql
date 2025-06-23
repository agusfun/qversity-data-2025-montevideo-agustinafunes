-- gold_acquisition_trends.sql
-- Tracks new customer acquisition over time by operator.
-- Source: silver_customers_full (registration_date and operator cleaned)

{{ config(materialized='table') }}

SELECT 
    TO_CHAR(DATE_TRUNC('month', cleaned_registration_date), 'YYYY-MM') AS registration_month,
    cleaned_operator,
    COUNT(DISTINCT cleaned_record_uuid) AS new_customers
FROM {{ ref('silver_customers_full') }}
WHERE cleaned_registration_date IS NOT NULL
GROUP BY registration_month, cleaned_operator
ORDER BY registration_month