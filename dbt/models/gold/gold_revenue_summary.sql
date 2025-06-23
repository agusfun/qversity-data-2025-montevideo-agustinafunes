-- gold_revenue_summary.sql
-- Calculates revenue and ARPU (Average Revenue Per User) by customer and plan.
-- Source: silver_customers_full 
-- Note: cleaned_customer_id comes from silver_cleaned_customers.

{{ config(materialized='table') }}

SELECT 
    cleaned_record_uuid,
    cleaned_customer_id,
    cleaned_plan_type,
    cleaned_operator,
    cleaned_country,
    ROUND(SUM(COALESCE(payment_amount, 0))::numeric, 2) AS total_revenue,
    COUNT(*) FILTER (WHERE COALESCE(payment_amount, 0) > 0) AS payments_count,
    ROUND(
        COALESCE(
            SUM(COALESCE(payment_amount, 0)) / NULLIF(COUNT(DISTINCT cleaned_customer_id), 0),
            0
        )::numeric, 2
    ) AS arpu
FROM {{ ref('silver_customers_full') }}
GROUP BY cleaned_record_uuid, cleaned_customer_id, cleaned_plan_type, cleaned_operator, cleaned_country