-- gold_customer_summary.sql
-- Summary of customer information for analysis.
-- Source: silver_customers_full (already contains cleaned and enriched customer data)

{{ config(materialized='table') }}

SELECT 
    cleaned_record_uuid,
    cleaned_customer_id,
    cleaned_country,
    cleaned_city,
    cleaned_operator,
    cleaned_plan_type,
    cleaned_status,
    cleaned_age,
    cleaned_credit_score,
    cleaned_device_brand,
    cleaned_registration_date
FROM {{ ref('silver_customers_full') }}