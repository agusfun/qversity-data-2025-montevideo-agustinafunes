-- gold_services_summary.sql
-- List of contracted services by customer.
-- Source: silver_customers_full (services_list already included)

{{ config(materialized='table') }}

SELECT 
    cleaned_record_uuid,
    cleaned_customer_id,
    services_list
FROM {{ ref('silver_customers_full') }}
WHERE services_list IS NOT NULL