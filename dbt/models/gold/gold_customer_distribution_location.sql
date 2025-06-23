-- gold_customer_distribution_location.sql
-- Shows how customers are distributed by country and city.
-- Source: silver_customers_full (cleaned_country and cleaned_city already cleaned and mapped)

{{ config(materialized='table') }}

SELECT 
    cleaned_country,
    cleaned_city,
    COUNT(DISTINCT cleaned_record_uuid) AS customer_count
FROM {{ ref('silver_customers_full') }}
GROUP BY cleaned_country, cleaned_city
ORDER BY customer_count DESC