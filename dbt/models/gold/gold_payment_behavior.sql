-- gold_payment_behavior.sql
-- Summarizes payment behavior per user based on payment history.
-- Source: silver_exploded_payment_history

{{ config(materialized='table') }}

SELECT
    cleaned_record_uuid,
    COUNT(*) FILTER (WHERE payment_status ILIKE 'pending') AS pending_payments,
    COUNT(*) FILTER (WHERE payment_status ILIKE 'late' OR payment_status ILIKE 'failed') AS problem_payments,
    ROUND(SUM(payment_amount), 2) AS total_paid
FROM {{ ref('silver_exploded_payment_history') }}
GROUP BY cleaned_record_uuid