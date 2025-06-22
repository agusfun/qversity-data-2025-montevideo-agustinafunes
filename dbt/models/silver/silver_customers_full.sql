-- silver_customer_full.sql
-- This model joins `silver_cleaned_customers` with `silver_exploded_payment_history` 
-- and `silver_services_list` using LEFT JOINs on `cleaned_record_uuid`.
-- It results in a denormalized table where each row may represent a combination of customer,
-- payment record, and a list of services.

{{ config(
    materialized='table'
) }}

with customers as (
    select *
    from {{ ref('silver_cleaned_customers') }}
),

payments as (
    select *
    from {{ ref('silver_exploded_payment_history') }}
),

services as (
    select *
    from {{ ref('silver_services_list') }}  -- Updated to reflect services list
),

joined as (
    select
        c.*,
        p.payment_date,
        p.payment_status,
        p.payment_amount,
        s.services_list  -- Here we now select the list of services
    from customers c
    left join payments p on c.cleaned_record_uuid = p.cleaned_record_uuid
    left join services s on c.cleaned_record_uuid = s.cleaned_record_uuid
)

select *
from joined