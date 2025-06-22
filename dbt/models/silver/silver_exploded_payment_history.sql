-- silver_exploded_payment_history.sql
-- This model explodes the `payment_history` JSONB column from the `silver_cleaned_customers` table,
-- creating one row per payment.
-- Each row includes the `cleaned_record_uuid`, cleaned payment date, status, and amount.

{{ config(
    materialized='table'
) }}

with source as (
    select *
    from {{ ref('silver_cleaned_customers') }}
    where payment_history is not null
      and jsonb_typeof(payment_history::jsonb) = 'array'
),

exploded as (
    select 
        s.cleaned_record_uuid,

        -- Clean and validate date
        case 
            when ph.value ->> 'date' ~ '^\d{4}-\d{2}-\d{2}$'
                 and to_date(ph.value ->> 'date', 'YYYY-MM-DD') <= current_date
            then to_date(ph.value ->> 'date', 'YYYY-MM-DD')
            else null
        end as payment_date,

        -- Clean status
        initcap(nullif(trim(ph.value ->> 'status'), '')) as payment_status,

        -- Clean amount, coalesced to 0 if null or invalid
        coalesce(
            case 
                when (ph.value ->> 'amount') ~ '^\d+(\.\d+)?$'
                then (ph.value ->> 'amount')::float
                else null
            end,
            0
        ) as payment_amount

    from source s,
         jsonb_array_elements(s.payment_history::jsonb) as ph
)

select *
from exploded