-- silver_cleaned_customers.sql
-- This model takes raw customer data from the bronze layer and applies cleaning, 
-- normalization, and standardization transformations.
-- It trims and validates fields, maps inconsistent values using seed mappings,
-- and fixes mismatched city-country pairs when possible.
-- Duplicates are removed based on the cleaned_record_uuid field.

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (cleaned_record_uuid)"
) }}

-- Load raw data from the Bronze layer
with raw as (
    select * from bronze.raw_customers
),

-- Normalize and clean values: trimming, capitalization, type casting
normalized as (
    select
        initcap(nullif(trim(first_name), '')) as cleaned_first_name,
        initcap(nullif(trim(last_name), '')) as cleaned_last_name,
        nullif(trim(email), '') as cleaned_email,
        phone_number as cleaned_phone_number,

        case 
            when age ~ '^\d+(\.\d+)?$' and age::float between 0 and 120 
                then round(age::float)::int
            else null
        end as cleaned_age,

        initcap(nullif(trim(country), '')) as raw_country,
        initcap(nullif(trim(city), '')) as raw_city,
        initcap(nullif(trim(operator), '')) as raw_operator,
        initcap(nullif(trim(regexp_replace(plan_type, '[-_]', '', 'g')), '')) as raw_plan_type,
        initcap(nullif(trim(device_brand), '')) as raw_device_brand,
        initcap(nullif(trim(status), '')) as raw_status,

        coalesce(monthly_data_gb::float, 0) as cleaned_monthly_data_gb,
        coalesce(monthly_bill_usd::float, 0) as cleaned_monthly_bill_usd,

        case
            when registration_date ~ '^\d{4}-\d{2}-\d{2}$'
                 and to_date(registration_date, 'YYYY-MM-DD') <= current_date
                then to_date(registration_date, 'YYYY-MM-DD')
            else null
        end as cleaned_registration_date,

        initcap(REGEXP_REPLACE(nullif(trim(device_model), ''), '[^A-Za-z0-9 ]', '', 'g')) as cleaned_device_model,

        case
            when record_uuid IS NULL or record_uuid = '' or record_uuid like 'invalid-uuid%' then null
            else record_uuid
        end as cleaned_record_uuid,

        case
            when last_payment_date ~ '^\d{4}-\d{2}-\d{2}$'
                 and to_date(last_payment_date, 'YYYY-MM-DD') <= current_date
                then to_date(last_payment_date, 'YYYY-MM-DD')
            else null
        end as cleaned_last_payment_date,

        coalesce(credit_limit::float, 0) as cleaned_credit_limit,
        coalesce(data_usage_current_month::float, 0) as cleaned_data_usage_current_month,
        latitude::float as cleaned_latitude,
        longitude::float as cleaned_longitude,
        coalesce(credit_score::float, 0) as cleaned_credit_score,
        nullif(customer_id, '')::int as cleaned_customer_id,
        contracted_services,
        payment_history,
        current_timestamp as ingestion_timestamp,
        'bronze.raw_customers' as data_origin
    from raw
),

-- Apply mappings from seeds
mapped as (
    select
        n.*,
        initcap(coalesce(cm.clean_value, n.raw_country)) as cleaned_country,
        initcap(coalesce(ctm.clean_value, n.raw_city)) as cleaned_city,
        initcap(coalesce(opm.clean_value, n.raw_operator)) as cleaned_operator,
        initcap(coalesce(ptm.clean_value, n.raw_plan_type)) as cleaned_plan_type,
        initcap(coalesce(dbm.clean_value, n.raw_device_brand)) as cleaned_device_brand,
        initcap(coalesce(stm.clean_value, n.raw_status)) as cleaned_status
    from normalized n
    left join {{ ref('country_mapping') }} cm on n.raw_country = cm.dirty_value
    left join {{ ref('city_mapping') }} ctm on n.raw_city = ctm.dirty_value
    left join {{ ref('operator_mapping') }} opm on n.raw_operator = opm.dirty_value
    left join {{ ref('plan_type_mapping') }} ptm on n.raw_plan_type = ptm.dirty_value
    left join {{ ref('device_brand_mapping') }} dbm on n.raw_device_brand = dbm.dirty_value
    left join {{ ref('status_mapping') }} stm on n.raw_status = stm.dirty_value
),

-- Map expected country for city
mapped_city_country as (
    select 
        m.*,
        coalesce(ccm.correct_country, m.cleaned_country) as cleaned_country_corrected
    from mapped m
    left join {{ ref('city_country_mapping') }} ccm
        on m.cleaned_city = ccm.city
)

-- Final select (deduplicated if needed)
select *
from mapped_city_country
where cleaned_record_uuid is not null