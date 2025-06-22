-- silver_services_list.sql
-- This model transforms the `contracted_services` JSONB array from the `silver_cleaned_customers` table
-- into a single `services_list` column.
-- Each row contains the `cleaned_record_uuid` and a comma-separated string of normalized services.

{{ config(
    materialized='table'
) }}

with source as (
    select *
    from {{ ref('silver_cleaned_customers') }}
    where jsonb_typeof(contracted_services::jsonb) = 'array'
),

exploded_cleaned as (
    select
        cleaned_record_uuid,
        initcap(trim(translate(cs.value::text, '\"', ''))) as cleaned_service
    from source,
         jsonb_array_elements_text(contracted_services::jsonb) as cs
    where trim(translate(cs.value::text, '\"', '')) <> ''
),

grouped as (
    select
        cleaned_record_uuid,
        string_agg(cleaned_service, ', ') as services_list
    from exploded_cleaned
    group by cleaned_record_uuid
)

select *
from grouped