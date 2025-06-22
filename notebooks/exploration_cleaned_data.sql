-- This SQL script contains exploratory queries executed during the Silver layer analysis.
-- Queries were tested and run using Beekeeper Studio connected to the local PostgreSQL database.
-- These queries helped validate key fields, check cleaning effectiveness, and surface inconsistencies in the cleaned data.

-- Total number of rows in silver_cleaned_customers
SELECT COUNT(*) AS total_rows
FROM public_silver.silver_cleaned_customers;

-- Column names
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public_silver'
  AND table_name = 'silver_cleaned_customers'
ORDER BY column_name;

-- Count of columns
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'public_silver'
  AND table_name = 'silver_cleaned_customers';

-- Column names with data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public_silver'
  AND table_name = 'silver_cleaned_customers'
ORDER BY ordinal_position;

-- Check for NULLs and uniqueness in IDs
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT cleaned_customer_id) AS unique_cleaned_customer_ids,
  COUNT(cleaned_customer_id) AS non_null_cleaned_customer_ids,
  COUNT(DISTINCT cleaned_record_uuid) AS unique_cleaned_record_uuids,
  COUNT(cleaned_record_uuid) AS non_null_cleaned_record_uuids
FROM public_silver.silver_cleaned_customers;

-- Duplicate UUIDs
SELECT cleaned_record_uuid, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_record_uuid
HAVING COUNT(*) > 1;

-- Duplicate customer IDs
SELECT cleaned_customer_id, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_customer_id
HAVING COUNT(*) > 1;

-- Operator distribution
SELECT cleaned_operator, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_operator
ORDER BY count DESC;

-- Country distribution
SELECT cleaned_country, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_country
ORDER BY count DESC;

-- City distribution
SELECT cleaned_city, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_city
ORDER BY count DESC;

-- Plan type distribution
SELECT cleaned_plan_type, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_plan_type
ORDER BY count DESC;

-- Check for inconsistent city-country pairings
WITH city_country_counts AS (
  SELECT 
    LOWER(cleaned_city) AS city_lc,
    LOWER(cleaned_country) AS country_lc,
    COUNT(*) AS count
  FROM public_silver.silver_cleaned_customers
  GROUP BY LOWER(cleaned_city), LOWER(cleaned_country)
),
suspicious_cities AS (
  SELECT city_lc
  FROM city_country_counts
  GROUP BY city_lc
  HAVING COUNT(*) > 1
)
SELECT *
FROM city_country_counts
WHERE city_lc IN (SELECT city_lc FROM suspicious_cities)
ORDER BY city_lc, count DESC;

-- Invalid or missing registration dates
SELECT cleaned_record_uuid, cleaned_registration_date
FROM public_silver.silver_cleaned_customers
WHERE cleaned_registration_date IS NULL;

-- Future registration dates
SELECT cleaned_record_uuid, cleaned_registration_date
FROM public_silver.silver_cleaned_customers
WHERE cleaned_registration_date > CURRENT_DATE;

-- Non-integer ages
SELECT cleaned_record_uuid, cleaned_age
FROM public_silver.silver_cleaned_customers
WHERE cleaned_age IS NOT NULL
  AND cleaned_age !~ '^\d+$';

-- Device brand distribution
SELECT cleaned_device_brand, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_device_brand
ORDER BY count DESC;

-- Status distribution
SELECT cleaned_status, COUNT(*) AS count
FROM public_silver.silver_cleaned_customers
GROUP BY cleaned_status
ORDER BY count DESC;