-- This SQL script contains exploratory queries executed during the Bronze layer analysis.
-- Queries were tested and run using Beekeeper Studio connected to the local PostgreSQL database.
-- These queries helped validate key fields, detect dirty values, and guide data cleaning decisions.

-- Returns the total number of rows in the bronze.raw_customers table.
SELECT COUNT(*) AS total_rows
FROM bronze.raw_customers;

-- Lists all column names in bronze.raw_customers and shows the total number of columns.
SELECT 
    column_name
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'raw_customers'
ORDER BY column_name;

-- To count how many columns:
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'raw_customers';
  
-- Lists all columns in bronze.raw_customers with their data type.
SELECT 
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'raw_customers'
ORDER BY ordinal_position;

-- Check for NULLs and uniqueness in customer_id and record_uuid
SELECT
  COUNT(*) AS total_rows,

  COUNT(DISTINCT customer_id) AS unique_customer_ids,
  COUNT(customer_id) AS non_null_customer_ids,

  COUNT(DISTINCT record_uuid) AS unique_record_uuids,
  COUNT(record_uuid) AS non_null_record_uuids
FROM bronze.raw_customers;

-- Identifies duplicate record_uuid values in the bronze.raw_customers table.
-- This helps validate whether record_uuid can be used as a reliable primary key.
SELECT record_uuid, COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY record_uuid
HAVING COUNT(*) > 1;

-- Identifies duplicate customer_id values in the bronze.raw_customers table.
-- This helps validate whether customer_id can be used as a reliable primary key.
SELECT customer_id, COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Lists all operator values from bronze.raw_customers along with their frequency.
-- Useful for identifying both common and rare operator entries.
SELECT 
  initcap(trim(operator)) AS operator_formatted,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY initcap(trim(operator))
ORDER BY count DESC;

-- Lists all country values in bronze.raw_customers along with their frequency.
-- Useful to detect common countries and spot typos or dirty values.
SELECT 
  initcap(trim(country)) AS country_formatted,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY initcap(trim(country))
ORDER BY count DESC;

-- Lists all city values in bronze.raw_customers along with their frequency.
-- Useful for identifying frequent cities and spotting inconsistencies or typos.
SELECT 
  initcap(trim(city)) AS city_formatted,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY initcap(trim(city))
ORDER BY count DESC;

-- Lists all plan_type values in bronze.raw_customers with their frequency.
-- Helps identify valid values and potential typos or inconsistent formats.
SELECT 
  initcap(trim(plan_type)) AS plan_type_formatted,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY initcap(trim(plan_type))
ORDER BY count DESC;

-- Lists all lowercase city-country pairs where a city appears with more than one country.
-- Helps detect inconsistencies ignoring casing or formatting.
WITH city_country_counts AS (
  SELECT 
    LOWER(TRIM(city)) AS city_lc,
    LOWER(TRIM(country)) AS country_lc,
    COUNT(*) AS count
  FROM bronze.raw_customers
  GROUP BY LOWER(TRIM(city)), LOWER(TRIM(country))
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

-- Lists only the record_uuid and registration_date for rows with invalid or improperly formatted dates.
SELECT record_uuid, registration_date
FROM bronze.raw_customers
WHERE registration_date IS NULL
   OR TRIM(registration_date) = ''
   OR registration_date = '--'
   OR registration_date NOT LIKE '____-__-__';
   
-- Detects customers with a registration date in the future
SELECT record_uuid, registration_date
FROM bronze.raw_customers
WHERE registration_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND registration_date::date > CURRENT_DATE;
  
  
-- Lists all rows where age is not a valid integer (e.g., contains letters, symbols, decimals, etc.)
SELECT record_uuid, age
FROM bronze.raw_customers
WHERE age IS NOT NULL
  AND age !~ '^\d+$';
  
-- Checks if payment_history or contracted_services still contain array or JSON-like structures
SELECT record_uuid,
       payment_history,
       contracted_services
FROM bronze.raw_customers
WHERE 
    -- Detect array or JSON-style strings
    payment_history LIKE '[%' OR
    contracted_services LIKE '[%';
    
-- Lists all device_brand values and how many times each appears
SELECT 
  device_brand,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY device_brand
ORDER BY count DESC;

-- Lists all unique status values from bronze.raw_customers,
-- formatted with capitalization and trimmed, along with their frequency.
-- Useful for spotting dirty or inconsistent status entries before cleaning.
SELECT 
  initcap(trim(status)) AS status_formatted,
  COUNT(*) AS count
FROM bronze.raw_customers
GROUP BY initcap(trim(status))
ORDER BY count DESC;