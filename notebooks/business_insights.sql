-- SQL file to answer all business questions using Gold layer tables

-- 1. ARPU by plan type
SELECT cleaned_plan_type, ROUND(AVG(arpu), 2) AS avg_arpu FROM public_gold.gold_revenue_summary GROUP BY cleaned_plan_type;

-- 2. Revenue distribution by geographic location
SELECT cleaned_country, SUM(total_revenue) AS total_revenue FROM public_gold.gold_revenue_summary GROUP BY cleaned_country ORDER BY total_revenue DESC;

-- 3. Customer segments generating the highest revenue
    -- Revenue by Country
    -- Revenue by Age
    -- Revenue by Operator
SELECT 
    cs.cleaned_country AS country,
    SUM(r.total_revenue) AS total_revenue
FROM public_gold.gold_revenue_summary r
LEFT JOIN public_gold.gold_customer_summary cs
    ON r.cleaned_record_uuid = cs.cleaned_record_uuid
GROUP BY country
ORDER BY total_revenue DESC;

SELECT 
    CASE 
        WHEN cs.cleaned_age < 18 THEN 'Under 18'
        WHEN cs.cleaned_age BETWEEN 18 AND 30 THEN '18-30'
        WHEN cs.cleaned_age BETWEEN 31 AND 50 THEN '31-50'
        WHEN cs.cleaned_age > 50 THEN 'Over 50'
    END AS age_group,
    SUM(r.total_revenue) AS total_revenue
FROM public_gold.gold_revenue_summary r
LEFT JOIN public_gold.gold_customer_summary cs
    ON r.cleaned_record_uuid = cs.cleaned_record_uuid
WHERE cs.cleaned_age IS NOT NULL
GROUP BY age_group
ORDER BY total_revenue DESC;

SELECT 
    cs.cleaned_operator AS operator,
    SUM(r.total_revenue) AS total_revenue
FROM public_gold.gold_revenue_summary r
LEFT JOIN public_gold.gold_customer_summary cs
    ON r.cleaned_record_uuid = cs.cleaned_record_uuid
GROUP BY operator
ORDER BY total_revenue DESC;

-- 4. Distribution of customers by location
SELECT cleaned_country, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY cleaned_country ORDER BY customer_count DESC;

-- 5. Age distribution by plan type
SELECT cleaned_plan_type, ROUND(AVG(cleaned_age), 1) AS avg_age FROM public_gold.gold_customer_summary GROUP BY cleaned_plan_type;

-- 6. Age distribution by country and operator
SELECT cleaned_country, cleaned_operator, ROUND(AVG(cleaned_age), 1) AS avg_age FROM public_gold.gold_customer_summary GROUP BY cleaned_country, cleaned_operator;

-- 7. Customers distribution across operators
SELECT cleaned_operator, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY cleaned_operator;

-- 8. Customer segmentation by credit score ranges
SELECT CASE WHEN cleaned_credit_score < 300 THEN 'Low' WHEN cleaned_credit_score BETWEEN 300 AND 600 THEN 'Medium' ELSE 'High' END AS credit_score_segment, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY credit_score_segment;

-- 9. Most popular device brands
SELECT cleaned_device_brand, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY cleaned_device_brand ORDER BY customer_count DESC;

-- 10. Device brand preference by country and operator
SELECT cleaned_country, cleaned_operator, cleaned_device_brand, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY cleaned_country, cleaned_operator, cleaned_device_brand ORDER BY customer_count DESC;

-- 11. Device brand preference by plan type
SELECT cleaned_plan_type, cleaned_device_brand, COUNT(DISTINCT cleaned_customer_id) AS customer_count FROM public_gold.gold_customer_summary GROUP BY cleaned_plan_type, cleaned_device_brand ORDER BY customer_count DESC;

-- 12. Most commonly contracted services
SELECT 
    TRIM(service) AS service,
    COUNT(DISTINCT cleaned_customer_id) AS customer_count
FROM public_gold.gold_services_summary,
     LATERAL UNNEST(STRING_TO_ARRAY(services_list, ',')) AS service
WHERE services_list IS NOT NULL
GROUP BY TRIM(service)
ORDER BY customer_count DESC;

-- 13. Most popular service combinations
SELECT 
    services_list,
    COUNT(DISTINCT cleaned_customer_id) AS customer_count
FROM public_gold.gold_services_summary
WHERE services_list IS NOT NULL
GROUP BY services_list
ORDER BY customer_count DESC
LIMIT 5;

-- 14. Percentage of customers with payment issues
SELECT 
    ROUND(
        100.0 * COUNT(DISTINCT cleaned_record_uuid) FILTER (WHERE problem_payments > 0) 
        / COUNT(DISTINCT cleaned_record_uuid),
    2) AS pct_with_payment_issues
FROM public_gold.gold_payment_behavior;

-- 15. Customers with pending payments
SELECT COUNT(*) AS customers_with_pending_payments
FROM public_gold.gold_payment_behavior
WHERE pending_payments > 0;

-- 16. Credit score correlation with payment behavior (approximation)
SELECT 
    cs.cleaned_credit_score, 
    ROUND(AVG(pb.problem_payments), 2) AS avg_problem_payments
FROM public_gold.gold_payment_behavior pb 
LEFT JOIN public_gold.gold_customer_summary cs 
USING (cleaned_record_uuid) 
GROUP BY cs.cleaned_credit_score 
ORDER BY cs.cleaned_credit_score
LIMIT 10;

-- 17. Distribution of new customers over time
SELECT 
    registration_month, 
    SUM(new_customers) AS total_new_customers
FROM public_gold.gold_acquisition_trends
GROUP BY registration_month
ORDER BY registration_month
LIMIT 10;

-- 18. Customer acquisition trends by operator
SELECT 
    registration_month, 
    cleaned_operator, 
    SUM(new_customers) AS total_new_customers
FROM public_gold.gold_acquisition_trends 
GROUP BY registration_month, cleaned_operator 
ORDER BY registration_month, cleaned_operator
LIMIT 10;

-- 19. Percentage of customers by status
SELECT 
    cleaned_status, 
    ROUND(100.0 * COUNT(DISTINCT cleaned_record_uuid) / SUM(COUNT(DISTINCT cleaned_record_uuid)) OVER (), 2) AS pct_customers 
FROM public_gold.gold_customer_summary 
WHERE cleaned_status IN ('Active', 'Suspended', 'Inactive')
GROUP BY cleaned_status;

-- 20. Service combinations driving highest revenue
SELECT 
    ss.services_list, 
    ROUND(SUM(rs.total_revenue), 2) AS total_revenue 
FROM public_gold.gold_services_summary ss 
JOIN public_gold.gold_revenue_summary rs 
USING (cleaned_record_uuid) 
WHERE ss.services_list IS NOT NULL 
GROUP BY ss.services_list 
ORDER BY total_revenue DESC
LIMIT 10;

-- 21. Mean and median revenue per user by plan type and operator
SELECT 
    cleaned_plan_type, 
    cleaned_operator, 
    ROUND(AVG(total_revenue), 2) AS mean_revenue, 
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue)::numeric, 2) AS median_revenue 
FROM public_gold.gold_revenue_summary 
GROUP BY cleaned_plan_type, cleaned_operator;