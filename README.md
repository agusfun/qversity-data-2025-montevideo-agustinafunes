## Project Structure

```
QVERSITY-DATA-FINAL-PROJECT-2025/
├── dags/                           # Airflow DAGs for pipeline orchestration
│   ├── dag_bronze.py
│   ├── dag_gold.py
│   ├── dag_pipeline.py
│   ├── dag_silver.py
│   ├── dag_tests.py
│
├── data/
│   ├── raw/
│       ├── mobile_customers_messy_dataset.json
│
├── dbt/
│   ├── models/
│       ├── bronze/
│       ├── gold/
│           ├── gold_acquisition_trends.sql
│           ├── gold_customer_distribution_location.sql
│           ├── gold_customer_summary.sql
│           ├── gold_payment_behavior.sql
│           ├── gold_revenue_summary.sql
│           ├── gold_services_summary.sql
│       ├── silver/
│           ├── silver_cleaned_customers.sql
│           ├── silver_customers_full.sql
│           ├── silver_exploded_payment_history.sql
│           ├── silver_services_list.sql
│   ├── seeds/mappings/
│       ├── city_country_mapping.csv
│       ├── city_mapping.csv
│       ├── country_mapping.csv
│       ├── device_brand_mapping.csv
│       ├── operator_mapping.csv
│       ├── plan_type_mapping.csv
│       ├── status_mapping.csv
│
├── tests/                          # dbt test configurations
│   ├── gold_summary_tests.yml
│   ├── silver_cleaned_customers_tests.yml
│
├── images/
│   ├── silver_layer_ERD.png
│   ├── gold_layer_ERD.png          # ERD for Gold Layer tables
│
├── notebooks/
│   ├── business_insights.sql       # Business Insights queries for Gold Layer
│   ├── exploration_cleaned_data.sql
│   ├── exploration_raw_data.sql
│
├── .gitignore
├── business_insights.md            # Business Insights results for Gold Layer
├── docker-compose.yml
├── README.md
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
```

## All dbt commands are executed from the Airflow container since it already has the project mounted at `/opt/airflow/dbt` with access to models, seeds, and profiles. The standalone dbt container was not used to simplify configuration.

## Pipeline Overview

- The **Bronze** layer ingests raw JSON data using Python and stores it in PostgreSQL.
- The **Silver** layer applies cleaning, normalization, and JSON explosion using dbt and PostgreSQL functions.
- The **Gold** layer generates curated, analysis-ready tables.
- Tests are defined with dbt and executed via a dedicated Airflow DAG.
- The pipeline is orchestrated by `dag_pipeline.py`, which chains all DAGs together. Running this DAG triggers the full process.

## Technical Notes

### Bronze Layer: JSON Ingestion

Python's `json.dumps()` was used to serialize nested arrays and dictionaries before inserting them into PostgreSQL. This ensures valid JSON storage and enables later use of functions like `jsonb_array_elements()`.

### Silver Layer: Exploding JSON

Serialized JSON fields are cast to `jsonb`, validated as arrays, and exploded into individual rows using `jsonb_array_elements()`. Cleaning and validation are applied during this step.

An Entity-Relationship Diagram (ERD) of the Silver Layer tables is available in: `images/silver_layer_ERD.png`.

### Silver Layer: Mapping Files

CSV seeds provide lookups for country, city, operator, plan type, device brand, and status. This allows standardization of dirty or inconsistent real-world data without hardcoding cleaning logic.

## Gold Layer: Business Insights

The SQL queries to answer business questions for the Gold Layer are located in: `notebooks/business_insights.sql`.

The results of these queries are documented in: `business_insights.md`.

An Entity-Relationship Diagram (ERD) of the Gold Layer tables is available in: `images/gold_layer_ERD.png`.
