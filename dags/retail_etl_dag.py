"""
Airflow DAG for Retail Data Pipeline
This DAG orchestrates the full ELT process:
1. Load CSV data to Snowflake (Extract & Load)
2. Run dbt transformations (Transform)
3. Run dbt tests for data quality

Schedule: Daily at 2 AM UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts directory to Python path
sys.path.append('/path/to/your/retail_pipeline/scripts')

# Import the load function from our script
from load_csv_to_snowflake import main as load_csv_to_snowflake

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Define the DAG
with DAG(
    dag_id='retail_etl_pipeline',
    default_args=default_args,
    description='End-to-end retail data pipeline: CSV -> Snowflake -> dbt',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=['retail', 'etl', 'snowflake', 'dbt'],
    max_active_runs=1,  # Only one DAG run at a time
) as dag:

    # Task 1: Load CSV data to Snowflake
    load_data_task = PythonOperator(
        task_id='load_csv_to_snowflake',
        python_callable=load_csv_to_snowflake,
        doc_md="""
        ### Load CSV to Snowflake
        Extracts data from OnlineRetail.csv and loads it into 
        the RAW.ONLINE_RETAIL table in Snowflake.
        
        **Source**: data/OnlineRetail.csv
        **Target**: RETAIL_DW.RAW.ONLINE_RETAIL
        """
    )

    # Task 2: Install dbt dependencies
    dbt_deps = BashOperator(
        task_id='dbt_install_dependencies',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt deps',
        doc_md="""
        ### Install dbt Packages
        Installs required dbt packages (dbt_utils) defined in packages.yml
        """
    )

    # Task 3: Run dbt models (staging layer)
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt run --models staging',
        doc_md="""
        ### Run Staging Models
        Executes dbt staging models that clean and standardize raw data.
        
        **Models executed**: stg_online_retail
        **Target schema**: RETAIL_DW.STAGING
        """
    )

    # Task 4: Test staging models
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt test --models staging',
        doc_md="""
        ### Test Staging Models
        Runs data quality tests on staging models (null checks, etc.)
        """
    )

    # Task 5: Run dbt models (analytics layer)
    dbt_run_analytics = BashOperator(
        task_id='dbt_run_analytics',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt run --models analytics',
        doc_md="""
        ### Run Analytics Models
        Executes dbt analytics models (dimensional model).
        
        **Models executed**:
        - dim_customers
        - dim_products
        - fact_sales
        
        **Target schema**: RETAIL_DW.ANALYTICS
        """
    )

    # Task 6: Test analytics models
    dbt_test_analytics = BashOperator(
        task_id='dbt_test_analytics',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt test --models analytics',
        doc_md="""
        ### Test Analytics Models
        Runs data quality tests on analytics models including:
        - Primary key uniqueness
        - Foreign key relationships
        - Data integrity checks
        """
    )

    # Task 7: Generate dbt documentation
    dbt_docs_generate = BashOperator(
        task_id='dbt_generate_docs',
        bash_command='cd /path/to/your/retail_pipeline/dbt_project && dbt docs generate',
        doc_md="""
        ### Generate dbt Documentation
        Creates updated data lineage and catalog documentation
        """
    )

    # Define task dependencies (execution order)
    # This creates the workflow: Load -> Transform -> Test
    load_data_task >> dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_analytics >> dbt_test_analytics >> dbt_docs_generate


# Task execution flow explanation:
# 1. load_csv_to_snowflake: Loads raw CSV data into Snowflake
# 2. dbt_install_dependencies: Installs required dbt packages
# 3. dbt_run_staging: Creates staging views with cleaned data
# 4. dbt_test_staging: Validates staging data quality
# 5. dbt_run_analytics: Creates dimension and fact tables
# 6. dbt_test_analytics: Validates analytics layer integrity
# 7. dbt_generate_docs: Updates data documentation