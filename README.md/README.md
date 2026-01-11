# End-to-End Retail Data Pipeline

![Pipeline Architecture](https://img.shields.io/badge/Python-3.9+-blue) ![dbt](https://img.shields.io/badge/dbt-1.7+-orange) ![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.7+-red)

## 📋 Project Overview

A production-ready data engineering pipeline that demonstrates modern ELT (Extract, Load, Transform) architecture. This project extracts retail transaction data from CSV files, loads it into Snowflake, transforms it using dbt into a dimensional model, and orchestrates the entire workflow with Apache Airflow.

**Dataset**: Online Retail Dataset (541,909 transactions)  
**Tech Stack**: Python, SQL, dbt, Snowflake, Apache Airflow

## 🏗️ Architecture

```
CSV Files (OnlineRetail.csv)
         ↓
    [Python Script] ← Extract & Load
         ↓
Snowflake RAW Layer (RETAIL_DW.RAW)
         ↓
    [dbt Models] ← Transform
         ↓
Snowflake STAGING Layer (RETAIL_DW.STAGING)
         ↓
    [dbt Models] ← Dimensional Modeling
         ↓
Snowflake ANALYTICS Layer (RETAIL_DW.ANALYTICS)
    ├── dim_customers
    ├── dim_products
    └── fact_sales
         ↓
    [Airflow DAG] ← Orchestration
```

## 🎯 Key Features

- **Modular Design**: Separate layers for raw, staging, and analytics
- **Data Quality**: Built-in dbt tests for data validation
- **Scalability**: Batch processing with configurable batch sizes
- **Automation**: Scheduled daily runs via Airflow
- **Documentation**: Auto-generated data lineage with dbt docs
- **Best Practices**: Dimensional modeling with facts and dimensions

## 📊 Data Model

### Dimensional Model (Star Schema)

**Fact Table**:
- `fact_sales` - Grain: One row per invoice line item (541K+ rows)

**Dimension Tables**:
- `dim_customers` - Customer attributes and lifetime metrics
- `dim_products` - Product information and sales performance

### Key Metrics Available

**Customer Metrics**:
- Total revenue per customer
- Customer lifetime value
- Order frequency
- Average order value
- Customer segmentation (High/Medium/Low value)

**Product Metrics**:
- Total units sold
- Revenue by product
- Price variance analysis
- Product popularity ranking

**Sales Metrics**:
- Daily/Monthly/Quarterly sales
- Sales by country
- Sales trends over time

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Snowflake account (free trial available)
- Apache Airflow 2.7+
- dbt-core with snowflake adapter

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/retail-data-pipeline.git
cd retail-data-pipeline
```

2. **Set up virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure Snowflake connection**

Edit `config/config.py` with your Snowflake credentials:
```python
SNOWFLAKE_CONFIG = {
    'user': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'account': 'YOUR_ACCOUNT',
    'warehouse': 'COMPUTE_WH',
    'database': 'RETAIL_DW',
    'schema': 'RAW'
}
```

5. **Set up Snowflake tables**
```sql
-- Run in Snowflake UI
CREATE DATABASE RETAIL_DW;
CREATE SCHEMA RETAIL_DW.RAW;
CREATE SCHEMA RETAIL_DW.STAGING;
CREATE SCHEMA RETAIL_DW.ANALYTICS;

-- Create raw table (see setup SQL in docs)
```

6. **Configure dbt**

Edit `dbt_project/profiles.yml` with your credentials, then:
```bash
cd dbt_project
dbt deps  # Install dbt packages
dbt debug  # Test connection
```

### Running the Pipeline

#### Option 1: Manual Execution

**Step 1: Load data to Snowflake**
```bash
python scripts/load_csv_to_snowflake.py
```

**Step 2: Run dbt transformations**
```bash
cd dbt_project
dbt run  # Run all models
dbt test  # Run all tests
```

**Step 3: View documentation**
```bash
dbt docs generate
dbt docs serve  # Opens in browser at localhost:8080
```

#### Option 2: Automated with Airflow

1. **Copy DAG to Airflow**
```bash
cp dags/retail_etl_dag.py $AIRFLOW_HOME/dags/
```

2. **Update paths in DAG file**
Edit `retail_etl_dag.py` and update:
- `/path/to/your/retail_pipeline/` with your actual path

3. **Trigger DAG**
```bash
# Via CLI
airflow dags trigger retail_etl_pipeline

# Or use Airflow UI at localhost:8080
```

## 📁 Project Structure

```
retail_pipeline/
├── config/
│   └── config.py              # Snowflake connection settings
├── dags/
│   └── retail_etl_dag.py      # Airflow orchestration
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_online_retail.sql
│   │   │   └── schema.yml
│   │   └── analytics/
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── fact_sales.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── scripts/
│   └── load_csv_to_snowflake.py
├── data/
│   └── OnlineRetail.csv
└── README.md
```

## 🔍 Sample Queries

Once the pipeline runs, you can query the analytics layer:

```sql
-- Top 10 customers by revenue
SELECT 
    customer_id,
    country,
    total_revenue,
    total_orders,
    customer_segment
FROM RETAIL_DW.ANALYTICS.DIM_CUSTOMERS
ORDER BY total_revenue DESC
LIMIT 10;

-- Monthly sales trend
SELECT 
    DATE_TRUNC('month', invoice_date) AS month,
    COUNT(DISTINCT invoice_number) AS num_orders,
    SUM(line_total) AS total_revenue,
    AVG(line_total) AS avg_order_value
FROM RETAIL_DW.ANALYTICS.FACT_SALES
GROUP BY 1
ORDER BY 1;

-- Top selling products
SELECT 
    p.product_description,
    p.total_quantity_sold,
    p.total_revenue,
    p.product_category
FROM RETAIL_DW.ANALYTICS.DIM_PRODUCTS p
ORDER BY total_revenue DESC
LIMIT 10;
```

## 🧪 Data Quality Tests

The pipeline includes automated data quality checks:

- **Uniqueness**: Primary keys are unique
- **Not Null**: Required fields have values
- **Referential Integrity**: Foreign keys match dimension tables
- **Accepted Values**: Categorical fields have valid values
- **Custom Tests**: Business rule validations

Run tests:
```bash
dbt test  # All tests
dbt test --models staging  # Staging only
dbt test --models analytics  # Analytics only
```

## 📈 Performance Metrics

- **Data Volume**: 541,909 transactions processed
- **Load Time**: ~30 seconds for full CSV load
- **Transform Time**: ~45 seconds for all dbt models
- **Total Pipeline Runtime**: ~2 minutes end-to-end

## 🛠️ Technologies Used

| Technology | Purpose |
|-----------|---------|
| Python | Data extraction and loading |
| SQL | Data transformation logic |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation framework |
| Apache Airflow | Workflow orchestration |
| Git | Version control |

## 📚 Learning Outcomes

This project demonstrates:

1. **ELT Architecture**: Modern cloud-based data warehousing approach
2. **Dimensional Modeling**: Star schema with facts and dimensions
3. **Data Quality**: Testing and validation at every layer
4. **Orchestration**: Scheduling and dependency management
5. **Best Practices**: Code organization, documentation, and modularity
6. **SQL Skills**: Complex transformations, window functions, CTEs
7. **Cloud Platforms**: Snowflake warehouse and compute management

## 🔮 Future Enhancements

- [ ] Add incremental loading (CDC)
- [ ] Implement data lineage visualization
- [ ] Add machine learning predictions
- [ ] Create BI dashboards (Tableau/Power BI)
- [ ] Add data quality monitoring alerts
- [ ] Implement data versioning with dbt snapshots

## 👤 Author

**Your Name**  
Data Engineer  
[LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourusername) | [Email](mailto:your.email@example.com)

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Dataset: UCI Machine Learning Repository
- Inspired by modern data engineering best practices
- Built with guidance from dbt Labs and Snowflake documentation