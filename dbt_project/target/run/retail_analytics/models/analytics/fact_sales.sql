
  
    

create or replace transient table RETAIL_DW.STAGING_analytics.fact_sales
    
    
    
    as (/*
Sales Fact Table
Grain: One row per invoice line item
Links to customer and product dimensions
*/

WITH sales_with_row_numbers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_number, stock_code) AS row_num
    FROM RETAIL_DW.STAGING_staging.stg_online_retail
),

sales_with_keys AS (
    SELECT
        -- Generate surrogate key for fact table including row number for uniqueness
        md5(cast(coalesce(cast(invoice_number as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(stock_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(invoice_date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(row_num as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS sales_key,
        
        -- Foreign keys to dimensions
        md5(cast(coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS customer_key,
        md5(cast(coalesce(cast(stock_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS product_key,
        
        -- Degenerate dimensions (attributes that don't belong in dimension tables)
        invoice_number,
        
        -- Metrics (measures)
        quantity,
        unit_price,
        line_total,
        
        -- Date dimension attributes
        invoice_date,
        invoice_date_day,
        invoice_month,
        invoice_year,
        DAYNAME(invoice_date) AS day_of_week,
        DAYOFWEEK(invoice_date) AS day_of_week_number,
        QUARTER(invoice_date) AS quarter,
        
        -- Geographic dimension
        country,
        
        -- Metadata
        loaded_at
        
    FROM sales_with_row_numbers
)

SELECT
    sales_key,
    customer_key,
    product_key,
    invoice_number,
    quantity,
    ROUND(unit_price, 2) AS unit_price,
    ROUND(line_total, 2) AS line_total,
    invoice_date,
    invoice_date_day,
    invoice_month,
    invoice_year,
    day_of_week,
    day_of_week_number,
    quarter,
    country,
    loaded_at,
    CURRENT_TIMESTAMP() AS dw_created_at
FROM sales_with_keys
    )
;


  