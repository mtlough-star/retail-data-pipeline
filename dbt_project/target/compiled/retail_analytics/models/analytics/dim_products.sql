/*
Product Dimension Table
Contains one row per unique product with sales metrics
*/

WITH product_sales AS (
    SELECT
        stock_code,
        -- Use the most recent product description (in case it changed)
        MAX(product_description) AS product_description,
        
        -- Aggregated sales metrics
        COUNT(DISTINCT invoice_number) AS times_ordered,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(quantity) AS total_quantity_sold,
        SUM(line_total) AS total_revenue,
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,
        
        -- Date information
        MIN(invoice_date) AS first_sold_date,
        MAX(invoice_date) AS last_sold_date
        
    FROM RETAIL_DW.STAGING_staging.stg_online_retail
    GROUP BY stock_code
),

product_categories AS (
    SELECT
        *,
        -- Categorize products by revenue
        CASE 
            WHEN total_revenue >= 10000 THEN 'Top Seller'
            WHEN total_revenue >= 1000 THEN 'Popular'
            WHEN total_revenue >= 100 THEN 'Regular'
            ELSE 'Low Volume'
        END AS product_category,
        
        -- Price variance indicator
        CASE 
            WHEN max_unit_price - min_unit_price > avg_unit_price * 0.2 
            THEN 'High Variance'
            ELSE 'Stable Pricing'
        END AS price_stability,
        
        -- Calculate days product has been available
        DATEDIFF('day', first_sold_date, last_sold_date) AS days_on_market
        
    FROM product_sales
)

SELECT
    md5(cast(coalesce(cast(stock_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS product_key,
    stock_code,
    product_description,
    times_ordered,
    unique_customers,
    total_quantity_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_unit_price, 2) AS avg_unit_price,
    ROUND(min_unit_price, 2) AS min_unit_price,
    ROUND(max_unit_price, 2) AS max_unit_price,
    first_sold_date,
    last_sold_date,
    days_on_market,
    product_category,
    price_stability,
    CURRENT_TIMESTAMP() AS dw_created_at,
    CURRENT_TIMESTAMP() AS dw_updated_at
FROM product_categories