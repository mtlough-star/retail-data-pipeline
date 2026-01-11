/*
Customer Dimension Table
Contains one row per unique customer with aggregated metrics
This is a Type 1 SCD (Slowly Changing Dimension)
*/

WITH customer_orders AS (
    SELECT
        customer_id,
        MAX(country) AS country,
        MIN(invoice_date) AS first_order_date,
        MAX(invoice_date) AS last_order_date,
        COUNT(DISTINCT invoice_number) AS total_orders,
        COUNT(*) AS total_line_items,
        SUM(quantity) AS total_items_purchased,
        SUM(line_total) AS total_revenue,
        AVG(line_total) AS avg_line_value,
        MAX(line_total) AS max_line_value
    FROM {{ ref('stg_online_retail') }}
    GROUP BY customer_id
),

customer_segments AS (
    SELECT
        *,
        -- Customer lifetime value segment
        CASE 
            WHEN total_revenue >= 5000 THEN 'High Value'
            WHEN total_revenue >= 1000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment,
        
        -- Order frequency segment
        CASE 
            WHEN total_orders >= 20 THEN 'Frequent'
            WHEN total_orders >= 5 THEN 'Regular'
            ELSE 'Occasional'
        END AS frequency_segment,
        
        -- Calculate days between first and last order
        DATEDIFF('day', first_order_date, last_order_date) AS customer_lifetime_days,
        
        -- Calculate average days between orders
        CASE 
            WHEN total_orders > 1 
            THEN DATEDIFF('day', first_order_date, last_order_date) / (total_orders - 1)
            ELSE NULL
        END AS avg_days_between_orders
        
    FROM customer_orders
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    country,
    first_order_date,
    last_order_date,
    customer_lifetime_days,
    total_orders,
    total_line_items,
    total_items_purchased,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_line_value, 2) AS avg_line_value,
    ROUND(max_line_value, 2) AS max_line_value,
    ROUND(avg_days_between_orders, 1) AS avg_days_between_orders,
    customer_segment,
    frequency_segment,
    CURRENT_TIMESTAMP() AS dw_created_at,
    CURRENT_TIMESTAMP() AS dw_updated_at
FROM customer_segments