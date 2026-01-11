/*
Staging model for Online Retail data
This model cleans and standardizes raw data:
- Filters out cancelled orders (InvoiceNo starting with 'C')
- Removes invalid quantities and prices
- Calculates line total
- Standardizes data types
*/

WITH source AS (
    SELECT *
    FROM RETAIL_DW.RAW.online_retail
),

cleaned AS (
    SELECT
        -- Primary identifiers
        InvoiceNo AS invoice_number,
        StockCode AS stock_code,
        CustomerID AS customer_id,
        
        -- Product information
        TRIM(Description) AS product_description,
        
        -- Transaction details
        Quantity AS quantity,
        UnitPrice AS unit_price,
        
        -- Calculate line total (quantity * unit price)
        Quantity * UnitPrice AS line_total,
        
        -- Date information
        InvoiceDate AS invoice_date,
        DATE_TRUNC('day', InvoiceDate) AS invoice_date_day,
        DATE_TRUNC('month', InvoiceDate) AS invoice_month,
        DATE_TRUNC('year', InvoiceDate) AS invoice_year,
        
        -- Geographic information
        UPPER(TRIM(Country)) AS country,
        
        -- Metadata
        loaded_at
        
    FROM source
    
    WHERE 1=1
        -- Filter out cancelled orders (invoices starting with 'C')
        AND LEFT(InvoiceNo, 1) != 'C'
        
        -- Filter out invalid quantities (must be positive)
        AND Quantity > 0
        
        -- Filter out invalid prices (must be positive)
        AND UnitPrice > 0
        
        -- Filter out records with unknown customers for customer analysis
        AND CustomerID != 'UNKNOWN'
        
        -- Filter out invalid dates
        AND InvoiceDate IS NOT NULL
)

SELECT * FROM cleaned