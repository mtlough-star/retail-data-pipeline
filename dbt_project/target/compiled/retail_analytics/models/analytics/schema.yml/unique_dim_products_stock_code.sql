
    
    

select
    stock_code as unique_field,
    count(*) as n_records

from RETAIL_DW.STAGING_analytics.dim_products
where stock_code is not null
group by stock_code
having count(*) > 1


