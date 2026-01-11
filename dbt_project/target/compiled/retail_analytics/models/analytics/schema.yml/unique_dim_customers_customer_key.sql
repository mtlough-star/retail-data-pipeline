
    
    

select
    customer_key as unique_field,
    count(*) as n_records

from RETAIL_DW.STAGING_analytics.dim_customers
where customer_key is not null
group by customer_key
having count(*) > 1


