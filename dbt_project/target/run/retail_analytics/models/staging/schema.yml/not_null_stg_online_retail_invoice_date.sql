
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select invoice_date
from RETAIL_DW.STAGING_staging.stg_online_retail
where invoice_date is null



  
  
      
    ) dbt_internal_test