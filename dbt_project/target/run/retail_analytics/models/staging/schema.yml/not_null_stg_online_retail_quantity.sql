
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantity
from RETAIL_DW.STAGING_staging.stg_online_retail
where quantity is null



  
  
      
    ) dbt_internal_test