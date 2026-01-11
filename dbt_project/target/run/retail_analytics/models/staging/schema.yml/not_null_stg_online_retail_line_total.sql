
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_total
from RETAIL_DW.STAGING_staging.stg_online_retail
where line_total is null



  
  
      
    ) dbt_internal_test