select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dim_product_sk
from PRODUCT_CATALOG.CORE_core.dim_product_scd2_window
where dim_product_sk is null



      
    ) dbt_internal_test