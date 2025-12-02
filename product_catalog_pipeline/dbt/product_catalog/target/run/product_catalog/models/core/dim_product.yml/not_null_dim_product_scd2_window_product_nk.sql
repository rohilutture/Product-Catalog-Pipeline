select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_nk
from PRODUCT_CATALOG.CORE_core.dim_product_scd2_window
where product_nk is null



      
    ) dbt_internal_test