select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select is_current
from PRODUCT_CATALOG.CORE_core.dim_product_scd2_from_snapshot
where is_current is null



      
    ) dbt_internal_test