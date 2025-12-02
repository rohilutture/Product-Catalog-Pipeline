select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    dim_product_sk as unique_field,
    count(*) as n_records

from PRODUCT_CATALOG.CORE_core.dim_product_scd2_window
where dim_product_sk is not null
group by dim_product_sk
having count(*) > 1



      
    ) dbt_internal_test