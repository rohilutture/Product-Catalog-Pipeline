





with validation_errors as (

    select
        product_nk, valid_from
    from PRODUCT_CATALOG.CORE_core.dim_product_scd2_from_snapshot
    group by product_nk, valid_from
    having count(*) > 1

)

select *
from validation_errors


