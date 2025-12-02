
  
    

        create or replace transient table PRODUCT_CATALOG.CORE_core.dim_product_scd2_from_snapshot
         as
        (

WITH snap AS (
  SELECT
    product_nk,
    url, title, images, description,
    product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
    dbt_valid_from AS valid_from,
    dbt_valid_to   AS valid_to
  FROM PRODUCT_CATALOG.snapshots.product_snapshot
),
dedup AS (
  SELECT DISTINCT * FROM snap
)
SELECT
  MD5( COALESCE(CAST(product_nk AS STRING), '') || COALESCE(CAST(valid_from AS STRING), '') ) AS dim_product_sk,
  product_nk, url, title, images, description,
  product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
  valid_from, valid_to,
  IFF(valid_to IS NULL, TRUE, FALSE) AS is_current,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM dedup
        );
      
  