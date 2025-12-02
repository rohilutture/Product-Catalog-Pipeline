
  create or replace   view PRODUCT_CATALOG.CORE_staging.stg_products
  
   as (
    

WITH base AS (
  SELECT
      TRY_TO_NUMBER(index)                                        AS row_idx,
      url,
      title,
      images,
      description,
      NULLIF(product_id, '')                                       AS product_id,
      NULLIF(sku, '')                                              AS sku,
      NULLIF(gtin13, '')                                           AS gtin13,
      NULLIF(brand, '')                                            AS brand,
      TRY_TO_DECIMAL(REGEXP_REPLACE(price, '[^0-9\.]', ''), 18, 2) AS price,
      UPPER(NULLIF(currency, ''))                                   AS currency,
      LOWER(NULLIF(availability, ''))                               AS availability,
      NULLIF(uniq_id, '')                                          AS uniq_id,
      -- make it work whether column is text or timestamp
      TRY_TO_TIMESTAMP_NTZ(scraped_at::STRING)                      AS scraped_at
  FROM product_catalog.staging.raw_products
),
clean AS (
  SELECT
    row_idx,
    url,
    title,
    images,
    description,
    COALESCE(product_id, sku, uniq_id)        AS product_nk,
    product_id,
    sku,
    gtin13,
    brand,
    price,
    COALESCE(currency, 'USD')                 AS currency,
    availability,
    uniq_id,
    COALESCE(scraped_at, CURRENT_TIMESTAMP()) AS updated_at,
    CURRENT_TIMESTAMP()                       AS loaded_at
  FROM base
)
SELECT * FROM clean
  );

