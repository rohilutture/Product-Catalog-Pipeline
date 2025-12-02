
{{ config(materialized='table') }}

WITH snap AS (
  SELECT
    product_nk,
    url, title, images, description,
    product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
    dbt_valid_from AS valid_from,
    dbt_valid_to   AS valid_to
  FROM {{ ref('product_snapshot') }}
),
dedup AS (
  SELECT DISTINCT * FROM snap
)
SELECT
  {{ surrogate_key(['product_nk', 'valid_from']) }} AS dim_product_sk,
  product_nk, url, title, images, description,
  product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
  valid_from, valid_to,
  IFF(valid_to IS NULL, TRUE, FALSE) AS is_current,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM dedup
