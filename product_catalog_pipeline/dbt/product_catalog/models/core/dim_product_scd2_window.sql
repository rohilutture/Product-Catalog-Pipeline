{{ config(materialized='table') }}

WITH src AS (
  SELECT
    row_idx,
    product_nk,
    url,
    title,
    images,
    description,
    product_id,
    sku,
    gtin13,
    brand,
    price,
    currency,
    availability,
    uniq_id,
    updated_at
  FROM {{ ref('stg_products') }}
),

-- keep one row per (product_nk, updated_at)
dedup AS (
  SELECT *
  FROM src
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_nk, updated_at
    ORDER BY row_idx
  ) = 1
),

-- build SCD2 boundaries with window functions
ordered AS (
  SELECT
    product_nk,
    url, title, images, description,
    product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
    updated_at,
    LAG(updated_at)  OVER (PARTITION BY product_nk ORDER BY updated_at) AS valid_from,
    LEAD(updated_at) OVER (PARTITION BY product_nk ORDER BY updated_at) AS valid_to
  FROM dedup
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['product_nk','valid_from']) }} AS dim_product_sk,
    product_nk,
    url, title, images, description,
    product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
    COALESCE(valid_from, updated_at) AS valid_from,
    valid_to,
    IFF(valid_to IS NULL, TRUE, FALSE) AS is_current,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
  FROM ordered
)

SELECT * FROM final



