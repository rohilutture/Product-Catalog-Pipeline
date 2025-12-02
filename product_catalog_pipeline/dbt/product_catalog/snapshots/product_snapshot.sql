
{% snapshot product_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='product_nk',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}
select
  product_nk,
  url, title, images, description,
  product_id, sku, gtin13, brand, price, currency, availability, uniq_id,
  updated_at
from {{ ref('stg_products') }}
{% endsnapshot %}
