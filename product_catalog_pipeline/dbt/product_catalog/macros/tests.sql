
-- Ensure exactly one current record per product_nk
{% test single_current_per_key(model, key) %}
with x as (
  select {{ key }} as k, sum(case when is_current then 1 else 0 end) as cnt
  from {{ model }}
  group by 1
)
select * from x where cnt != 1
{% endtest %}

-- Ensure no overlapping validity windows for each product_nk
{% test no_overlaps(model, key, valid_from, valid_to) %}
with ordered as (
  select
    {{ key }} as k,
    {{ valid_from }} as vf,
    {{ valid_to }} as vt
  from {{ model }}
),
violations as (
  select a.k, a.vf, a.vt, b.vf as other_vf, b.vt as other_vt
  from ordered a
  join ordered b
    on a.k = b.k
   and a.vf < coalesce(b.vt, '9999-12-31'::timestamp_ntz)
   and coalesce(a.vt, '9999-12-31'::timestamp_ntz) > b.vf
   and (a.vf, coalesce(a.vt, '9999-12-31'::timestamp_ntz)) != (b.vf, coalesce(b.vt, '9999-12-31'::timestamp_ntz))
)
select * from violations
{% endtest %}
