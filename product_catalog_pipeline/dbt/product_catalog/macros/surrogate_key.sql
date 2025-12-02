
{% macro surrogate_key(columns) -%}
    {%- set expr = [] -%}
    {%- for col in columns -%}
        {%- do expr.append("COALESCE(CAST(" ~ col ~ " AS STRING), '')") -%}
    {%- endfor -%}
    MD5( {{ ' || ' . join(expr) }} )
{%- endmacro %}
