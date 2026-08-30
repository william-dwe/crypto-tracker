{#
    Multiplying two DECIMAL(38,18) values overflows: DuckDB wants precision
    p1+p2 and scale s1+s2 for a product, i.e. DECIMAL(76,36), which exceeds the
    38-digit maximum and raises

        Out of Range Error: Overflow in multiplication of DECIMAL(38)

    Every monetary multiplication therefore casts its operands down to scales
    whose sum still fits in 38 digits. 6 decimal places of currency and 8 of FX
    rate are far beyond the source data's own precision (CoinGecko quotes
    fractional cents; ECB publishes 5 significant digits), so nothing real is
    lost.
#}

{# Currency amount: up to 18 integer digits, 6 fractional. #}
{% macro money(expression) -%}
    cast({{ expression }} as decimal(24, 6))
{%- endmacro %}

{# FX rate: up to 6 integer digits (IDR ~17,712 is the largest tracked), 8 fractional. #}
{% macro fx_rate(expression) -%}
    cast({{ expression }} as decimal(14, 8))
{%- endmacro %}

{# Convert a USD amount to a local currency. Result is DECIMAL(38,14). #}
{% macro to_local(usd_expression, rate_expression) -%}
    {{ money(usd_expression) }} * {{ fx_rate(rate_expression) }}
{%- endmacro %}

{# Asset quantity: up to 12 integer digits, 8 fractional (satoshi-level and finer). #}
{% macro quantity(expression) -%}
    cast({{ expression }} as decimal(20, 8))
{%- endmacro %}

{#
   Value a holding: quantity * unit price. Result is DECIMAL(38,14).
   The price cast keeps 12 integer digits, so it comfortably covers bitcoin-scale
   unit prices rather than clipping them.
#}
{% macro position_value(quantity_expression, price_expression) -%}
    {{ quantity(quantity_expression) }} * cast({{ price_expression }} as decimal(18, 6))
{%- endmacro %}
