SELECT * 
FROM {{source}} 
WHERE
{% for col in incremental_cols -%}
    {% if not loop.last -%}
        ({{col}} >= '{{start_time}}' AND {{col}} <= '{{end_time}}') OR
    {% else -%}
        ({{col}} >= '{{start_time}}' AND {{col}} <= '{{end_time}}')
    {% endif -%}
{%- endfor %}