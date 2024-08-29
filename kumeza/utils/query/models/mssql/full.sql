SELECT 
    {% if aggfunc|length != 0 -%}
        {% if column|length != 0 -%}
            {{aggfunc}}({{column}}) 
            {% if alias|length != 0 -%}
                as '{{alias}}'
            {%- endif %}
        {%- endif %}
    {% else -%}
        {% if column|length != 0 -%}
            {% if rowlimit|length != 0 -%}
                TOP {{rowlimit}} {{column}}
            {% else -%}
                {{column}}
            {%- endif %}
        {% else -%}
            {% if rowlimit|length != 0 -%}
                TOP {{rowlimit}} *
            {% else -%}
                *
            {%- endif %}
        {%- endif %}
    {%- endif %}
FROM {{source}} 
{% if filter == true %}
    WHERE (
    {% if custom_filter|length != 0 -%}
        ({{custom_filter}})
    AND
    {%- endif %}
    {% if start_time|length != 0 -%} 
        ({{datecol}} >= '{{start_time}}'
    {%- endif %} 
    {% if end_time|length != 0 -%} 
        AND 
        {{datecol}} <= '{{end_time}}')
    {% else -%}
        )
    {%- endif %}
    )
{%- endif %}
{% if groupby == true -%}
GROUP BY 
    {% if aggtime|length != 0 -%}
    time({{aggtime}}) 
        {% if aggcol|length != 0 -%}
        , {{aggcol}}
        {%- endif %}
    {%- endif %}
{%- endif %}