{% macro status(sale_status) %}
    case
        when {{ sale_status }} = 'completed' then 'Processing Closed'
        when {{ sale_status }} = 'pending' then 'Processing Opened'
        when {{ sale_status }} = 'canceled' then 'Processing Aborted'
        else 'Failed'
    end
{% endmacro %}