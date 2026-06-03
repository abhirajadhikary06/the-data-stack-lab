{{ config(materialized='view') }}

{# Group orders by city and order status #}

select
    c.city,
    o.order_status,
    sum(o.amount) as total_amount,
    count(o.order_id) as total_orders
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
group by c.city, o.order_status