{{ config(materialized='ephemeral') }}

select * from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c
on o.customer_id = c.customer_id
where o.order_status = 'completed'