{{ config(materialized='table') }}

select
    s.customer_name,
    count(*) as total_orders,
    sum(s.quantity) as total_quantity_sold,
    sum(s.sale_amount) as total_sales_amount
from {{ ref('stg_raw_sales') }} s
group by s.customer_name
