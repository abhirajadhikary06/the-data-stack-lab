{{ config(materialized='table') }}

select 
    p.product_id,
    p.product_name,
    p.category,
    sum(s.quantity) as total_quantity_sold,
    sum(s.sale_amount) as total_sales_amount
from {{ ref('stg_raw_products') }} p
join {{ ref('stg_raw_sales') }} s
    on p.product_id = s.product_id
group by 
    p.product_id,
    p.product_name,
    p.category,
    p.brand