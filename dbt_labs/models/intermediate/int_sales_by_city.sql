{{ config(materialized='table') }}

select
    s.city,
    p.product_id,
    p.product_name,
    count(*) as total_sales,
    sum(s.sale_amount) as total_sales_amount,
    sum(s.quantity) as total_quantity_sold
from {{ ref('stg_raw_products') }} p
join {{ ref('stg_raw_sales') }} s
    on p.product_id = s.product_id
group by 
    s.city,
    p.product_id,
    p.product_name