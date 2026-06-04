{{ config(materialized='table') }}

select * from {{ ref('stg_raw_products') }} p
join {{ ref('stg_raw_sales') }} s
    on p.product_id = s.product_id