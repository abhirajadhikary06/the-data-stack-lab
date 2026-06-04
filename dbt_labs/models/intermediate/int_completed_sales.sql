{{ config(materialized='table') }}

select * from {{ ref('stg_raw_sales') }} s
join {{ ref('stg_raw_products') }} p
    on s.product_id = p.product_id
where {{ status('s.sale_status') }} = 'Processing Closed'