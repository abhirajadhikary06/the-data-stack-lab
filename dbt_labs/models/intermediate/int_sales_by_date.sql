{{ config(materialized='incremental') }}

select
    s.sale_date,
    count(*) as total_orders,
    sum(s.quantity) as total_quantity_sold,
    sum(s.sale_amount) as total_sales_amount
from {{ ref('stg_raw_sales') }} s
{% if is_incremental() %}
    where s.sale_date > (select max(sale_date) from {{ this }})
{% endif %}
group by s.sale_date
