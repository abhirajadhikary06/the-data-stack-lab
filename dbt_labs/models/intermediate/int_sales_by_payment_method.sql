{{ config(materialized='table') }}

select
    {{ payment_types('s.payment_method') }} as payment_type,
    count(*) as total_orders,
    sum(s.quantity) as total_quantity_sold,
    sum(s.sale_amount) as total_sales_amount,
    d.description as payment_method_description
from {{ ref('stg_raw_sales') }} s
left join {{ ref('payment_method_desc') }} d
    on s.payment_method = d.payment_method
group by 1, 5