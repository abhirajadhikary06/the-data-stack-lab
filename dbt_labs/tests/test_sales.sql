select
    s.sale_id,
    s.quantity,
    s.sale_amount,
    s.payment_method
from {{ ref('stg_raw_sales') }} s
where
    s.sale_id is null
    or s.quantity <= 0
    or s.sale_amount <= 0
    or s.payment_method not in ('Card', 'UPI', 'NetBanking')
