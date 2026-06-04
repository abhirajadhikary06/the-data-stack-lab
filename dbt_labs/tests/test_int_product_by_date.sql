select
    p.product_id,
    p.category,
    p.created_at,
    p.product_name
from {{ ref('int_product_by_date') }} p
where p.created_at is null
   or p.created_at <= '2024-01-01'
