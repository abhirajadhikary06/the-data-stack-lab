select
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.is_active
from {{ ref('stg_raw_products') }} p
where
    p.product_id is null
    or p.product_name is null
    or p.category is null
    or p.brand is null
    or p.is_active is null
