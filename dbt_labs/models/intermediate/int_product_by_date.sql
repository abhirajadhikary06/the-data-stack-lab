{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_id'
) }}

select
    p.product_id,
    p.product_name,
    p.category,
    p.created_at
from {{ ref('stg_raw_products') }} p

{% if is_incremental() %}
where p.product_id is not null
  and p.created_at > (select max(created_at) from {{ this }})
{% endif %}
