# Common SQL Patterns in dbt Models

## 1. `select ... from ...`

This is the basic shape of almost every dbt model.

```sql
select *
from {{ ref('stg_customers') }}
```

In dbt, models are usually just `SELECT` queries that define the final dataset.

---

## 2. `WITH ... AS (...)` (CTEs)

Very common for organizing transformations cleanly.

```sql
with source_data as (

    select *
    from {{ source('raw', 'customers') }}

),

final as (

    select
        id,
        upper(name) as name
    from source_data

)

select *
from final
```

CTEs make dbt models easier to read and maintain.

---

## 3. `ref()`

This is dbt’s most important SQL helper.

```sql
select *
from {{ ref('stg_orders') }}
```

It means:

- "Use another dbt model here"
- dbt will build dependency order automatically
- dbt can track lineage

---

## 4. `source()`

Used for raw source tables.

```sql
select *
from {{ source('raw', 'orders') }}
```

This tells dbt the table comes from an external source, not another dbt model.

---

## 5. `case when`

Used for conditional logic.

```sql
select
    customer_id,
    case
        when total_amount >= 1000 then 'vip'
        when total_amount >= 500 then 'regular'
        else 'new'
    end as customer_segment
from {{ ref('stg_customers') }}
```

Very common for:

- Categorization
- Flags
- Business rules

---

## 6. `join`

Used to combine datasets.

```sql
select
    o.order_id,
    c.customer_name
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
```

Common join types in dbt models:

- `left join`
- `inner join`
- `full outer join`

---

## 7. `group by`

Used for aggregations.

```sql
select
    customer_id,
    count(*) as order_count,
    sum(order_amount) as total_spent
from {{ ref('stg_orders') }}
group by customer_id
```

This is common in mart models and reporting models.

---

## 8. Window Functions

Very common in dbt for ranking, deduplication, and analytics.

```sql
select
    *,
    row_number() over (
        partition by customer_id
        order by updated_at desc
    ) as rn
from {{ ref('stg_customers') }}
```

Useful functions:

- `row_number()`
- `rank()`
- `dense_rank()`
- `lag()`
- `lead()`
- `sum() over (...)`

---

## 9. Deduplication Pattern

A very common dbt SQL pattern.

```sql
with ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as rn
    from {{ source('raw', 'customers') }}

)

select *
from ranked
where rn = 1
```

This keeps only the latest record per key.

---

## 10. Incremental Filter Logic

Used in incremental models.

```sql
{% if is_incremental() %}

where updated_at > (
    select max(updated_at)
    from {{ this }}
)

{% endif %}
```

This tells dbt to process only new or changed rows.

---

## 11. `union all`

Used to stack datasets.

```sql
select * from {{ ref('stg_orders_us') }}

union all

select * from {{ ref('stg_orders_eu') }}
```

Very common when combining similar tables from different regions or systems.

---

## 12. `coalesce`

Used to handle nulls.

```sql
select
    customer_id,
    coalesce(phone, email, 'unknown') as contact_value
from {{ ref('stg_customers') }}
```

Useful for:

- Default values
- Fallback logic
- Cleaning data

---

## 13. `cast`

Used to convert data types.

```sql
select
    cast(order_date as date) as order_date
from {{ ref('stg_orders') }}
```

Very common when source systems store data in inconsistent formats.

---

## 14. `dbt_utils`-Style Patterns

If your project uses packages, you may also see macros like:

### `dbt_utils.star()`

```sql
{{ dbt_utils.star(from=ref('stg_customers')) }}
```

### `dbt_utils.generate_surrogate_key()`

```sql
{{ dbt_utils.generate_surrogate_key([
    'customer_id',
    'order_id'
]) }}
```

These are not plain SQL, but they are very common in dbt projects.