with orders as (
    select
        101 as order_id,
        1 as customer_id,
        500 as amount,
        'completed' as order_status,
        cast('2024-01-05 09:15:00' as timestamp) as order_created_at
    union all
    select 102, 2, 800, 'completed', cast('2024-01-06 11:20:00' as timestamp)
    union all
    select 103, 3, 250, 'pending', cast('2024-01-07 14:05:00' as timestamp)
    union all
    select 104, 1, 1200, 'completed', cast('2024-01-08 16:45:00' as timestamp)
    union all
    select 105, 4, 300, 'cancelled', cast('2024-01-09 10:30:00' as timestamp)
    union all
    select 106, 5, 950, 'completed', cast('2024-01-10 13:10:00' as timestamp)
    union all
    select 107, 6, 150, 'pending', cast('2024-01-11 08:55:00' as timestamp)
    union all
    select 108, 7, 700, 'completed', cast('2024-01-12 12:40:00' as timestamp)
    union all
    select 109, 8, 430, 'completed', cast('2024-01-13 15:25:00' as timestamp)
    union all
    select 110, 9, 1100, 'completed', cast('2024-01-14 09:05:00' as timestamp)
    union all
    select 111, 10, 275, 'pending', cast('2024-01-15 17:15:00' as timestamp)
    union all
    select 112, 11, 640, 'completed', cast('2024-01-16 10:50:00' as timestamp)
    union all
    select 113, 12, 980, 'completed', cast('2024-01-17 14:35:00' as timestamp)
    union all
    select 114, 13, 220, 'cancelled', cast('2024-01-18 11:00:00' as timestamp)
    union all
    select 115, 14, 1500, 'completed', cast('2024-01-19 16:20:00' as timestamp)
    union all
    select 116, 15, 390, 'pending', cast('2024-01-20 09:40:00' as timestamp)
    union all
    select 117, 16, 760, 'completed', cast('2024-01-21 13:55:00' as timestamp)
    union all
    select 118, 17, 540, 'completed', cast('2024-01-22 08:25:00' as timestamp)
    union all
    select 119, 18, 880, 'pending', cast('2024-01-23 12:10:00' as timestamp)
    union all
    select 120, 19, 610, 'completed', cast('2024-01-24 15:45:00' as timestamp)
)

select
    order_id,
    customer_id,
    amount,
    order_status,
    order_created_at,
    cast(order_created_at as date) as order_created_date,
    cast(order_created_at as time) as order_created_time
from orders