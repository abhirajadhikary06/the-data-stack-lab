with customers as(
    SELECT
        1 AS customer_id,
        'James Wilson' AS customer_name,
        'james.wilson@example.com' AS email,
        'New York' AS city,
        CAST('2023-01-12 08:30:00' AS TIMESTAMP) AS created_at
    UNION ALL
    SELECT 2, 'Sophia Martinez', 'sophia.mtz@provider.net', 'Austin', CAST('2023-01-15 10:15:00' AS TIMESTAMP)
    UNION ALL
    SELECT 3, 'Michael Chen', 'mchen99@techcorp.com', 'San Francisco', CAST('2023-02-01 14:20:00' AS TIMESTAMP)
    UNION ALL
    SELECT 4, 'Emma Thompson', 'emma.t@freemail.org', 'Chicago', CAST('2023-02-10 09:45:00' AS TIMESTAMP)
    UNION ALL
    SELECT 5, 'Liam O’Connor', 'liam.oconnor@bizmail.com', 'Boston', CAST('2023-03-05 11:10:00' AS TIMESTAMP)
    UNION ALL
    SELECT 6, 'Olivia Garcia', 'olivia.g@webmail.com', 'Miami', CAST('2023-03-22 16:05:00' AS TIMESTAMP)
    UNION ALL
    SELECT 7, 'William Brown', 'wbrown_data@outlook.com', 'Seattle', CAST('2023-04-02 13:30:00' AS TIMESTAMP)
    UNION ALL
    SELECT 8, 'Isabella Rossi', 'i.rossi@service.it', 'Denver', CAST('2023-04-18 08:50:00' AS TIMESTAMP)
    UNION ALL
    SELECT 9, 'Ethan Wright', 'ewright@startup.io', 'Portland', CAST('2023-05-05 17:40:00' AS TIMESTAMP)
    UNION ALL
    SELECT 10, 'Ava Johnson', 'ava.j@global.com', 'Atlanta', CAST('2023-05-12 12:25:00' AS TIMESTAMP)
    UNION ALL
    SELECT 11, 'Lucas Silva', 'lsilva@brasil.br', 'Los Angeles', CAST('2023-06-01 10:00:00' AS TIMESTAMP)
    UNION ALL
    SELECT 12, 'Mia Tanaka', 'm.tanaka@japan-tech.jp', 'San Diego', CAST('2023-06-14 15:15:00' AS TIMESTAMP)
    UNION ALL
    SELECT 13, 'Noah Smith', 'nsmith82@provider.net', 'Dallas', CAST('2023-07-04 09:20:00' AS TIMESTAMP)
    UNION ALL
    SELECT 14, 'Charlotte White', 'c.white@agency.com', 'Philadelphia', CAST('2023-07-19 11:55:00' AS TIMESTAMP)
    UNION ALL
    SELECT 15, 'Benjamin Lee', 'blee_dev@gmail.com', 'Salt Lake City', CAST('2023-08-01 14:40:00' AS TIMESTAMP)
    UNION ALL
    SELECT 16, 'Amelia Jones', 'ajones@lifestyle.com', 'Nashville', CAST('2023-08-15 08:10:00' AS TIMESTAMP)
    UNION ALL
    SELECT 17, 'Alexander Kim', 'akim.consulting@mail.com', 'Phoenix', CAST('2023-09-02 13:05:00' AS TIMESTAMP)
    UNION ALL
    SELECT 18, 'Harper Davis', 'harper.d@edu.org', 'Minneapolis', CAST('2023-09-20 16:50:00' AS TIMESTAMP)
    UNION ALL
    SELECT 19, 'Sebastian Vogt', 's.vogt@berlin.de', 'Charlotte', CAST('2023-10-05 10:30:00' AS TIMESTAMP)
    UNION ALL
    SELECT 20, 'Chloe Miller', 'chloe.m@creatives.net', 'Las Vegas', CAST('2023-10-25 12:00:00' AS TIMESTAMP)
)

select customer_id, customer_name, email, city, created_at,
    cast(created_at as date) as created_date,
    cast(created_at as time) as created_time
from customers