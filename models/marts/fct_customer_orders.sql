with Orders as (
    select * from {{ source("jaffle_shop", "orders") }}
),

Payments as (
    select * from {{ source("jaffle_shop", "stripe_payments") }}
),

Customers as (
    select * from {{ source("jaffle_shop", "customers") }}
),

finalized_orders as (
    select ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid
        from Payments
        where STATUS <> 'fail'
        group by 1
),

paid_orders as (select Orders.ID as order_id,
    Orders.USER_ID	as customer_id,
    Orders.ORDER_DATE AS order_placed_at,
        Orders.STATUS AS order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.FIRST_NAME    as customer_first_name,
        C.LAST_NAME as customer_last_name
FROM Orders
left join finalized_orders as p ON orders.ID = p.order_id
left join  Customers as C on orders.USER_ID = C.ID ),

customer_orders 
as (select C.ID as customer_id
    , min(ORDER_DATE) as first_order_date
    , max(ORDER_DATE) as most_recent_order_date
    , count(ORDERS.ID) AS number_of_orders
from Customers C 
left join Orders
on orders.USER_ID = C.ID 
group by 1)



select
p.*,
ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
CASE WHEN c.first_order_date = p.order_placed_at
THEN 'new'
ELSE 'return' END as nvsr,
sum(p.total_amount_paid) OVER (PARTITION BY p.customer_id order by p.order_id) as customer_lifetime_value,
c.first_order_date as fdos
FROM paid_orders p
left join customer_orders as c USING (customer_id)
ORDER BY order_id