with orders as (
    select * from {{ source("jaffle_shop", "orders") }}
),

payments as (
    select * from {{ source("jaffle_shop", "stripe_payments") }}
),

customers as (
    select * from {{ source("jaffle_shop", "customers") }}
),

finalized_orders as (
    select 
        orderid as order_id, max(created) as payment_finalized_date, sum(amount) / 100.0 as total_amount_paid
    from Payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.id as order_id,
        orders.user_id	as customer_id,
        orders.order_date AS order_placed_at,
        orders.status AS order_status,

        finalized_orders.total_amount_paid,
        finalized_orders.payment_finalized_date,

        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join finalized_orders on orders.id = finalized_orders.order_id
    left join customers on orders.user_id = customers.id
),

customer_orders 
as (select C.ID as customer_id
    , min(ORDER_DATE) as first_order_date
    , max(ORDER_DATE) as most_recent_order_date
    , count(ORDERS.ID) AS number_of_orders
from customers C 
left join orders
on orders.USER_ID = C.ID 
group by 1)



select
    paid_orders.*,
    ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = paid_orders.order_placed_at
    THEN 'new'
    ELSE 'return' END as nvsr,
    sum(paid_orders.total_amount_paid) OVER (PARTITION BY paid_orders.customer_id order by paid_orders.order_id) as customer_lifetime_value,
    c.first_order_date as fdos
FROM paid_orders
left join customer_orders as c USING (customer_id)
ORDER BY order_id