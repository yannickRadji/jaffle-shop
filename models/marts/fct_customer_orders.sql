with orders as (
    select * from {{ ref("stg_jaffle_shop__orders") }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

customers as (
    select * from {{ ref("stg_jaffle_shop__customers") }}
),

finalized_orders as (
    select 
        order_id, max(payment_created_at) as payment_finalized_date, sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,

        finalized_orders.total_amount_paid,
        finalized_orders.payment_finalized_date,

        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join finalized_orders on orders.order_id = finalized_orders.order_id
    left join customers on orders.customer_id = customers.customer_id
),

final as (
    select
        paid_orders.*,

        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,

        min(paid_orders.order_placed_at) OVER (PARTITION by customer_id ) as first_order_date,
        CASE WHEN first_order_date = paid_orders.order_placed_at
        THEN 'new'
        ELSE 'return' END as nvsr,
        first_order_date as fdos,
        
        sum(paid_orders.total_amount_paid) OVER (PARTITION BY paid_orders.customer_id order by paid_orders.order_id) as customer_lifetime_value
        
    FROM paid_orders
)

select * from final
ORDER BY order_id