{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

with payments as (
    select *
    from {{ ref('stg_stripe__payments') }}
),

final as (
    select order_id,
    {% for payment_var in payment_methods -%} 
    sum(case when payment_method = '{{ payment_var }}' then payment_amount else 0 end) as {{ payment_var }}_amount
    {%- if not loop.last -%} , {% endif %}
    {% endfor -%}
    from payments group by 1
)
    
select * from final