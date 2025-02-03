with source as (
    select * from {{ source("jaffle_shop", "customers") }}
)

select 
    id as customer_id,
    last_name as customer_last_name,
    first_name as customer_first_name
from source
