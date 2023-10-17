{{

config(
    materialized='incremental'
)

}}

select * from {{ source('public','ORDERS_STAGE')}}

{% If is_incremental() %}
where UPDATED_AT >= (select max(UPDATED_AT) from {{this}})
{% endif %}