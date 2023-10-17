{{

config(
    materialized='incremental'
)

}}

select * from {{ source('OUR_FIRST_DB','public','MY_SALES')}}

{% If is_incremental() %}
where DATE_UPDATE >= (select max(DATE_UPDATE) from {{this}})
{% endif %}