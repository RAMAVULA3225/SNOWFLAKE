/*{{

config(
materialized='incremental'
)

}}

select * from {{ source('public','ORDERS_STAGE')}}

{% If is_incremental() %}
where UPDATED_AT >= (select max(UPDATED_AT) from {{this}})
{% endif %}
*/
{{

config(
    materialized='incremental'
)
}}
SELECT 
ORDERID,
ORDERDATE,
CUSTOMERID,
EMPLOYEEID,
STOREID,
STATUS AS STATUSCD,
CASE 
   WHEN STATUS=01 THEN 'INPROGRESS'
   WHEN STATUS=02 THEN 'COMPLETED'
   WHEN STATUS=03 THEN 'CANCELLED'
   ELSE NULL END AS STATUSDesc,
CASE 
   WHEN STOREID=1000 THEN 'ONLINE'
   ELSE 'IN-STORE'
   END AS ORDER_CHANNEL,
   UPDATED_AT,
   CURRENT_TIMESTAMP AS DBT_UPDATED_AT
   FROM {{ source('public','ORDERS_STAGE')}}

{% If is_incremental() %}
where UPDATED_AT >= (select max(UPDATED_AT) from {{this}})
{% endif %}








