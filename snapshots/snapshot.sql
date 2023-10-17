{% snapshot ORDERS_STAGE %}

{{
     config( 
        target_schema='DBT_RAVULA',
        strategy='timestamp',
        updated_at='STOREID '
     )
}}

select * from {{ source('PUBLIC','ORDERS_STAGE')}}

{% endsnapshot %}