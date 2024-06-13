with 

source as (

    select * from {{ source('erp', 'Customer') }}

),

clientes as (

    select
        cast(customerid as int) as pk_cliente
        , cast(personid as varchar) as fk_pessoa
        , cast(storeid as varchar) as fk_loja
        , cast(territoryid as varchar) as fk_territorio
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from clientes
