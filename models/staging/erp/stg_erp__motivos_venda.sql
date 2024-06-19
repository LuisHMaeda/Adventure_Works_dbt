with 

source as (

    select * from {{ source('erp', 'SalesReason') }}

),

motivos as (

    select
        cast(salesreasonid as int) as pk_motivo_venda 
        , cast(name as varchar) as nm_motivo_venda
        , cast(reasontype as varchar) as tipo_motivo
        , cast(MODIFIEDDATE as date) as  data_modificacao

    from source

)

select * from motivos
