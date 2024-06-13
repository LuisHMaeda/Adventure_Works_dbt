with 

source as (

    select * from {{ source('erp', 'SalesReason') }}

),

motivos as (

    select
        cast(salesreasonid as int) as pk_motivo_venda 
        , cast(name as varchar) as nm_motivo_venda
        , cast(reasontype as varchar) as tipo_motivo
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from motivos
