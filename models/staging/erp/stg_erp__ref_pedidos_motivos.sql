with 

source as (

    select * from {{ source('erp', 'Salesorderheadersalesreason') }}

),

ref_pedidos_motivos as (

    select
        cast(salesorderid as int) as fk_pedido
        ,cast(salesreasonid as int) as fk_motivo_venda
        ,cast(modifieddate as date) as data_modificacao

    from source

)

select * from ref_pedidos_motivos
