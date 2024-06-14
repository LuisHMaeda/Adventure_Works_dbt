with 

source as (

    select * from {{ source('erp', 'SalesOrderDetail') }}

),

pedidos_detalhes as (

    select
        cast(salesorderid as varchar) as fk_pedido
        , cast(salesorderdetailid as varchar) as pk_pedido_detalhes
        , cast(carriertrackingnumber as varchar) as numero_rastreamento
        , cast(orderqty as varchar) as quantidade_pedido
        , cast(productid as varchar) as fk_produto
        , cast(specialofferid as varchar) as fk_oferta_especial
        , cast(unitprice as varchar) as preco_unitario
        , cast(unitpricediscount as varchar) as desconto_preco_unitario
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from pedidos_detalhes
