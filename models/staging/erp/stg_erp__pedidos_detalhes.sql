with 

source as (

    select * from {{ source('erp', 'SalesOrderDetail') }}

),

pedidos_detalhes as (

    select
        cast(salesorderid as int) as fk_pedido
        , cast(salesorderdetailid as int) as pk_pedido_detalhes
        , cast(carriertrackingnumber as varchar) as numero_rastreamento
        , cast(orderqty as int) as quantidade_pedido
        , cast(productid as varchar) as fk_produto
        , cast(specialofferid as varchar) as fk_oferta_especial
        , cast(unitprice as numeric(18,2)) as preco_unitario
        , cast(unitpricediscount as numeric(18,2)) as desconto_preco_unitario
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select *
from pedidos_detalhes