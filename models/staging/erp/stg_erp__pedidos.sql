with 

source as (

    select * from {{ source('erp', 'SalesOrderHeader') }}

),

renamed as (

    select
        cast(salesorderid as int) as pk_pedido
        , cast(revisionnumber as int) as numero_revisao
        , TO_CHAR(cast(orderdate as date)::DATE,'DD/MM/YYYY') as  data_pedido
        , TO_CHAR(cast(duedate as date)::DATE,'DD/MM/YYYY') as  data_vencimento
        , TO_CHAR(cast(shipdate as date)::DATE,'DD/MM/YYYY') as  data_envio
        , cast(status as int) as status
        , cast(onlineorderflag as boolean) as pedido_online
        , cast(purchaseordernumber as varchar) as numero_compra
        , cast(accountnumber as varchar) as numero_conta
        , cast(customerid as int) as fk_cliente
        , cast(salespersonid as int) as fk_representante_vendas
        , cast(territoryid as int) as fk_territorio
        , cast(billtoaddressid as int) as fk_endereco_pagamento
        , cast(shiptoaddressid as int) as fk_endereco_envio
        , cast(shipmethodid as int) as fk_metodo_envio
        , cast(creditcardid as int) as fk_cartoes_credito
        , cast(creditcardapprovalcode as varchar) as codigo_aprovacao
        , cast(currencyrateid as int) as fk_taxa_cambio
        , cast(subtotal as float) as subtotal
        , cast(taxamt as float) as imposto
        , cast(freight as float) as frete
        , cast(totaldue as float) as total_pedido_venda
        , cast(comment as varchar) as comentario
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from renamed
