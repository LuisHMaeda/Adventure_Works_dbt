with   
    pedidos as (
        select *
        from {{ref("stg_erp__pedidos")}}

    )

    ,pedidos_detalhes as (
        select *
        from {{ref("stg_erp__pedidos_detalhes")}}

    )

    , motivos as (
        select *
        from {{ ref('stg_erp__ref_pedidos_motivos') }}
    )

    ,joined as (
        select
            /*Chaves*/
            detalhes.fk_pedido
            , detalhes.pk_pedido_detalhes
            , pedidos.fk_cliente
            , pedidos.fk_representante_vendas
            , pedidos.fk_territorio
            , pedidos.fk_endereco_pagamento
            , pedidos.fk_endereco_envio
            , pedidos.fk_metodo_envio
            , pedidos.fk_cartoes_credito
            , detalhes.fk_produto
            , detalhes.fk_oferta_especial
            , pedidos.fk_taxa_cambio
            , motivos.fk_motivo_venda
            
            , detalhes.numero_rastreamento
            , detalhes.quantidade_pedido
            , detalhes.preco_unitario
            , detalhes.desconto_preco_unitario
            , pedidos.numero_revisoes
            , pedidos.data_pedido
            , pedidos.data_vencimento
            , pedidos.data_envio
            , case
                when pedidos.status = 1 then 'In progress'
                when pedidos.status = 2 then 'Approved'
                when pedidos.status = 3 then 'Backordered'
                when pedidos.status = 4 then 'Rejected'
                when pedidos.status = 5 then 'Shipped'
                when pedidos.status = 6 then 'Cancelled'
                else pedidos.status
            end as status
            , pedidos.pedido_online
            , pedidos.numero_compra
            , pedidos.numero_conta
            , pedidos.codigo_aprovacao
            , pedidos.subtotal
            , pedidos.imposto
            , pedidos.frete
            , pedidos.total_custo
            , pedidos.comentario

        from pedidos
        left join pedidos_detalhes as detalhes on detalhes.fk_pedido = pedidos.pk_pedido
        left join motivos on pedidos.pk_pedido = motivos.fk_pedido
    )

    , criada_chave_primaria as (
        select 
            cast(fk_pedido as varchar) || '-' || cast(fk_produto as varchar) || '-' || coalesce(cast(fk_motivo_venda as varchar),'') as sk_vendas /*surrogate key*/
            , *
        from joined
    )

select *
from criada_chave_primaria