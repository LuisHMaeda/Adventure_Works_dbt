with   
    itens_por_pedido as (
        select *
        from {{ref("int_itens_por_pedido")}}

    )

    , metricas as (
        select
            /*Chaves*/
            sk_vendas
            , fk_cliente
            , fk_representante_vendas
            , fk_territorio
            , fk_endereco_pagamento
            , fk_endereco_envio
            , fk_metodo_envio
            , fk_cartoes_credito
            , fk_produto
            , fk_oferta_especial
            , fk_taxa_cambio

            /*Datas*/
            , cast(data_pedido as date) as data_pedido
            , data_vencimento
            , data_envio

            /*Outros*/
            , numero_rastreamento
            , quantidade_pedido
            , preco_unitario
            , desconto_preco_unitario
            , status
            , pedido_online
            , numero_compra
            , codigo_aprovacao
            , comentario

            /*Metricas*/
            /*Colunas criadas como "rateada" para que o valor não fique duplicado*/
            , cast(
                subtotal / count(fk_pedido) over(partition by fk_pedido) 
                as numeric(18,2)
            ) as subtotal_rateado

            , cast(
                numero_revisoes / count(fk_pedido) over(partition by fk_pedido)
                as numeric(18,2)
            ) as numero_revisoes_rateado

            , cast(
                imposto / count(fk_pedido) over(partition by fk_pedido)
                as numeric(18,2)
            ) as imposto_rateado

            , cast(
                frete / count(fk_pedido) over(partition by fk_pedido)
                as numeric(18,2)
            ) as frete_rateado
            
            , cast(
                total_custo / count(fk_pedido) over(partition by fk_pedido)  
                as numeric(18,2)
            ) as total_custo_rateado

            , preco_unitario*quantidade_pedido as faturamento_bruto

        from itens_por_pedido
    )
    select * 
    from metricas
 