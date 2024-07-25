 with   
    itens_por_pedido as (
        select *
        from {{ref('int_itens_por_pedido')}}
    )

    , metricas as (
        select
            /*Chaves*/
            sk_vendas
            , fk_pedido
            , fk_produto
            , fk_motivo_venda
            , fk_cliente
            , fk_representante_vendas
            , fk_territorio
            , fk_endereco_pagamento
            , fk_endereco_envio
            , fk_metodo_envio
            , fk_cartoes_credito
            , fk_oferta_especial
            , fk_taxa_cambio

            /*Datas*/
            , data_pedido
            , data_vencimento
            , data_envio

            /*Outros*/
            , numero_rastreamento
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
                as numeric(18,4)
            ) as subtotal_rateado

            , (numero_revisoes / count(fk_pedido) over(partition by fk_pedido)) as numero_revisoes_rateado

            , cast(
                imposto / count(fk_pedido) over(partition by fk_pedido)
                as numeric(18,4)
            ) as imposto_rateado

            , cast(
                frete / count(fk_pedido) over(partition by fk_pedido)
                as numeric(18,4)
            ) as frete_rateado
            
            , cast(
                total_custo / count(fk_pedido) over(partition by fk_pedido)  
                as numeric(18,4)
            ) as total_custo_rateado

            , cast(
                quantidade_pedido / count(sk_vendas) over(partition by sk_vendas)  
                as numeric(18,4)
            ) as quantidade_pedido_rateado

            ,quantidade_pedido

            , cast(
                preco_unitario / count(fk_pedido||'-'||fk_produto) over(partition by fk_pedido||'-'||fk_produto)  
                as numeric(18,4)
            ) as preco_unitario_rateado

            , cast(
                desconto_preco_unitario / count(fk_pedido||'-'||fk_produto) over(partition by fk_pedido||'-'||fk_produto)  
                as numeric(18,4)
            ) as desconto_preco_unitario_rateado


        from itens_por_pedido
    )

    , calculo_faturamento as (
        select 
        *
        , quantidade_pedido*preco_unitario_rateado as faturamento_bruto
        , (quantidade_pedido*preco_unitario_rateado) * (1-desconto_preco_unitario_rateado) as faturamento_liquido
        from metricas
    )

select *
from calculo_faturamento