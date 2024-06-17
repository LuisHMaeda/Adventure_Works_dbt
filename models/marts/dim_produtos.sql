with 
    produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )

select 
    pk_produto
    , nm_produto
    , codigo_produto
    , produto_fabricado
    , produto_acabado
    , cor
    , custo_padrao
    , preco_tabela
    , tamanho
    , cd_tamanho_unidade_medida
    , cd_peso_unidade_Medida
    , peso
    , dias_para_fabricar
    , linha_produto
    , classe
    , estilo
    , data_inicio_venda
    , data_fim_venda
    , data_descontinuado
from produtos