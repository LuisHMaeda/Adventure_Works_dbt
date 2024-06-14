with 

source as (

    select * from {{ source('erp', 'Product') }}

),

produtos as (

    select
        cast(productid as int) as pk_produto
        , cast(name as varchar) as nm_produto
        , cast(productnumber as varchar) as codigo_produto
        , cast(makeflag as boolean) as produto_fabricado
        , cast(finishedgoodsflag as boolean) as produto_acabado
        , cast(color as varchar) as cor
        , cast(safetystocklevel as int) as nivel_estoque_de_seguranca
        , cast(reorderpoint as int) as ponto_de_reposicao
        , cast(standardcost as float) as custo_padrao
        , cast(listprice as float) as preco_tabela
        , cast(size as varchar) as tamanho
        , cast(sizeunitmeasurecode as varchar) as cd_tamanho_unidade_medida
        , cast(weightunitmeasurecode as varchar) as cd_peso_unidade_Medida
        , cast(weight as int) as peso
        , cast(daystomanufacture as int) as dias_para_fabricar
        , cast(productline as varchar) as linha_produto
        , cast(class as varchar) as classe
        , cast(style as varchar) as estilo
        , cast(productsubcategoryid as int) as fk_subcategoria
        , cast(productmodelid as int) as fk_modelo_produto
        , TO_CHAR(cast(sellstartdate as date)::DATE,'DD/MM/YYYY') as data_inicio_venda
        , TO_CHAR(cast(sellenddate as date)::DATE,'DD/MM/YYYY') as data_fim_venda
        , cast(discontinueddate as varchar) as data_descontinuado
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao
    from source

)

select * from produtos
