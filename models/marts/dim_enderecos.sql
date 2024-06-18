with 
    enderecos as (
        select 
            pk_endereco
            , endereco_linha_1
            , endereco_linha_2
            , cidade
            , fk_estados_provincias
            , codigo_postal
            , localizacao_espacial
            , rowguid
            , data_modificacao

        from {{ref("stg_erp__enderecos")}}
    )

    , estados_provincias as (
        select 
            pk_estado_provincia
            , cd_estado_provincia
            , fk_pais_regiao
            , apenas_estado_provincia
            , nm_estado_provincia
            , fk_territorio
            , rowguid
            , data_modificacao

        from {{ref("stg_erp__estado_provincia")}}

    )

    , paises_regioes as (
        select
            pk_pais_regiao
            , nm_pais_regiao
            , data_modificacao

        from {{ref("stg_erp__paises_regioes")}}
    )

    , joined  as (
        select
            enderecos.pk_endereco
            , enderecos.endereco_linha_1
            , enderecos.endereco_linha_2
            , enderecos.cidade
            , enderecos.codigo_postal
            , enderecos.localizacao_espacial
            , estados.cd_estado_provincia
            , estados.nm_estado_provincia
            , estados.apenas_estado_provincia
            , paises.nm_pais_regiao
        from enderecos
        left join estados_provincias as estados on enderecos.fk_estados_provincias = estados.pk_estado_provincia
        left join paises_regioes as paises on estados.fk_pais_regiao = paises.pk_pais_regiao

    )

    Select *
    from joined