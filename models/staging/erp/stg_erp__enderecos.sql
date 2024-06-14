with 

source as (

    select * from {{ source('erp', 'Address') }}

),

enderecos as (

    select
        cast(addressid as int) as pk_endereco 
        , cast(addressline1 as varchar) as endereco_linha_1
        , cast(addressline2 as varchar) as endereco_linha_2
        , cast(city as varchar) as cidade
        , cast(stateprovinceid as int) as fk_estados_provincias
        , cast(postalcode as varchar) as codigo_postal
        , cast(spatiallocation as varchar) as localizacao_espacial
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from enderecos
