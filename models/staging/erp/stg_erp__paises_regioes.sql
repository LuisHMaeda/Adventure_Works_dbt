with 

source as (

    select * from {{ source('erp', 'Countryregion') }}

),

paises_regioes as (

    select
        cast(countryregioncode as varchar) as pk_pais_regiao
        , cast(name as varchar) as nm_pais_regiao
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from paises_regioes
