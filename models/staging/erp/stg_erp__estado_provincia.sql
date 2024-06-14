with 

source as (

    select * from {{ source('erp', 'Stateprovince') }}

),

renamed as (

    select
        cast(stateprovinceid as int) as pk_estado_provincia
        , cast(stateprovincecode as varchar) as cd_estado_provincia
        , cast(countryregioncode as varchar) as fk_pais_regiao
        , cast(isonlystateprovinceflag as boolean) as apenas_estado_provincia
        , cast(name as varchar) as nm_estado_provincia
        , cast(territoryid as int) as fk_territorio
        , cast(rowguid as varchar) as rowguid
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao

    from source

)

select * from renamed
