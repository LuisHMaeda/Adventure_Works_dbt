with 

source as (

    select * from {{ source('erp', 'Person') }}

),

pessoas as (

    select
        cast(businessentityid as int) as pk_pessoa
        , cast(persontype as varchar) as tipo_pessoa
        , cast(namestyle as boolean) as estilo_nome
        , cast(title as varchar) as titulo
        , cast(firstname as varchar) as primeiro_nome
        , cast(middlename as varchar) as nome_do_meio
        , cast(lastname as varchar) as ultimo_nome
        , cast(suffix as varchar) as sufixo
        , cast(emailpromotion as int) as promocao_email
        , cast(additionalcontactinfo as varchar) as informacoes_adicionais
        , cast(demographics as varchar) as informacoes_demograficas
        , cast(rowguid as varchar) as rowguide
        , TO_CHAR(cast(MODIFIEDDATE as date)::DATE,'DD/MM/YYYY') as  data_modificacao


    from source

)

select * from pessoas
