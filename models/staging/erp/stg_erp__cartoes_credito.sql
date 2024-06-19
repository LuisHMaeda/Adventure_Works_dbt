with 

source as (

    select * from {{ source('erp', 'CreditCard') }}

),

cartoes as (

    select
        cast(creditcardid as int) as pk_cartoes_credito
        , cast(cardtype as varchar) as tipo_cartao
        , cast(cardnumber as int) as numero_cartao
        , cast(expmonth as int) as mes_expiracao
        , cast(expyear as int) as ano_expiracao
        , cast(MODIFIEDDATE as date) as  data_modificacao
    from source

)

select * from cartoes
