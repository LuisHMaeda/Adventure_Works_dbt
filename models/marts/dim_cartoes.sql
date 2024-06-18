with
    cartoes_credito as (
        select 
            pk_cartoes_credito
            , tipo_cartao
            , numero_cartao
            , mes_expiracao
            , ano_expiracao

        from {{ref("stg_erp__cartoes_credito")}}
    )

select * 
from cartoes_credito