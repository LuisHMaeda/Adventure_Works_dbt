with
    motivos as (
    select 
        pk_motivo_venda
        , nm_motivo_venda
        , tipo_motivo

    from {{ref("stg_erp__motivos_venda")}}
    )

select *
from motivos
