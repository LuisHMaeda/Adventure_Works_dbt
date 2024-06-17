with  
    clientes as (
        select 
            pk_cliente
            , fk_pessoa
            , fk_loja
            , fk_territorio

        from {{ref("stg_erp__clientes")}}

    )

    , pessoas as (
        select 
            pk_pessoa
            , tipo_pessoa
            , estilo_nome
            , titulo
            , primeiro_nome
            , nome_do_meio
            , ultimo_nome
            , sufixo
            , promocao_email
            , informacoes_adicionais
            , informacoes_demograficas
            , rowguide
            , data_modificacao
        from {{ref("stg_erp__pessoas")}}
    )


    

select 
    clientes.pk_cliente
    , pessoas.primeiro_nome
    , pessoas.nome_do_meio
    , pessoas.ultimo_nome
    , pessoas.sufixo
    , pessoas.titulo
from clientes
left join pessoas on clientes.fk_pessoa = pessoas.pk_pessoa