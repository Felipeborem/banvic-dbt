with fonte_clientes as (
        select *
        from {{ source('erp', 'clientes') }}
    )

    , renomeado as (
        select
            cast(cast(COD_CLIENTE as numeric) as int) as pk_cliente
            , cast(cast(cod_localidade as numeric) as int) as fk_localidade
            , primeiro_nome || '  ' || ultimo_nome as nome_cliente_completo
            , email as email_cliente
            , tipo_cliente
            , cast(DATA_INCLUSAO as timestamp) as ts_inclusao
            , regexp_replace(cpfcnpj, '[^a-zA-Z0-9]', '') as cpfcnpj_cliente
            , cast(data_nascimento as date) as data_nascimento_cliente
            , endereco as endereco_cliente
            , regexp_replace(cep, '[^a-zA-Z0-9]', '') as cep_cliente
        from fonte_clientes
    )

select *
from renomeado