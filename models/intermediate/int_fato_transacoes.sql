with
    transacoes as (
        select
            pk_transacao,
            try_cast(fk_conta as numeric) as fk_conta,
            numero_transacao,
            data_transacao,
            ts_transacao,
            nome_transacao,
            tipo_transacao,
            valor_transacao
        from {{ ref('stg_erp__transacoes') }}
    ),

    contas as (
        select 
            try_cast(pk_conta as numeric) as pk_conta,
            try_cast(fk_cliente as numeric) as fk_cliente,
            try_cast(fk_agencia as numeric) as fk_agencia,
            try_cast(fk_colaborador as numeric) as fk_colaborador
        from {{ ref('int_fato_contas') }}
    ),

    datas as (
        select 
            pk_data,
            data_completa
        from {{ ref('int_dimensao_datas') }}
    ),

    joined as (
        select
            t.pk_transacao,
            t.fk_conta,
            c.fk_cliente,
            c.fk_agencia,
            c.fk_colaborador,
            d.pk_data as fk_data,
            t.numero_transacao,
            t.data_transacao,
            t.ts_transacao,
            t.nome_transacao,
            t.tipo_transacao,
            t.valor_transacao
        from transacoes t
        left join contas c 
            on t.fk_conta = c.pk_conta
        left join datas d 
            on try_cast(t.data_transacao as date) = d.data_completa
    )

select
    pk_transacao,
    fk_conta,
    fk_cliente,
    fk_agencia,
    fk_colaborador,
    fk_data,
    numero_transacao,
    data_transacao,
    ts_transacao,
    nome_transacao,
    tipo_transacao,
    valor_transacao
from joined