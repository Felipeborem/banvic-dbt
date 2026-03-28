

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

    colaboradores as (
        select
            cod_colaborador,
            nome_completo as nome_completo_colaborador,
            email_colaborador
        from {{ ref('dim_colaboradores')}}
    ),



    agencias as(
        select
            COD_AGENCIA as cod_agencia,
            nome_agencia,
            cod_localidade_agencia,
            tipo_agencia
        from {{ ref('dim_agencias')}}
    ),

    clientes as(
        select
            pk_cliente,
            fk_localidade,
            nome_cliente_completo,
            email_cliente,
            tipo_cliente,
            ts_inclusao,
            cpfcnpj_cliente,
            data_nascimento_cliente,
            endereco_cliente,
            cep_cliente,
            cidade_cliente,
            uf_cliente
        from {{ref('dim_clientes')}}

    ),

    datas as (
        select 
            pk_data,
            data_completa
        from {{ ref('dim_datas') }}
    ),

    joined as (
        select
            agencias.cod_agencia,
            agencias.nome_agencia,
            agencias.cod_localidade_agencia,
            agencias.tipo_agencia,
            colaboradores.cod_colaborador,
            colaboradores.nome_completo_colaborador,
            colaboradores.email_colaborador,
            clientes.email_cliente,
            clientes.tipo_cliente,
            clientes.nome_cliente_completo,
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
        left join clientes
        left join colaboradores
        left join agencias
    )

select
    cod_agencia,
    nome_agencia,
    cod_localidade_agencia,
    tipo_agencia,
    cod_colaborador,
    nome_completo_colaborador,
    email_colaborador,
    email_cliente,
    tipo_cliente,
    nome_cliente_completo,
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