with
    fonte_transacoes as (
        select *
        from {{ source('erp', 'transacoes') }}
    )

    , datas_validas as (
        select *
        from fonte_transacoes
        where try_cast(data_transacao as date) is not null
    )
    
    , renomeado as (
        select
            cast(cast(cod_transacao as numeric) as int) as pk_transacao
            , cast(cast(num_conta as numeric) as int) as fk_conta
            , cast(cast(cod_transacao as numeric) as int) as numero_transacao
            , cast(data_transacao as date) as data_transacao
            , cast(data_transacao as timestamp) as ts_transacao
            , nome_transacao
            , case 
                when cast(valor_transacao as numeric(28,2)) > 0 then 'Crédito'
                when cast(valor_transacao as numeric(28,2)) < 0 then 'Débito'
                else null 
            end as tipo_transacao
            , cast(valor_transacao as numeric(28,2)) as valor_transacao
        from datas_validas
    )

select *
from renomeado