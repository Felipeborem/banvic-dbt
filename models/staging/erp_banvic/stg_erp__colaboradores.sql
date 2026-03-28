with fonte_colaboradores as (
    select *
    from {{ source('erp', 'colaboradores') }}
)

, renomeado as (
    select
        cast(cast(cod_colaborador as numeric) as int) as pk_colaborador
        , cast(COD_COLABORADOR as int) as cod_colaborador
        , CAST(PRIMEIRO_NOME || ' ' || ULTIMO_NOME AS STRING) AS nome_completo
        , cast(EMAIL as STRING) as email_colaborador
        , cast(CPF as STRING) as cpf_colaborador
        , cast(DATA_NASCIMENTO as date) as data_nascimento_colaborador
        , cast(ENDERECO as STRING) as endereco_colaborador
        , cast(CEP as STRING) as cep_colaborador
        , cast(COD_GERENTE as int) as cod_gerente_colaborador
        , cast(COD_LOCALIDADE as int) as cod_localidade_colaborador
    from fonte_colaboradores
)


select*
from renomeado