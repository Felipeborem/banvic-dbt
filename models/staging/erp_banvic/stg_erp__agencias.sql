with fonte_agencias as (
    select *
    from {{ source('erp', 'agencias') }}
),

renomeado as (
    select
        COD_AGENCIA
        ,NOME as nome_agencia
        ,ENDERECO as endereco_agencia
        ,COD_LOCALIDADE as cod_localidade_agencia
        ,DATA_ABERTURA as data_abertura_agencia
        ,TIPO_AGENCIA
    from fonte_agencias
)

select *
from renomeado