with fonte_agencias as (
    select *
    from {{ source('erp', 'agencias') }}
)

, renomeado as (
    select
        cast(cast(cod_agencia as numeric) as int) as pk_agencia
    from fonte_agencias
)

select *
from renomeado