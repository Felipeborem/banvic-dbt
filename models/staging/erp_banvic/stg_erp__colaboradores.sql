with fonte_colaboradores as (
    select *
    from {{ source('erp', 'colaboradores') }}
)

, renomeado as (
    select
        cast(cast(cod_colaborador as numeric) as int) as pk_colaborador
    from fonte_colaboradores
)

select *
from renomeado