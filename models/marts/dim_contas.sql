with
    dim_contas as (
        select *
        from {{ ref('int_dim_contas') }}
    )

select *
from dim_contas