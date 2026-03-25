with fonte_colaboradores as (
    select*
    from{{source('erp', 'colaboradores')}}
    
)

select*
from fonte_colaboradores