with fonte_agencias as (
    select*
    from{{source('erp', 'agencias')}}
    
)

select*
from fonte_agencias