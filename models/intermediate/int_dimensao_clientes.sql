with clientes as (
        select*
        from {{ref ('stg_erp__clientes')}}
)

, localidades as (
        select*
        from {{ref ('stg_erp__localidades')}}
)

, clientes_enrriquecido as (
        select
            clientes.pk_cliente
            , clientes.fk_localidade
            , clientes.nome_cliente_completo
            , clientes.email_cliente
            , clientes.tipo_cliente
            , clientes.ts_inclusao
            , clientes.cpfcnpj_cliente
            , clientes.data_nascimento_cliente
            , clientes.endereco_cliente
            , clientes.cep_cliente

            , localidades.cidade as cidade_cliente
            ,localidades.uf as uf_cliente

        from clientes
        left join localidades on clientes.fk_localidade = localidades.pk_localidade
)

select*
from clientes_enrriquecido