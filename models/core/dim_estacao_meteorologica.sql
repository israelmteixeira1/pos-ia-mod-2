-- dim_estacao_meteorologica.sql

{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with estacoes_base as (
    select distinct regiao, uf, estacao
    from {{ ref('stg_inmet_data') }}
    where estacao is not null
)

select
    md5(regiao || '|' || uf || '|' || estacao) as sk_estacao,
    estacao as codigo_estacao,
    regiao,
    uf,
    case 
        when uf='SP' then 'São Paulo'
        when uf='RJ' then 'Rio de Janeiro'
        when uf='MG' then 'Minas Gerais'
        when uf='GO' then 'Goiás'
        else uf end as nome_estado,
    case 
        when uf in ('SP','RJ','MG','ES','GO','MT','MS','DF') then 'SE'
        when uf in ('RS','SC','PR') then 'S'
        when uf in ('BA','PE','CE','RN','PB','AL','SE','MA','PI') then 'NE'
        when uf in ('PA','AM','RO','AC','RR','AP','TO') then 'N'
        else 'OUTROS' end as subsistema_eletrico,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from estacoes_base
