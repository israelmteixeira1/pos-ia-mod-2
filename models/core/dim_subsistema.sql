-- dim_subsistema.sql

{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with subsistemas_base as (
    select distinct
        id_subsistema,
        nom_subsistema
    from {{ ref('stg_balanco_energia_subsistema') }}
    where id_subsistema is not null
)

select
    md5(id_subsistema) as sk_subsistema,
    id_subsistema,
    nom_subsistema,
    case 
        when id_subsistema='SE' then 'Sudeste/Centro-Oeste'
        when id_subsistema='S' then 'Sul'
        when id_subsistema='NE' then 'Nordeste'
        when id_subsistema='N' then 'Norte'
        else nom_subsistema end as nome_subsistema_completo,
    case 
        when id_subsistema='SE' then 'Sudeste'
        when id_subsistema='S' then 'Sul'
        when id_subsistema='NE' then 'Nordeste'
        when id_subsistema='N' then 'Norte'
        else 'Não Classificado' end as regiao_geografica,
    case 
        when id_subsistema='SE' then 'Grande Centro de Carga'
        when id_subsistema='S' then 'Mix Energético Diversificado'
        when id_subsistema='NE' then 'Forte Participação Eólica'
        when id_subsistema='N' then 'Predominância Hidrelétrica'
        else 'Características Mistas' end as perfil_energetico,
    case 
        when id_subsistema='SE' then 'SP,RJ,MG,ES,GO,MT,MS,DF'
        when id_subsistema='S' then 'RS,SC,PR'
        when id_subsistema='NE' then 'BA,PE,CE,RN,PB,AL,SE,MA,PI'
        when id_subsistema='N' then 'PA,AM,RO,AC,RR,AP,TO'
        else 'N/A' end as estados_principais,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from subsistemas_base
