{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with usinas_base as (
    select distinct 
        NOM_USINA as nom_usina, 
        ID_SUBSISTEMA as id_subsistema, 
        NOM_SUBSISTEMA as nom_subsistema, 
        ID_ESTADO as id_estado, 
        NOM_TIPOCOMBUSTIVEL as nom_tipocombustivel
    from {{ source('raw_ons', 'DISPONIBILIDADE_USINA_GERAL') }}
    where NOM_USINA is not null
)

select
    md5(nom_usina || '|' || id_subsistema) as sk_usina,
    nom_usina as nome_usina,
    id_subsistema,
    nom_subsistema,
    id_estado,
    nom_tipocombustivel,
    case 
        when upper(nom_tipocombustivel) like '%HIDRA%' then 'Hidrelétrica'
        when upper(nom_tipocombustivel) like '%EOLIC%' then 'Eólica'
        when upper(nom_tipocombustivel) like '%SOLAR%' then 'Solar'
        else 'Outros' end as tipo_usina,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from usinas_base
