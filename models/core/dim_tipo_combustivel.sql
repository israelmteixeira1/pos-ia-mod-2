{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with tipos_combustivel_base as (
    select distinct NOM_TIPOCOMBUSTIVEL as nom_tipocombustivel
    from {{ source('raw_ons', 'DISPONIBILIDADE_USINA_GERAL') }}
    where NOM_TIPOCOMBUSTIVEL is not null
)

select
    md5(nom_tipocombustivel) as sk_tipo_combustivel,
    nom_tipocombustivel,
    upper(trim(nom_tipocombustivel)) as nome_tipo_combustivel_padronizado,
    case 
        when upper(nom_tipocombustivel) like '%HIDRA%' or upper(nom_tipocombustivel) like '%AGUA%' then 'Hidrelétrica'
        when upper(nom_tipocombustivel) like '%EOLIC%' or upper(nom_tipocombustivel) like '%VENTO%' then 'Eólica'
        when upper(nom_tipocombustivel) like '%SOLAR%' then 'Solar'
        when upper(nom_tipocombustivel) like '%BIOMASS%' then 'Biomassa'
        when upper(nom_tipocombustivel) like '%GAS%' then 'Gás Natural'
        when upper(nom_tipocombustivel) like '%CARVAO%' then 'Carvão'
        when upper(nom_tipocombustivel) like '%OLEO%' or upper(nom_tipocombustivel) like '%DIESEL%' then 'Derivados de Petróleo'
        when upper(nom_tipocombustivel) like '%NUCLEAR%' then 'Nuclear'
        else 'Outros' end as fonte_energetica,
    case 
        when upper(nom_tipocombustivel) like '%HIDRA%' or upper(nom_tipocombustivel) like '%EOLIC%' or upper(nom_tipocombustivel) like '%SOLAR%' or upper(nom_tipocombustivel) like '%BIOMASS%' then 'Renovável'
        else 'Não Renovável' end as classificacao_renovavel,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from tipos_combustivel_base
