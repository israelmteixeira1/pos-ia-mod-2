-- fato_balanco_energetico_diario.sql

{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with balanco_tratado as (
    select 
        id_subsistema,
        nom_subsistema,
        date(din_instante) as data_referencia,
        coalesce(try_cast(replace(val_gerhidraulica, ',', '.') as float), 0) as ger_hidraulica_mw,
        coalesce(try_cast(replace(val_gertermica, ',', '.') as float), 0) as ger_termica_mw,
        coalesce(try_cast(replace(val_gereolica, ',', '.') as float), 0) as ger_eolica_mw,
        coalesce(try_cast(replace(val_gersolar, ',', '.') as float), 0) as ger_solar_mw,
        coalesce(try_cast(replace(val_carga, ',', '.') as float), 0) as carga_mw,
        coalesce(try_cast(replace(val_intercambio, ',', '.') as float), 0) as intercambio_mw
    from {{ ref('stg_balanco_energia_subsistema') }}
    where din_instante is not null and id_subsistema is not null
),
balanco_diario_agregado as (
    select 
        data_referencia, id_subsistema, nom_subsistema,
        sum(ger_hidraulica_mw) as ger_hidraulica_mwh_dia,
        sum(ger_termica_mw) as ger_termica_mwh_dia,
        sum(ger_eolica_mw) as ger_eolica_mwh_dia,
        sum(ger_solar_mw) as ger_solar_mwh_dia,
        sum(carga_mw) as carga_mwh_dia,
        avg(intercambio_mw) as intercambio_mw_medio_dia,
        sum(ger_hidraulica_mw + ger_termica_mw + ger_eolica_mw + ger_solar_mw) as total_geracao_mwh_dia,
        count(*) as num_registros_horarios
    from balanco_tratado
    group by data_referencia, id_subsistema, nom_subsistema
)

select 
    md5(to_varchar(data_referencia) || '|' || id_subsistema) as sk_fato_balanco,
    data_referencia, id_subsistema, nom_subsistema,
    ger_hidraulica_mwh_dia, ger_termica_mwh_dia, ger_eolica_mwh_dia, ger_solar_mwh_dia,
    carga_mwh_dia, intercambio_mw_medio_dia, total_geracao_mwh_dia,
    (total_geracao_mwh_dia - carga_mwh_dia) as deficit_superavit_mwh_dia,
    case when total_geracao_mwh_dia > 0 then round((ger_hidraulica_mwh_dia / total_geracao_mwh_dia) * 100, 2) else 0 end as pct_ger_hidraulica,
    case when total_geracao_mwh_dia > 0 then round((ger_termica_mwh_dia / total_geracao_mwh_dia) * 100, 2) else 0 end as pct_ger_termica,
    case when total_geracao_mwh_dia > 0 then round((ger_eolica_mwh_dia / total_geracao_mwh_dia) * 100, 2) else 0 end as pct_ger_eolica,
    case when total_geracao_mwh_dia > 0 then round((ger_solar_mwh_dia / total_geracao_mwh_dia) * 100, 2) else 0 end as pct_ger_solar,
    num_registros_horarios,
    case when num_registros_horarios = 24 then 'COMPLETO' when num_registros_horarios >= 20 then 'QUASE_COMPLETO' else 'INCOMPLETO' end as status_qualidade_dados,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from balanco_diario_agregado
