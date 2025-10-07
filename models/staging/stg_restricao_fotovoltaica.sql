{{ config(
    materialized='view',
    schema='STAGING_ONS'
) }}

with dados_unificados as (
    
    {% for mes in range(4, 13) %}
    select 
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ID_ESTADO,
        NOM_ESTADO,
        NOM_USINA,
        ID_ONS,
        CEG,
        DIN_INSTANTE,
        VAL_GERACAO,
        VAL_GERACAOLIMITADA,
        VAL_DISPONIBILIDADE,
        VAL_GERACAOREFERENCIA,
        VAL_GERACAOREFERENCIAFINAL,
        COD_RAZAORESTRICAO,
        COD_ORIGEMRESTRICAO,
        2024 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'RESTRICAO_COFF_FOTOVOLTAICA_2024_' + '%02d'|format(mes)) }}
    union all
    {% endfor %}
    
    {% for mes in range(1, 10) %}
    select 
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ID_ESTADO,
        NOM_ESTADO,
        NOM_USINA,
        ID_ONS,
        CEG,
        DIN_INSTANTE,
        VAL_GERACAO,
        VAL_GERACAOLIMITADA,
        VAL_DISPONIBILIDADE,
        VAL_GERACAOREFERENCIA,
        VAL_GERACAOREFERENCIAFINAL,
        COD_RAZAORESTRICAO,
        COD_ORIGEMRESTRICAO,
        2025 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'RESTRICAO_COFF_FOTOVOLTAICA_2025_' + '%02d'|format(mes)) }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
    -- Identificadores básicos
    case when trim(ID_SUBSISTEMA) = '' then null else trim(ID_SUBSISTEMA) end as id_subsistema,
    case when trim(NOM_SUBSISTEMA) = '' then null else upper(trim(NOM_SUBSISTEMA)) end as nome_subsistema,
    case when trim(ID_ESTADO) = '' then null else trim(ID_ESTADO) end as id_estado,
    case when trim(NOM_ESTADO) = '' then null else upper(trim(NOM_ESTADO)) end as nome_estado,
    case when trim(NOM_USINA) = '' then null else upper(trim(NOM_USINA)) end as nome_usina,
    case when trim(ID_ONS) = '' then null else trim(ID_ONS) end as id_ons,
    case when trim(CEG) = '' then null else upper(trim(CEG)) end as ceg,
    
    -- Data e tempo
    DIN_INSTANTE as data_instante,
    date(DIN_INSTANTE) as data_operacao,
    hour(DIN_INSTANTE) as hora_operacao,
    
    -- Valores de geração (MW)
    case when VAL_GERACAO < 0 then null else VAL_GERACAO end as geracao_mw,
    case when VAL_GERACAOLIMITADA < 0 then null else VAL_GERACAOLIMITADA end as geracao_limitada_mw,
    case when VAL_DISPONIBILIDADE < 0 then null else VAL_DISPONIBILIDADE end as disponibilidade_mw,
    case when VAL_GERACAOREFERENCIA < 0 then null else VAL_GERACAOREFERENCIA end as geracao_referencia_mw,
    case when VAL_GERACAOREFERENCIAFINAL < 0 then null else VAL_GERACAOREFERENCIAFINAL end as geracao_referencia_final_mw,
    
    -- Códigos de restrição
    case when trim(COD_RAZAORESTRICAO) = '' then null else trim(COD_RAZAORESTRICAO) end as codigo_razao_restricao,
    case when trim(COD_ORIGEMRESTRICAO) = '' then null else trim(COD_ORIGEMRESTRICAO) end as codigo_origem_restricao,
    
    -- Métricas derivadas
    case 
        when VAL_GERACAO > 0 and VAL_GERACAOLIMITADA > 0 then
            (VAL_GERACAO - VAL_GERACAOLIMITADA)
        else null
    end as restricao_aplicada_mw,
    
    case 
        when VAL_DISPONIBILIDADE > 0 and VAL_GERACAO > 0 then
            round((VAL_GERACAO / VAL_DISPONIBILIDADE) * 100, 2)
        else null
    end as utilizacao_disponibilidade_pct,
    
    case 
        when VAL_GERACAOLIMITADA > 0 and VAL_GERACAO > VAL_GERACAOLIMITADA then 'RESTRINGIDA'
        when VAL_GERACAOLIMITADA > 0 and VAL_GERACAO <= VAL_GERACAOLIMITADA then 'NORMAL'
        else 'SEM_LIMITE'
    end as status_restricao,
    
    ano_dados,
    mes_dados
    
from dados_unificados

where DIN_INSTANTE is not null
  and NOM_USINA is not null
  and trim(NOM_USINA) != ''
  and NOM_SUBSISTEMA is not null
  and trim(NOM_SUBSISTEMA) != ''
