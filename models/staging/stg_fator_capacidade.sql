{{ config(
    materialized='view',
    schema='STAGING_ONS'
) }}

with dados_unificados as (
    
    -- 2023 - Todos os 12 meses
    {% for mes in range(1, 13) %}
    select 
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ID_ESTADO,
        NOM_ESTADO,
        COD_PONTOCONEXAO,
        NOM_PONTOCONEXAO,
        NOM_LOCALIZACAO,
        VAL_LATITUDESECOLETORA,
        VAL_LONGITUDESECOLETORA,
        VAL_LATITUDEPONTOCONEXAO,
        VAL_LONGITUDEPONTOCONEXAO,
        NOM_MODALIDADEOPERACAO,
        NOM_TIPOUSINA,
        NOM_USINA_CONJUNTO,
        ID_ONS,
        CEG,
        DIN_INSTANTE,
        VAL_GERACAOPROGRAMADA,
        VAL_GERACAOVERIFICADA,
        VAL_CAPACIDADEINSTALADA,
        VAL_FATORCAPACIDADE,
        2023 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'FATOR_CAPACIDADE_2023_' + '%02d'|format(mes)) }}
    union all
    {% endfor %}
    
    -- 2024 - Todos os 12 meses
    {% for mes in range(1, 13) %}
    select 
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ID_ESTADO,
        NOM_ESTADO,
        COD_PONTOCONEXAO,
        NOM_PONTOCONEXAO,
        NOM_LOCALIZACAO,
        VAL_LATITUDESECOLETORA,
        VAL_LONGITUDESECOLETORA,
        VAL_LATITUDEPONTOCONEXAO,
        VAL_LONGITUDEPONTOCONEXAO,
        NOM_MODALIDADEOPERACAO,
        NOM_TIPOUSINA,
        NOM_USINA_CONJUNTO,
        ID_ONS,
        CEG,
        DIN_INSTANTE,
        VAL_GERACAOPROGRAMADA,
        VAL_GERACAOVERIFICADA,
        VAL_CAPACIDADEINSTALADA,
        VAL_FATORCAPACIDADE,
        2024 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'FATOR_CAPACIDADE_2024_' + '%02d'|format(mes)) }}
    union all
    {% endfor %}
    
    -- 2025 - Janeiro até setembro (mês 9)
    {% for mes in range(1, 10) %}
    select 
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ID_ESTADO,
        NOM_ESTADO,
        COD_PONTOCONEXAO,
        NOM_PONTOCONEXAO,
        NOM_LOCALIZACAO,
        VAL_LATITUDESECOLETORA,
        VAL_LONGITUDESECOLETORA,
        VAL_LATITUDEPONTOCONEXAO,
        VAL_LONGITUDEPONTOCONEXAO,
        NOM_MODALIDADEOPERACAO,
        NOM_TIPOUSINA,
        NOM_USINA_CONJUNTO,
        ID_ONS,
        CEG,
        DIN_INSTANTE,
        VAL_GERACAOPROGRAMADA,
        VAL_GERACAOVERIFICADA,
        VAL_CAPACIDADEINSTALADA,
        VAL_FATORCAPACIDADE,
        2025 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'FATOR_CAPACIDADE_2025_' + '%02d'|format(mes)) }}
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
    
    -- Ponto de conexão
    case when trim(COD_PONTOCONEXAO) = '' then null else trim(COD_PONTOCONEXAO) end as codigo_ponto_conexao,
    case when trim(NOM_PONTOCONEXAO) = '' then null else upper(trim(NOM_PONTOCONEXAO)) end as nome_ponto_conexao,
    case when trim(NOM_LOCALIZACAO) = '' then null else upper(trim(NOM_LOCALIZACAO)) end as nome_localizacao,
    
    -- Coordenadas geográficas
    case when VAL_LATITUDESECOLETORA = 0 then null else VAL_LATITUDESECOLETORA end as latitude_se_coletora,
    case when VAL_LONGITUDESECOLETORA = 0 then null else VAL_LONGITUDESECOLETORA end as longitude_se_coletora,
    case when VAL_LATITUDEPONTOCONEXAO = 0 then null else VAL_LATITUDEPONTOCONEXAO end as latitude_ponto_conexao,
    case when VAL_LONGITUDEPONTOCONEXAO = 0 then null else VAL_LONGITUDEPONTOCONEXAO end as longitude_ponto_conexao,
    
    -- Características operacionais
    case when trim(NOM_MODALIDADEOPERACAO) = '' then null else upper(trim(NOM_MODALIDADEOPERACAO)) end as modalidade_operacao,
    case when trim(NOM_TIPOUSINA) = '' then null else upper(trim(NOM_TIPOUSINA)) end as tipo_usina,
    case when trim(NOM_USINA_CONJUNTO) = '' then null else upper(trim(NOM_USINA_CONJUNTO)) end as nome_usina_conjunto,
    case when trim(ID_ONS) = '' then null else trim(ID_ONS) end as id_ons,
    case when trim(CEG) = '' then null else upper(trim(CEG)) end as ceg,
    
    -- Data e tempo
    DIN_INSTANTE as data_instante,
    date(DIN_INSTANTE) as data_operacao,
    
    -- Valores de geração (convertendo strings para números)
    case 
        when trim(VAL_GERACAOPROGRAMADA) in ('', 'NULL', 'N/A', '-') then null
        else try_cast(replace(VAL_GERACAOPROGRAMADA, ',', '.') as number(15,3))
    end as geracao_programada_mwh,
    
    case 
        when trim(VAL_GERACAOVERIFICADA) in ('', 'NULL', 'N/A', '-') then null
        else try_cast(replace(VAL_GERACAOVERIFICADA, ',', '.') as number(15,3))
    end as geracao_verificada_mwh,
    
    -- Capacidade instalada
    case when VAL_CAPACIDADEINSTALADA <= 0 then null else VAL_CAPACIDADEINSTALADA end as capacidade_instalada_mw,
    
    -- Fator de capacidade (convertendo string para número)
    case 
        when trim(VAL_FATORCAPACIDADE) in ('', 'NULL', 'N/A', '-') then null
        else try_cast(replace(VAL_FATORCAPACIDADE, ',', '.') as number(8,4))
    end as fator_capacidade_percentual,
    
    -- Metadados
    ano_dados,
    mes_dados
    
from dados_unificados

where DIN_INSTANTE is not null
  and NOM_USINA_CONJUNTO is not null
  and trim(NOM_USINA_CONJUNTO) != ''
  and NOM_SUBSISTEMA is not null
  and trim(NOM_SUBSISTEMA) != ''
