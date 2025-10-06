{{ config(
    materialized='view',
    schema='STAGING_ONS'
) }}

with 

dados_unificados as (
    
    -- 2023 - Todos os 12 meses
    {% for mes in range(1, 13) %}
    select 
        din_instante,
        nom_tipopatamar,
        id_subsistema,
        nom_subsistema,
        nom_usina,
        cod_usinaplanejamento,
        ceg,
        val_proggeracao,
        val_progordemmerito,
        val_progordemdemeritoref,
        val_progordemdemeritoacimadainflex,
        val_proginflexibilidade,
        val_proginflexembutmerito,
        val_proginflexibilidadedessem,
        val_proginflexpura,
        val_prograzaoeletrica,
        val_proggarantiaenergetica,
        val_proggfom,
        val_progreposicaoperdas,
        val_progexportacao,
        val_progreservapotencia,
        val_proggsub,
        val_progunitcommitment,
        val_progconstrainedoff,
        val_verifgeracao,
        val_verifordemmerito,
        val_verifordemdemeritoacimadainflex,
        val_verifinflexibilidade,
        val_verifinflexembutmerito,
        val_verifinflexpura,
        val_verifrazaoeletrica,
        val_verifgarantiaenergetica,
        val_verifgfom,
        val_verifreposicaoperdas,
        val_verifexportacao,
        val_verifreservapotencia,
        val_atendsatisfatoriorpo,
        val_verifgsub,
        val_verifunitcommitment,
        val_verifconstrainedoff,
        tip_restricaoeletrica,
        2023 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'GERACAO_TERMICA_DESPACHO_2023_' + '%02d'|format(mes)) }}
    union all
    {% endfor %}
    
    -- 2024 - Todos os 12 meses
    {% for mes in range(1, 13) %}
    select 
        din_instante,
        nom_tipopatamar,
        id_subsistema,
        nom_subsistema,
        nom_usina,
        cod_usinaplanejamento,
        ceg,
        val_proggeracao,
        val_progordemmerito,
        val_progordemdemeritoref,
        val_progordemdemeritoacimadainflex,
        val_proginflexibilidade,
        val_proginflexembutmerito,
        val_proginflexibilidadedessem,
        val_proginflexpura,
        val_prograzaoeletrica,
        val_proggarantiaenergetica,
        val_proggfom,
        val_progreposicaoperdas,
        val_progexportacao,
        val_progreservapotencia,
        val_proggsub,
        val_progunitcommitment,
        val_progconstrainedoff,
        val_verifgeracao,
        val_verifordemmerito,
        val_verifordemdemeritoacimadainflex,
        val_verifinflexibilidade,
        val_verifinflexembutmerito,
        val_verifinflexpura,
        val_verifrazaoeletrica,
        val_verifgarantiaenergetica,
        val_verifgfom,
        val_verifreposicaoperdas,
        val_verifexportacao,
        val_verifreservapotencia,
        val_atendsatisfatoriorpo,
        val_verifgsub,
        val_verifunitcommitment,
        val_verifconstrainedoff,
        tip_restricaoeletrica,
        2024 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'GERACAO_TERMICA_DESPACHO_2024_' + '%02d'|format(mes)) }}
    union all
    {% endfor %}
    
    -- 2025 - Janeiro até setembro
    {% for mes in range(1, 10) %}
    select 
        din_instante,
        nom_tipopatamar,
        id_subsistema,
        nom_subsistema,
        nom_usina,
        cod_usinaplanejamento,
        ceg,
        val_proggeracao,
        val_progordemmerito,
        val_progordemdemeritoref,
        val_progordemdemeritoacimadainflex,
        val_proginflexibilidade,
        val_proginflexembutmerito,
        val_proginflexibilidadedessem,
        val_proginflexpura,
        val_prograzaoeletrica,
        val_proggarantiaenergetica,
        val_proggfom,
        val_progreposicaoperdas,
        val_progexportacao,
        val_progreservapotencia,
        val_proggsub,
        val_progunitcommitment,
        val_progconstrainedoff,
        val_verifgeracao,
        val_verifordemmerito,
        val_verifordemdemeritoacimadainflex,
        val_verifinflexibilidade,
        val_verifinflexembutmerito,
        val_verifinflexpura,
        val_verifrazaoeletrica,
        val_verifgarantiaenergetica,
        val_verifgfom,
        val_verifreposicaoperdas,
        val_verifexportacao,
        val_verifreservapotencia,
        val_atendsatisfatoriorpo,
        val_verifgsub,
        val_verifunitcommitment,
        val_verifconstrainedoff,
        tip_restricaoeletrica,
        2025 as ano_dados,
        {{ mes }} as mes_dados
    from {{ source('raw_ons', 'GERACAO_TERMICA_DESPACHO_2025_' + '%02d'|format(mes)) }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
    -- Campos básicos
    din_instante as data_instante,
    
    case when trim(nom_tipopatamar) = '' then null else upper(trim(nom_tipopatamar)) end as tipo_patamar,
    case when trim(id_subsistema) = '' then null else trim(id_subsistema) end as id_subsistema,
    case when trim(nom_subsistema) = '' then null else upper(trim(nom_subsistema)) end as nome_subsistema,
    case when trim(nom_usina) = '' then null else upper(trim(nom_usina)) end as nome_usina,
    
    cod_usinaplanejamento as codigo_usina_planejamento,
    case when trim(ceg) = '' then null else upper(trim(ceg)) end as ceg,
    
    -- Campos programados - texto
    case when trim(val_proggeracao) in ('', 'NULL', 'N/A') then null else trim(val_proggeracao) end as prog_geracao,
    case when trim(val_progordemmerito) in ('', 'NULL', 'N/A') then null else trim(val_progordemmerito) end as prog_ordem_merito,
    case when trim(val_progordemdemeritoref) in ('', 'NULL', 'N/A') then null else trim(val_progordemdemeritoref) end as prog_ordem_merito_ref,
    case when trim(val_progordemdemeritoacimadainflex) in ('', 'NULL', 'N/A') then null else trim(val_progordemdemeritoacimadainflex) end as prog_ordem_merito_acima_inflex,
    case when trim(val_proginflexibilidade) in ('', 'NULL', 'N/A') then null else trim(val_proginflexibilidade) end as prog_inflexibilidade,
    case when trim(val_proginflexembutmerito) in ('', 'NULL', 'N/A') then null else trim(val_proginflexembutmerito) end as prog_inflex_embut_merito,
    case when trim(val_proginflexibilidadedessem) in ('', 'NULL', 'N/A') then null else trim(val_proginflexibilidadedessem) end as prog_inflexibilidade_dessem,
    
    -- Campos programados - numéricos
    case when val_proginflexpura < 0 then null else val_proginflexpura end as prog_inflex_pura,
    case when val_prograzaoeletrica < 0 then null else val_prograzaoeletrica end as prog_razao_eletrica,
    case when val_proggarantiaenergetica < 0 then null else val_proggarantiaenergetica end as prog_garantia_energetica,
    case when val_proggfom < 0 then null else val_proggfom end as prog_gfom,
    case when val_progreposicaoperdas < 0 then null else val_progreposicaoperdas end as prog_reposicao_perdas,
    case when val_progexportacao < 0 then null else val_progexportacao end as prog_exportacao,
    case when val_progreservapotencia < 0 then null else val_progreservapotencia end as prog_reserva_potencia,
    case when val_proggsub < 0 then null else val_proggsub end as prog_gsub,
    case when val_progunitcommitment < 0 then null else val_progunitcommitment end as prog_unit_commitment,
    case when val_progconstrainedoff < 0 then null else val_progconstrainedoff end as prog_constrained_off,
    
    -- Campos verificados - texto
    case when trim(val_verifgeracao) in ('', 'NULL', 'N/A') then null else trim(val_verifgeracao) end as verif_geracao,
    case when trim(val_verifordemmerito) in ('', 'NULL', 'N/A') then null else trim(val_verifordemmerito) end as verif_ordem_merito,
    case when trim(val_verifordemdemeritoacimadainflex) in ('', 'NULL', 'N/A') then null else trim(val_verifordemdemeritoacimadainflex) end as verif_ordem_merito_acima_inflex,
    case when trim(val_verifinflexibilidade) in ('', 'NULL', 'N/A') then null else trim(val_verifinflexibilidade) end as verif_inflexibilidade,
    case when trim(val_verifinflexembutmerito) in ('', 'NULL', 'N/A') then null else trim(val_verifinflexembutmerito) end as verif_inflex_embut_merito,
    
    -- Campos verificados - numéricos
    case when val_verifinflexpura < 0 then null else val_verifinflexpura end as verif_inflex_pura,
    case when val_verifrazaoeletrica < 0 then null else val_verifrazaoeletrica end as verif_razao_eletrica,
    case when val_verifgarantiaenergetica < 0 then null else val_verifgarantiaenergetica end as verif_garantia_energetica,
    case when val_verifgfom < 0 then null else val_verifgfom end as verif_gfom,
    case when val_verifreposicaoperdas < 0 then null else val_verifreposicaoperdas end as verif_reposicao_perdas,
    case when val_verifexportacao < 0 then null else val_verifexportacao end as verif_exportacao,
    case when val_verifreservapotencia < 0 then null else val_verifreservapotencia end as verif_reserva_potencia,
    case when val_atendsatisfatoriorpo < 0 then null else val_atendsatisfatoriorpo end as atendimento_satisfatorio_rpo,
    case when val_verifgsub < 0 then null else val_verifgsub end as verif_gsub,
    case when val_verifunitcommitment < 0 then null else val_verifunitcommitment end as verif_unit_commitment,
    case when val_verifconstrainedoff < 0 then null else val_verifconstrainedoff end as verif_constrained_off,
    case when tip_restricaoeletrica < 0 then null else tip_restricaoeletrica end as tipo_restricao_eletrica,
    
    -- Metadados
    ano_dados,
    mes_dados
    
from dados_unificados

where din_instante is not null
  and nom_usina is not null 
  and trim(nom_usina) != ''
  and nom_subsistema is not null
  and trim(nom_subsistema) != ''
