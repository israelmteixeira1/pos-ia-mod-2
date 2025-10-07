{{ config(
    materialized='table',
    schema='CORE_ONS'
) }}

with dados_restricao_fotovoltaica as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina,
        ceg,
        id_ons,
        geracao_mw,
        geracao_limitada_mw,
        disponibilidade_mw,
        geracao_referencia_mw,
        geracao_referencia_final_mw,
        restricao_aplicada_mw,
        utilizacao_disponibilidade_pct,
        status_restricao,
        codigo_razao_restricao,
        codigo_origem_restricao,
        ano_dados,
        mes_dados
    from {{ ref('stg_restricao_fotovoltaica') }}
    where id_subsistema = 'SE'
    and (
        geracao_mw is not null
        or geracao_limitada_mw is not null
        or disponibilidade_mw is not null
    )
),

agregacao_diaria as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina,
        ceg,
        id_ons,
        ano_dados,
        mes_dados,
        
        -- Geração e disponibilidade média diária
        avg(coalesce(geracao_mw, 0)) as geracao_media_mw,
        max(coalesce(geracao_mw, 0)) as geracao_maxima_mw,
        min(coalesce(geracao_mw, 0)) as geracao_minima_mw,
        sum(coalesce(geracao_mw, 0)) as geracao_total_mwh,
        
        -- Restrições aplicadas
        avg(coalesce(geracao_limitada_mw, 0)) as geracao_limitada_media_mw,
        sum(coalesce(restricao_aplicada_mw, 0)) as restricao_total_aplicada_mwh,
        
        -- Disponibilidade
        avg(coalesce(disponibilidade_mw, 0)) as disponibilidade_media_mw,
        max(coalesce(disponibilidade_mw, 0)) as disponibilidade_maxima_mw,
        
        -- Utilização
        avg(utilizacao_disponibilidade_pct) as utilizacao_disponibilidade_media_pct,
        
        -- Contadores de restrição
        count(case when status_restricao = 'RESTRINGIDA' then 1 end) as periodos_restringidos,
        count(case when status_restricao = 'NORMAL' then 1 end) as periodos_normais,
        count(case when restricao_aplicada_mw > 0 then 1 end) as periodos_com_restricao,

        mode(codigo_razao_restricao) as codigo_razao_predominante,
        mode(codigo_origem_restricao) as codigo_origem_predominante,
        
        count(*) as total_registros_dia
        
    from dados_restricao_fotovoltaica
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

resultado_final as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina,
        ceg,
        id_ons,
        ano_dados,
        mes_dados,
        
        -- Métricas de geração
        geracao_media_mw,
        geracao_maxima_mw,
        geracao_minima_mw,
        geracao_total_mwh,
        
        -- Métricas de restrição
        geracao_limitada_media_mw,
        restricao_total_aplicada_mwh,
        
        -- Disponibilidade
        disponibilidade_media_mw,
        disponibilidade_maxima_mw,
        utilizacao_disponibilidade_media_pct,
        
        -- Percentual de períodos com restrição
        case 
            when total_registros_dia > 0 then
                round((periodos_restringidos::float / total_registros_dia) * 100, 2)
            else 0
        end as percentual_periodos_restringidos,
        
        -- Eficiência energética
        case 
            when disponibilidade_media_mw > 0 then
                round((geracao_media_mw / disponibilidade_media_mw) * 100, 2)
            else null
        end as eficiencia_energetica_pct,
        
        -- Perda por restrição
        case 
            when geracao_total_mwh > 0 then
                round((restricao_total_aplicada_mwh / geracao_total_mwh) * 100, 2)
            else 0
        end as percentual_perda_restricao,
        
        -- Classificação de severidade de restrição
        case 
            when percentual_periodos_restringidos >= 50 then 'ALTA_RESTRICAO'
            when percentual_periodos_restringidos >= 20 then 'MEDIA_RESTRICAO'
            when percentual_periodos_restringidos > 0 then 'BAIXA_RESTRICAO'
            else 'SEM_RESTRICAO'
        end as severidade_restricao,
        
        codigo_razao_predominante,
        codigo_origem_predominante,
        
        periodos_restringidos,
        periodos_normais,
        periodos_com_restricao,
        total_registros_dia,
    
        'SE' as regiao_foco
        
    from agregacao_diaria
    where geracao_total_mwh > 0
       or disponibilidade_media_mw > 0
)

select * from resultado_final
order by data_operacao desc, nome_estado, nome_usina
