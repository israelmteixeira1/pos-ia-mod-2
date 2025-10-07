{{ config(
    materialized='table',
    schema='CORE_ONS'
) }}

with dados_fator_capacidade as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina_conjunto,
        tipo_usina,
        modalidade_operacao,
        ceg,
        id_ons,
        capacidade_instalada_mw,
        geracao_programada_mwh,
        geracao_verificada_mwh,
        fator_capacidade_percentual,
        ano_dados,
        mes_dados
    from {{ ref('stg_fator_capacidade') }}
    where id_subsistema = 'SE'  -- Filtro específico para subsistema SE
    and (
        capacidade_instalada_mw is not null
        or geracao_programada_mwh is not null
        or geracao_verificada_mwh is not null
    )
),

agregacao_diaria as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina_conjunto,
        tipo_usina,
        modalidade_operacao,
        ceg,
        id_ons,
        ano_dados,
        mes_dados,
        
        -- Capacidade instalada (valor fixo por usina)
        max(capacidade_instalada_mw) as capacidade_instalada_mw,
        
        -- Geração total diária
        sum(coalesce(geracao_programada_mwh, 0)) as geracao_programada_total_mwh,
        sum(coalesce(geracao_verificada_mwh, 0)) as geracao_verificada_total_mwh,
        
        -- Fator de capacidade médio do dia
        avg(fator_capacidade_percentual) as fator_capacidade_medio_pct,
        min(fator_capacidade_percentual) as fator_capacidade_minimo_pct,
        max(fator_capacidade_percentual) as fator_capacidade_maximo_pct,
        
        -- Contadores
        count(*) as total_registros_dia,
        count(case when geracao_verificada_mwh > 0 then 1 end) as periodos_com_geracao,
        count(case when fator_capacidade_percentual > 0 then 1 end) as periodos_com_fator_capacidade
        
    from dados_fator_capacidade
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),

resultado_final as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        id_estado,
        nome_estado,
        nome_usina_conjunto,
        tipo_usina,
        modalidade_operacao,
        ceg,
        id_ons,
        ano_dados,
        mes_dados,
        
        capacidade_instalada_mw,
        geracao_programada_total_mwh,
        geracao_verificada_total_mwh,
        

        (geracao_verificada_total_mwh - geracao_programada_total_mwh) as diferenca_prog_verif_mwh,

        case 
            when geracao_programada_total_mwh > 0 then
                round((geracao_verificada_total_mwh / geracao_programada_total_mwh) * 100, 2)
            else null 
        end as percentual_realizacao,
        
        fator_capacidade_medio_pct,
        fator_capacidade_minimo_pct,
        fator_capacidade_maximo_pct,
        
        case 
            when capacidade_instalada_mw > 0 and total_registros_dia > 0 then
                round((geracao_verificada_total_mwh / (capacidade_instalada_mw * 24)) * 100, 2)
            else null 
        end as utilizacao_capacidade_pct,
        
        'SE' as regiao_foco,
        
        case 
            when fator_capacidade_medio_pct >= 70 then 'ALTA_PERFORMANCE'
            when fator_capacidade_medio_pct >= 40 then 'MEDIA_PERFORMANCE'
            when fator_capacidade_medio_pct > 0 then 'BAIXA_PERFORMANCE'
            else 'SEM_DADOS'
        end as classificacao_performance,
        
        -- Contexto operacional
        total_registros_dia,
        periodos_com_geracao,
        periodos_com_fator_capacidade,
        
        case 
            when total_registros_dia > 0 then
                round(geracao_verificada_total_mwh / total_registros_dia, 2)
            else 0 
        end as media_horaria_geracao_mw
        
    from agregacao_diaria
    where geracao_verificada_total_mwh > 0 
       or geracao_programada_total_mwh > 0
       or capacidade_instalada_mw > 0
)

select * from resultado_final
order by data_operacao desc, nome_estado, nome_usina_conjunto
