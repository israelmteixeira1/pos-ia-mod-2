{{ config(
    materialized='table',
    schema='CORE_ONS'
) }}

with dados_geracao as (
    select
        data_instante,
        id_subsistema,
        nome_subsistema,
        nome_usina,
        codigo_usina_planejamento,
        ceg,
        prog_razao_eletrica,
        prog_garantia_energetica,
        ano_dados,
        mes_dados
    from {{ ref('stg_geracao_termica_despacho') }}
    where prog_razao_eletrica is not null 
       or prog_garantia_energetica is not null
),

agregacao_diaria as (
    select
        date(data_instante) as data_operacao,
        id_subsistema,
        nome_subsistema,
        nome_usina,
        codigo_usina_planejamento,
        ceg,
        ano_dados,
        mes_dados,
        
        sum(
            coalesce(prog_razao_eletrica, 0) + 
            coalesce(prog_garantia_energetica, 0)
        ) as ger_termica_nao_economica_mwh,
        
        sum(coalesce(prog_razao_eletrica, 0)) as prog_razao_eletrica_total_mwh,
        sum(coalesce(prog_garantia_energetica, 0)) as prog_garantia_energetica_total_mwh,
        count(*) as total_registros_dia,
        count(case when prog_razao_eletrica > 0 then 1 end) as periodos_com_razao_eletrica,
        count(case when prog_garantia_energetica > 0 then 1 end) as periodos_com_garantia_energetica
        
    from dados_geracao
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

resultado_final as (
    select
        data_operacao,
        id_subsistema,
        nome_subsistema,
        nome_usina,
        codigo_usina_planejamento,
        ceg,
        ano_dados,
        mes_dados,
        ger_termica_nao_economica_mwh,
        prog_razao_eletrica_total_mwh,
        prog_garantia_energetica_total_mwh,
        
        case 
            when ger_termica_nao_economica_mwh > 0 then
                round(
                    (prog_razao_eletrica_total_mwh / ger_termica_nao_economica_mwh) * 100, 2
                )
            else 0 
        end as pct_razao_eletrica,
        
        case 
            when ger_termica_nao_economica_mwh > 0 then
                round(
                    (prog_garantia_energetica_total_mwh / ger_termica_nao_economica_mwh) * 100, 2
                )
            else 0 
        end as pct_garantia_energetica,
        
        total_registros_dia,
        periodos_com_razao_eletrica,
        periodos_com_garantia_energetica,
        
        case 
            when total_registros_dia > 0 then
                round(ger_termica_nao_economica_mwh / total_registros_dia, 2)
            else 0 
        end as media_horaria_ger_nao_economica_mw
        
    from agregacao_diaria
    where ger_termica_nao_economica_mwh > 0
)

select * from resultado_final
order by data_operacao desc, nome_subsistema, nome_usina
