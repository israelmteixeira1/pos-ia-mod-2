{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}


with base_dados_ml as (
    select
        b.data_referencia, b.id_subsistema, b.nom_subsistema,
        b.ger_hidraulica_mwh_dia, b.ger_termica_mwh_dia, b.ger_eolica_mwh_dia, b.ger_solar_mwh_dia,
        b.carga_mwh_dia, b.intercambio_mw_medio_dia,
        r.ear_percentual, r.ena_bruta_percentual_mlt,
        d.disp_sincronizada_total_mw as disp_sincronizada_mw,
        c.temperatura_media_dia_c, c.precipitacao_total_dia_mm
    from {{ ref('fato_balanco_energetico_diario') }} b
    left join {{ ref('fato_reservatorio_energia_diario') }} r
      on b.data_referencia = r.data_referencia and b.id_subsistema = r.id_subsistema
    left join (
      select data_referencia, id_subsistema, sum(disp_sincronizada_total_mw) as disp_sincronizada_total_mw 
      from {{ ref('fato_disponibilidade_usinas_diario') }} 
      group by data_referencia, id_subsistema
    ) d
      on b.data_referencia = d.data_referencia and b.id_subsistema = d.id_subsistema
    left join (
      select data_referencia, avg(temperatura_media_dia_c) as temperatura_media_dia_c,
             avg(precipitacao_total_dia_mm) as precipitacao_total_dia_mm 
      from {{ ref('fato_clima_diario') }} 
      group by data_referencia
    ) c
      on b.data_referencia = c.data_referencia
),
quantis_risco as (
    select
        percentile_cont(0.20) within group (order by intercambio_mw_medio_dia) as limite_risco_alto,
        percentile_cont(0.50) within group (order by intercambio_mw_medio_dia) as limite_risco_baixo,
        percentile_cont(0.90) within group (order by intercambio_mw_medio_dia) as prc90
    from base_dados_ml
),
dados_com_classificacao as (
    select
        m.*,
        q.limite_risco_alto, q.limite_risco_baixo, q.prc90,
        case
          when intercambio_mw_medio_dia <= q.limite_risco_alto then 'Alto'
          when intercambio_mw_medio_dia > q.limite_risco_alto and intercambio_mw_medio_dia <= q.limite_risco_baixo then 'Medio'
          else 'Baixo'
        end as nivel_risco_categoria,
        case
          when intercambio_mw_medio_dia <= q.limite_risco_alto then 100
          when intercambio_mw_medio_dia > q.limite_risco_alto and intercambio_mw_medio_dia <= q.limite_risco_baixo then 50
          else 20
        end as score_risco_numerico,
        percent_rank() over (order by intercambio_mw_medio_dia) * 100 as intercambio_percentil_atual
    from base_dados_ml m cross join quantis_risco q
),
dados_com_features as (
    select
        *,
        lag(carga_mwh_dia, 1) over (partition by id_subsistema order by data_referencia) as carga_d_menos_1,
        lag(ear_percentual, 1) over (partition by id_subsistema order by data_referencia) as ear_d_menos_1,
        lag(ena_bruta_percentual_mlt, 1) over (partition by id_subsistema order by data_referencia) as ena_d_menos_1,
        lag(disp_sincronizada_mw, 1) over (partition by id_subsistema order by data_referencia) as disp_d_menos_1,
        avg(temperatura_media_dia_c) over (partition by id_subsistema order by data_referencia rows between 7 preceding and 1 preceding) as temp_media_max_7d,
        sum(precipitacao_total_dia_mm) over (partition by id_subsistema order by data_referencia rows between 7 preceding and 1 preceding) as precip_soma_7d
    from dados_com_classificacao
)


select
    md5(to_varchar(data_referencia) || '|' || id_subsistema) as sk_fato_risco,
    data_referencia, id_subsistema, nom_subsistema, nivel_risco_categoria, score_risco_numerico, intercambio_percentil_atual,
    intercambio_mw_medio_dia, limite_risco_alto, limite_risco_baixo,
    ger_hidraulica_mwh_dia, ger_termica_mwh_dia, ger_eolica_mwh_dia, ger_solar_mwh_dia,
    carga_mwh_dia, ear_percentual, ena_bruta_percentual_mlt, disp_sincronizada_mw,
    temperatura_media_dia_c, precipitacao_total_dia_mm,
    carga_d_menos_1, ear_d_menos_1, ena_d_menos_1, disp_d_menos_1, temp_media_max_7d, precip_soma_7d,
    case when intercambio_mw_medio_dia >= prc90 then true else false end as flag_importacao_critica,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from dados_com_features
