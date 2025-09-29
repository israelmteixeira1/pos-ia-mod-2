{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with inmet_tratado as (
    select
        regiao, uf, estacao, data_medicao as data_referencia,
        precipitacao_mm, temperatura_ar_c, temperatura_maxima_c, temperatura_minima_c,
        umidade_relativa_pct, vento_velocidade_ms, pressao_atmosferica_mb, radiacao_global_kj_m2
    from {{ ref('stg_inmet_data') }}
    where data_medicao is not null and estacao is not null
),
clima_diario_agregado as (
    select
        data_referencia, regiao, uf, estacao,
        sum(precipitacao_mm) as precipitacao_total_dia_mm,
        avg(temperatura_ar_c) as temperatura_media_dia_c,
        max(temperatura_maxima_c) as temperatura_maxima_dia_c,
        min(temperatura_minima_c) as temperatura_minima_dia_c,
        avg(umidade_relativa_pct) as umidade_relativa_media_dia_pct,
        avg(vento_velocidade_ms) as vento_velocidade_media_dia_ms,
        avg(pressao_atmosferica_mb) as pressao_atmosferica_media_mb,
        avg(radiacao_global_kj_m2) as radiacao_global_media_kj_m2,
        count(*) as num_registros_horarios,
        count(case when precipitacao_mm > 0 then 1 end) as horas_com_chuva
    from inmet_tratado
    group by data_referencia, regiao, uf, estacao
),
clima_com_metricas_derivadas as (
    select *, 
        (temperatura_maxima_dia_c - temperatura_minima_dia_c) as amplitude_termica_dia_c,
        case when num_registros_horarios > 0 then round((horas_com_chuva::float / num_registros_horarios) * 100, 2) else 0 end as pct_horas_com_chuva,
        case when temperatura_media_dia_c between 18 and 26 and umidade_relativa_media_dia_pct between 40 and 70 then 'CONFORTAVEL'
             when temperatura_media_dia_c > 30 or umidade_relativa_media_dia_pct > 80 then 'DESCONFORTAVEL_QUENTE_UMIDO'
             when temperatura_media_dia_c < 15 then 'DESCONFORTAVEL_FRIO'
             else 'MODERADO' end as categoria_conforto_termico
    from clima_diario_agregado
),
clima_com_tendencias as (
    select *, 
        avg(temperatura_maxima_dia_c) over (partition by estacao order by data_referencia rows between 7 preceding and 1 preceding) as temp_maxima_media_7d,
        sum(precipitacao_total_dia_mm) over (partition by estacao order by data_referencia rows between 7 preceding and 1 preceding) as precipitacao_acumulada_7d,
        lag(temperatura_media_dia_c, 1) over (partition by estacao order by data_referencia) as temperatura_media_dia_anterior,
        lag(precipitacao_total_dia_mm, 1) over (partition by estacao order by data_referencia) as precipitacao_dia_anterior
    from clima_com_metricas_derivadas
)

select
    md5(to_varchar(data_referencia) || '|' || estacao) as sk_fato_clima,
    data_referencia, regiao, uf, estacao,
    temperatura_media_dia_c, temperatura_maxima_dia_c, temperatura_minima_dia_c, amplitude_termica_dia_c,
    precipitacao_total_dia_mm, pct_horas_com_chuva, umidade_relativa_media_dia_pct, vento_velocidade_media_dia_ms,
    pressao_atmosferica_media_mb, radiacao_global_media_kj_m2, temp_maxima_media_7d, precipitacao_acumulada_7d,
    case when temperatura_media_dia_anterior is not null then round(temperatura_media_dia_c - temperatura_media_dia_anterior, 2) else null end as variacao_temperatura_d1,
    case when precipitacao_dia_anterior is not null then round(precipitacao_total_dia_mm - precipitacao_dia_anterior, 2) else null end as variacao_precipitacao_d1,
    categoria_conforto_termico,
    case when precipitacao_total_dia_mm = 0 then 'SEM_CHUVA' when precipitacao_total_dia_mm <= 5 then 'CHUVA_FRACA' when precipitacao_total_dia_mm <= 25 then 'CHUVA_MODERADA' else 'CHUVA_FORTE' end as categoria_precipitacao,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from clima_com_tendencias
