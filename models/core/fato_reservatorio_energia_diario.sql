{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with ear_tratado as (
    select
        id_subsistema, nom_subsistema, ear_data as data_referencia,
        coalesce(try_cast(replace(ear_max_subsistema, ',', '.') as float), 0) as ear_max_mwmes,
        coalesce(try_cast(replace(ear_verif_subsistema_mwmes, ',', '.') as float), 0) as ear_verificada_mwmes,
        coalesce(try_cast(replace(ear_verif_subsistema_percentual, ',', '.') as float), 0) as ear_percentual
    from {{ ref('stg_ear_diario_subsistema') }}
    where ear_data is not null and id_subsistema is not null
),
ena_tratado as (
    select
        id_subsistema, nom_subsistema, ena_data as data_referencia,
        coalesce(try_cast(replace(ena_bruta_regiao_mwmed, ',', '.') as float), 0) as ena_bruta_mwmed,
        coalesce(try_cast(replace(ena_armazenavel_regiao_mwmed, ',', '.') as float), 0) as ena_armazenavel_mwmed,
        coalesce(try_cast(replace(ena_bruta_regiao_percentualmlt, ',', '.') as float), 0) as ena_bruta_percentual_mlt,
        coalesce(try_cast(replace(ena_armazenavel_regiao_percentualmlt, ',', '.') as float), 0) as ena_armazenavel_percentual_mlt
    from {{ ref('stg_ena_diario_subsistema') }}
    where ena_data is not null and id_subsistema is not null
),
reservatorio_consolidado as (
    select
        coalesce(ear.data_referencia, ena.data_referencia) as data_referencia,
        coalesce(ear.id_subsistema, ena.id_subsistema) as id_subsistema,
        coalesce(ear.nom_subsistema, ena.nom_subsistema) as nom_subsistema,
        ear_max_mwmes, ear_verificada_mwmes, ear_percentual,
        ena_bruta_mwmed, ena_armazenavel_mwmed, ena_bruta_percentual_mlt, ena_armazenavel_percentual_mlt
    from ear_tratado ear
    full outer join ena_tratado ena on ear.data_referencia = ena.data_referencia and ear.id_subsistema = ena.id_subsistema
),
reservatorio_com_metricas as (
    select *, 
        case when ena_bruta_mwmed > 0 then round(ear_verificada_mwmes / ena_bruta_mwmed, 4) else null end as razao_ear_ena,
        case when ena_bruta_mwmed > 0 then round((ena_armazenavel_mwmed / ena_bruta_mwmed) * 100, 2) else null end as eficiencia_armazenamento_pct,
        case when ear_max_mwmes > 0 then round((ear_verificada_mwmes / ear_max_mwmes) * 100, 2) else null end as taxa_ocupacao_reservatorio_pct
    from reservatorio_consolidado
),
reservatorio_com_tendencias as (
    select *, 
        avg(ear_percentual) over (partition by id_subsistema order by data_referencia rows between 7 preceding and 1 preceding) as ear_percentual_media_7d_anterior,
        avg(ena_bruta_percentual_mlt) over (partition by id_subsistema order by data_referencia rows between 7 preceding and 1 preceding) as ena_percentual_media_7d_anterior,
        lag(ear_percentual, 1) over (partition by id_subsistema order by data_referencia) as ear_percentual_dia_anterior,
        lag(ena_bruta_percentual_mlt, 1) over (partition by id_subsistema order by data_referencia) as ena_percentual_dia_anterior
    from reservatorio_com_metricas
)

select
    md5(to_varchar(data_referencia) || '|' || id_subsistema) as sk_fato_reservatorio,
    data_referencia, id_subsistema, nom_subsistema,
    ear_max_mwmes, ear_verificada_mwmes, ear_percentual,
    ena_bruta_mwmed, ena_armazenavel_mwmed, ena_bruta_percentual_mlt, ena_armazenavel_percentual_mlt,
    razao_ear_ena, eficiencia_armazenamento_pct, taxa_ocupacao_reservatorio_pct,
    ear_percentual_media_7d_anterior, ena_percentual_media_7d_anterior, ear_percentual_dia_anterior, ena_percentual_dia_anterior,
    case when ear_percentual_dia_anterior is not null then round(ear_percentual - ear_percentual_dia_anterior, 2) else null end as variacao_ear_percentual_d1,
    case when ena_percentual_dia_anterior is not null then round(ena_bruta_percentual_mlt - ena_percentual_dia_anterior, 2) else null end as variacao_ena_percentual_d1,
    case when ear_percentual_media_7d_anterior is not null then round(ear_percentual - ear_percentual_media_7d_anterior, 2) else null end as tendencia_ear_7d,
    case when ear_percentual >= 80 then 'BAIXO_RISCO' when ear_percentual >= 50 then 'RISCO_MODERADO' when ear_percentual >= 30 then 'RISCO_ALTO' else 'RISCO_CRITICO' end as classificacao_risco_ear,
    case when ena_bruta_percentual_mlt >= 100 then 'ACIMA_DA_MEDIA' when ena_bruta_percentual_mlt >= 80 then 'NORMAL' when ena_bruta_percentual_mlt >= 60 then 'ABAIXO_DA_MEDIA' else 'MUITO_ABAIXO_DA_MEDIA' end as classificacao_ena,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from reservatorio_com_tendencias
