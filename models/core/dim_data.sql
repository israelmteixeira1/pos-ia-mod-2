-- models/core/dim_data.sql

{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with calendario_expandido as (
    select 
        dateadd(day, seq4(), '2020-01-01') as data_completa
    from table(generator(rowcount => 3653))
),
dim_data_prep as (
    select 
        data_completa,
        year(data_completa) as ano,
        month(data_completa) as mes,
        day(data_completa) as dia,
        dayofweek(data_completa) as dia_semana_num,
        dayofyear(data_completa) as dia_do_ano,
        weekofyear(data_completa) as semana_do_ano,
        quarter(data_completa) as trimestre,
        monthname(data_completa) as nome_mes,
        dayname(data_completa) as nome_dia_semana,
        case when dayofweek(data_completa) in (1,7) then 'Final de Semana' else 'Dia Útil' end as tipo_dia,
        case when month(data_completa) between 5 and 10 then 'Seca' else 'Chuvosa' end as periodo_hidrologico,
        case when day(data_completa) <= 15 then 1 else 2 end as quinzena,
        to_char(data_completa,'YYYY-MM') as ano_mes,
        case when dayofweek(data_completa) in (1,7) then true else false end as is_final_semana,
        case when month(data_completa) in (12,1,2) then 'Verão'
             when month(data_completa) in (3,4,5) then 'Outono'
             when month(data_completa) in (6,7,8) then 'Inverno'
             else 'Primavera' end as estacao_ano
    from calendario_expandido
    where data_completa between '2020-01-01' and '2030-12-31'
)

select
    md5(to_varchar(data_completa)) as sk_data,
    data_completa,
    ano,
    mes,
    dia,
    dia_semana_num,
    dia_do_ano,
    semana_do_ano,
    trimestre,
    quinzena,
    nome_mes,
    nome_dia_semana,
    tipo_dia,
    estacao_ano,
    periodo_hidrologico,
    ano_mes,
    is_final_semana,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from dim_data_prep
