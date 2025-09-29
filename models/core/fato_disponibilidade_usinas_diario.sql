{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

with disponibilidade_tratada as (
    select
        id_subsistema, nom_subsistema, nom_tipocombustivel, nom_usina,
        date(din_instante) as data_referencia,
        coalesce(try_cast(replace(val_potenciainstalada, ',', '.') as float), 0) as pot_instalada_mw,
        coalesce(try_cast(replace(val_dispoperacional, ',', '.') as float), 0) as disp_operacional_mw,
        coalesce(try_cast(replace(val_dispsincronizada, ',', '.') as float), 0) as disp_sincronizada_mw
    from {{ ref('stg_disponibilidade_usina') }}
    where din_instante is not null and id_subsistema is not null and nom_tipocombustivel is not null
),
disponibilidade_diaria as (
    select
        data_referencia, id_subsistema, nom_subsistema, nom_tipocombustivel,
        sum(pot_instalada_mw) as pot_instalada_total_mw,
        sum(disp_operacional_mw) as disp_operacional_total_mw,
        sum(disp_sincronizada_mw) as disp_sincronizada_total_mw,
        count(distinct nom_usina) as num_usinas_tipo,
        count(distinct case when disp_sincronizada_mw > 0 then nom_usina end) as num_usinas_ativas
    from disponibilidade_tratada
    group by data_referencia, id_subsistema, nom_subsistema, nom_tipocombustivel
),
disponibilidade_com_taxas as (
    select *, 
        case when pot_instalada_total_mw > 0 then round((disp_operacional_total_mw / pot_instalada_total_mw) * 100, 2) else 0 end as taxa_disponibilidade_operacional_pct,
        case when pot_instalada_total_mw > 0 then round((disp_sincronizada_total_mw / pot_instalada_total_mw) * 100, 2) else 0 end as taxa_disponibilidade_sincronizada_pct,
        case when num_usinas_tipo > 0 then round((num_usinas_ativas::float / num_usinas_tipo) * 100, 2) else 0 end as taxa_usinas_ativas_pct
    from disponibilidade_diaria
)

select
    md5(to_varchar(data_referencia) || '|' || id_subsistema || '|' || nom_tipocombustivel) as sk_fato_disponibilidade,
    data_referencia, id_subsistema, nom_subsistema, nom_tipocombustivel,
    pot_instalada_total_mw, disp_operacional_total_mw, disp_sincronizada_total_mw,
    num_usinas_tipo, num_usinas_ativas,
    taxa_disponibilidade_operacional_pct, taxa_disponibilidade_sincronizada_pct, taxa_usinas_ativas_pct,
    case when taxa_disponibilidade_sincronizada_pct >= 90 then 'ALTA' when taxa_disponibilidade_sincronizada_pct >= 70 then 'MEDIA' else 'BAIXA' end as categoria_disponibilidade,
    case when taxa_usinas_ativas_pct >= 80 then 'OTIMA' when taxa_usinas_ativas_pct >= 60 then 'BOA' else 'REGULAR' end as categoria_operacao_usinas,
    current_timestamp() as data_criacao,
    current_timestamp() as data_atualizacao
from disponibilidade_com_taxas
