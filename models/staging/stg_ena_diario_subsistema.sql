{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select *
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2025') }}
)

select
  ENA_DATA,
  ID_SUBSISTEMA,
  NOM_SUBSISTEMA,
  ENA_BRUTA_REGIAO_MWMED,
  ENA_ARMAZENAVEL_REGIAO_MWMED,
  ENA_BRUTA_REGIAO_PERCENTUALMLT,
  ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT
from raw
