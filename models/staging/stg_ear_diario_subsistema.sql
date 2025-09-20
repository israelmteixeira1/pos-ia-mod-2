{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select *
    from {{ source('raw_ons', 'EAR_DIARIO_SUBSISTEMA_2025') }}
)

select
  EAR_DATA,
  ID_SUBSISTEMA,
  NOM_SUBSISTEMA,
  EAR_MAX_SUBSISTEMA,
  EAR_VERIF_SUBSISTEMA_MWMES,
  EAR_VERIF_SUBSISTEMA_PERCENTUAL
from raw
