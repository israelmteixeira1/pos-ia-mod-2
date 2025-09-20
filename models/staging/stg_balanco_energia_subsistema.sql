{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select *
    from {{ source('raw_ons', 'BALANCO_ENERGIA_SUBSISTEMA_2025') }}
)

select
  VAL_CARGA,
  DIN_INSTANTE,
  VAL_GERSOLAR,
  ID_SUBSISTEMA,
  VAL_GEREOLICA,
  NOM_SUBSISTEMA,
  VAL_GERTERMICA,
  VAL_INTERCAMBIO,
  VAL_GERHIDRAULICA
from raw
