{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select *
    from {{ source('raw_ons', 'DISPONIBILIDADE_USINA_GERAL') }}
)

select
  CEG,
  ID_ONS,
  ID_ESTADO,
  NOM_USINA,
  NOM_ESTADO,
  DIN_INSTANTE,
  ID_TIPOUSINA,
  ID_SUBSISTEMA,
  NOM_SUBSISTEMA,
  NOM_TIPOCOMBUSTIVEL,
  VAL_DISPOPERACIONAL,
  VAL_DISPSINCRONIZADA,
  VAL_POTENCIAINSTALADA
from raw
