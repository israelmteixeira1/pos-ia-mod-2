{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select 
        VAL_CARGA,
        DIN_INSTANTE,
        VAL_GERSOLAR,
        ID_SUBSISTEMA,
        VAL_GEREOLICA,
        NOM_SUBSISTEMA,
        VAL_GERTERMICA,
        VAL_INTERCAMBIO,
        VAL_GERHIDRAULICA,
        2023 as ano_referencia
    from {{ source('raw_ons', 'BALANCO_ENERGIA_SUBSISTEMA_2023') }}
    
    union all
    
    select 
        VAL_CARGA,
        DIN_INSTANTE,
        VAL_GERSOLAR,
        ID_SUBSISTEMA,
        VAL_GEREOLICA,
        NOM_SUBSISTEMA,
        VAL_GERTERMICA,
        VAL_INTERCAMBIO,
        VAL_GERHIDRAULICA,
        2024 as ano_referencia
    from {{ source('raw_ons', 'BALANCO_ENERGIA_SUBSISTEMA_2024') }}
    
    union all
    
    select 
        VAL_CARGA,
        DIN_INSTANTE,
        VAL_GERSOLAR,
        ID_SUBSISTEMA,
        VAL_GEREOLICA,
        NOM_SUBSISTEMA,
        VAL_GERTERMICA,
        VAL_INTERCAMBIO,
        VAL_GERHIDRAULICA,
        2025 as ano_referencia
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
    VAL_GERHIDRAULICA,
    ano_referencia
from raw
