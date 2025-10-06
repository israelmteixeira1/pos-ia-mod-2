{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select 
        EAR_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        EAR_MAX_SUBSISTEMA,
        EAR_VERIF_SUBSISTEMA_MWMES,
        EAR_VERIF_SUBSISTEMA_PERCENTUAL,
        2023 as ano_referencia
    from {{ source('raw_ons', 'EAR_DIARIO_SUBSISTEMA_2023') }}
    
    union all
    
    select 
        EAR_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        EAR_MAX_SUBSISTEMA,
        EAR_VERIF_SUBSISTEMA_MWMES,
        EAR_VERIF_SUBSISTEMA_PERCENTUAL,
        2024 as ano_referencia
    from {{ source('raw_ons', 'EAR_DIARIO_SUBSISTEMA_2024') }}
    
    union all
    
    select 
        EAR_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        EAR_MAX_SUBSISTEMA,
        EAR_VERIF_SUBSISTEMA_MWMES,
        EAR_VERIF_SUBSISTEMA_PERCENTUAL,
        2025 as ano_referencia
    from {{ source('raw_ons', 'EAR_DIARIO_SUBSISTEMA_2025') }}
)

select
    EAR_DATA,
    ID_SUBSISTEMA,
    NOM_SUBSISTEMA,
    EAR_MAX_SUBSISTEMA,
    EAR_VERIF_SUBSISTEMA_MWMES,
    EAR_VERIF_SUBSISTEMA_PERCENTUAL,
    ano_referencia
from raw
