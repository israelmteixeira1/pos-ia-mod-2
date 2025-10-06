{{ config(schema='STAGING_ONS', materialized='view') }}

with raw as (
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2023 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2023') }}
    
    union all
    
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2024 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2024') }}
    
    union all
    
    select 
        ENA_DATA,
        ID_SUBSISTEMA,
        NOM_SUBSISTEMA,
        ENA_BRUTA_REGIAO_MWMED,
        ENA_ARMAZENAVEL_REGIAO_MWMED,
        ENA_BRUTA_REGIAO_PERCENTUALMLT,
        ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
        2025 as ano_referencia
    from {{ source('raw_ons', 'ENA_DIARIO_SUBSISTEMA_2025') }}
)

select
    ENA_DATA,
    ID_SUBSISTEMA,
    NOM_SUBSISTEMA,
    ENA_BRUTA_REGIAO_MWMED,
    ENA_ARMAZENAVEL_REGIAO_MWMED,
    ENA_BRUTA_REGIAO_PERCENTUALMLT,
    ENA_ARMAZENAVEL_REGIAO_PERCENTUALMLT,
    ano_referencia
from raw
