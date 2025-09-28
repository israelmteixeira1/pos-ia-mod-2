{{ config(schema='STAGING_ONS', materialized='view') }}

WITH raw_data AS (
  SELECT
    REGIAO,
    UF,
    ESTACAO,
    DATA_MEDICAO,
    HORA_UTC,
    PRECIPITACAO_TOTAL_HORARIO_MM   AS raw_precip_mm,
    PRESSAO_ATMOSFERICA_NIVEL_ESTACAO_MB AS raw_pressao_mb,
    PRESSAO_ATMOSFERICA_MAX_HORA_ANT_MB  AS raw_pressao_max_mb,
    PRESSAO_ATMOSFERICA_MIN_HORA_ANT_MB  AS raw_pressao_min_mb,
    RADIACAO_GLOBAL_KJ_M2            AS raw_radiacao_kj,
    TEMPERATURA_AR_BULBO_SECO_C      AS raw_temp_c,
    TEMPERATURA_PONTO_ORVALHO_C      AS raw_orvalho_c,
    TEMPERATURA_MAXIMA_HORA_ANT_C    AS raw_temp_max_c,
    TEMPERATURA_MINIMA_HORA_ANT_C    AS raw_temp_min_c,
    TEMPERATURA_ORVALHO_MAX_HORA_ANT_C AS raw_orvalho_max_c,
    TEMPERATURA_ORVALHO_MIN_HORA_ANT_C AS raw_orvalho_min_c,
    UMIDADE_REL_MAX_HORA_ANT_PCT     AS raw_umid_max_pct,
    UMIDADE_REL_MIN_HORA_ANT_PCT     AS raw_umid_min_pct,
    UMIDADE_RELATIVA_HORARIA_PCT     AS raw_umid_pct,
    VENTO_DIRECAO_HORARIA_GRAUS      AS raw_vento_dir_deg,
    VENTO_RAJADA_MAXIMA_MS           AS raw_vento_rajada_ms,
    VENTO_VELOCIDADE_HORARIA_MS      AS raw_vento_vel_ms

  FROM {{ source('raw_ons', 'INMET_PROCESSED_DATA') }}
)

SELECT
  REGIAO,
  UF,
  ESTACAO,

  -- converter data para DATE
  TRY_TO_DATE(DATA_MEDICAO, 'YYYY/MM/DD') AS DATA_MEDICAO,

  HORA_UTC,

  -- converter vírgula em ponto, cast e substituir NULL por 0
  COALESCE(TRY_CAST(REPLACE(raw_precip_mm, ',', '.') AS FLOAT), 0)      AS PRECIPITACAO_MM,
  COALESCE(TRY_CAST(REPLACE(raw_pressao_mb, ',', '.') AS FLOAT), 0)     AS PRESSAO_ATMOSFERICA_MB,
  COALESCE(TRY_CAST(REPLACE(raw_pressao_max_mb, ',', '.') AS FLOAT), 0) AS PRESSAO_ATMOSFERICA_MAX_MB,
  COALESCE(TRY_CAST(REPLACE(raw_pressao_min_mb, ',', '.') AS FLOAT), 0) AS PRESSAO_ATMOSFERICA_MIN_MB,
  COALESCE(TRY_CAST(REPLACE(raw_radiacao_kj, ',', '.') AS FLOAT), 0)    AS RADIACAO_GLOBAL_KJ_M2,
  COALESCE(TRY_CAST(REPLACE(raw_temp_c, ',', '.') AS FLOAT), 0)         AS TEMPERATURA_AR_C,
  COALESCE(TRY_CAST(REPLACE(raw_orvalho_c, ',', '.') AS FLOAT), 0)      AS TEMPERATURA_PONTO_ORVALHO_C,
  COALESCE(TRY_CAST(REPLACE(raw_temp_max_c, ',', '.') AS FLOAT), 0)     AS TEMPERATURA_MAXIMA_C,
  COALESCE(TRY_CAST(REPLACE(raw_temp_min_c, ',', '.') AS FLOAT), 0)     AS TEMPERATURA_MINIMA_C,
  COALESCE(TRY_CAST(REPLACE(raw_orvalho_max_c, ',', '.') AS FLOAT), 0)  AS TEMPERATURA_ORVALHO_MAX_C,
  COALESCE(TRY_CAST(REPLACE(raw_orvalho_min_c, ',', '.') AS FLOAT), 0)  AS TEMPERATURA_ORVALHO_MIN_C,
  COALESCE(TRY_CAST(REPLACE(raw_umid_max_pct, ',', '.') AS FLOAT), 0)   AS UMIDADE_RELATIVA_MAX_PCT,
  COALESCE(TRY_CAST(REPLACE(raw_umid_min_pct, ',', '.') AS FLOAT), 0)   AS UMIDADE_RELATIVA_MIN_PCT,
  COALESCE(TRY_CAST(REPLACE(raw_umid_pct, ',', '.') AS FLOAT), 0)       AS UMIDADE_RELATIVA_PCT,
  COALESCE(TRY_CAST(REPLACE(raw_vento_dir_deg, ',', '.') AS FLOAT), 0)  AS VENTO_DIRECAO_GRAUS,
  COALESCE(TRY_CAST(REPLACE(raw_vento_rajada_ms, ',', '.') AS FLOAT), 0)AS VENTO_RAJADA_MAX_MS,
  COALESCE(TRY_CAST(REPLACE(raw_vento_vel_ms, ',', '.') AS FLOAT), 0)   AS VENTO_VELOCIDADE_MS

FROM raw_data
ORDER BY ESTACAO, DATA_MEDICAO, HORA_UTC
