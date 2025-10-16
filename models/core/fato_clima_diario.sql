{{ config(
    schema='CORE_ONS',
    materialized='table'
) }}

WITH inmet_tratado AS (
    SELECT
        regiao, 
        uf, 
        estacao, 
        data_medicao AS data_referencia,
        precipitacao_mm, 
        temperatura_ar_c, 
        temperatura_maxima_c, 
        temperatura_minima_c,
        umidade_relativa_pct, 
        vento_velocidade_ms, 
        pressao_atmosferica_mb, 
        radiacao_global_kj_m2
    FROM {{ ref('stg_inmet_data') }}
    WHERE data_medicao IS NOT NULL 
      AND estacao IS NOT NULL
),

clima_diario_agregado AS (
    SELECT
        data_referencia, 
        regiao, 
        uf, 
        estacao,
        
        -- Agregações diárias
        SUM(COALESCE(precipitacao_mm, 0)) AS precipitacao_total_dia_mm,
        AVG(temperatura_ar_c) AS temperatura_media_dia_c,
        MAX(temperatura_maxima_c) AS temperatura_maxima_dia_c,
        MIN(temperatura_minima_c) AS temperatura_minima_dia_c,
        AVG(umidade_relativa_pct) AS umidade_relativa_media_dia_pct,
        AVG(vento_velocidade_ms) AS vento_velocidade_media_dia_ms,
        AVG(pressao_atmosferica_mb) AS pressao_atmosferica_media_mb,
        AVG(radiacao_global_kj_m2) AS radiacao_global_media_kj_m2,
        
        -- Contadores
        COUNT(*) AS num_registros_horarios,
        COUNT(CASE WHEN precipitacao_mm > 0 THEN 1 END) AS horas_com_chuva
        
    FROM inmet_tratado
    GROUP BY data_referencia, regiao, uf, estacao
),

clima_com_metricas_derivadas AS (
    SELECT 
        *, 
        
        -- Amplitude térmica
        (temperatura_maxima_dia_c - temperatura_minima_dia_c) AS amplitude_termica_dia_c,
        
        -- Percentual de horas com chuva
        CASE 
            WHEN num_registros_horarios > 0 
            THEN ROUND((horas_com_chuva::FLOAT / num_registros_horarios) * 100, 2) 
            ELSE 0 
        END AS pct_horas_com_chuva,
        
        -- Categoria de conforto térmico
        CASE 
            WHEN temperatura_media_dia_c BETWEEN 18 AND 26 
                 AND umidade_relativa_media_dia_pct BETWEEN 40 AND 70 
            THEN 'CONFORTAVEL'
            WHEN temperatura_media_dia_c > 30 
                 OR umidade_relativa_media_dia_pct > 80 
            THEN 'DESCONFORTAVEL_QUENTE_UMIDO'
            WHEN temperatura_media_dia_c < 15 
            THEN 'DESCONFORTAVEL_FRIO'
            ELSE 'MODERADO' 
        END AS categoria_conforto_termico
        
    FROM clima_diario_agregado
),

clima_com_tendencias AS (
    SELECT 
        *, 
        
        -- Médias móveis de 7 dias
        AVG(temperatura_maxima_dia_c) OVER (
            PARTITION BY estacao 
            ORDER BY data_referencia 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS temp_maxima_media_7d,
        
        SUM(precipitacao_total_dia_mm) OVER (
            PARTITION BY estacao 
            ORDER BY data_referencia 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS precipitacao_acumulada_7d,
        
        -- Valores do dia anterior
        LAG(temperatura_media_dia_c, 1) OVER (
            PARTITION BY estacao 
            ORDER BY data_referencia
        ) AS temperatura_media_dia_anterior,
        
        LAG(precipitacao_total_dia_mm, 1) OVER (
            PARTITION BY estacao 
            ORDER BY data_referencia
        ) AS precipitacao_dia_anterior
        
    FROM clima_com_metricas_derivadas
)

SELECT
    -- Chave surrogate (corrigida para evitar duplicatas)
    MD5(TO_VARCHAR(data_referencia) || '|' || regiao || '|' || uf || '|' || estacao) AS sk_fato_clima,
    
    -- Dimensões
    data_referencia, 
    regiao, 
    uf, 
    estacao,
    
    -- Métricas de temperatura
    temperatura_media_dia_c, 
    temperatura_maxima_dia_c, 
    temperatura_minima_dia_c, 
    amplitude_termica_dia_c,
    
    -- Métricas de precipitação
    precipitacao_total_dia_mm, 
    pct_horas_com_chuva,
    
    -- Outras métricas climáticas
    umidade_relativa_media_dia_pct, 
    vento_velocidade_media_dia_ms,
    pressao_atmosferica_media_mb, 
    radiacao_global_media_kj_m2,
    
    -- Tendências (7 dias)
    temp_maxima_media_7d, 
    precipitacao_acumulada_7d,
    
    -- Variações dia a dia
    CASE 
        WHEN temperatura_media_dia_anterior IS NOT NULL 
        THEN ROUND(temperatura_media_dia_c - temperatura_media_dia_anterior, 2) 
        ELSE NULL 
    END AS variacao_temperatura_d1,
    
    CASE 
        WHEN precipitacao_dia_anterior IS NOT NULL 
        THEN ROUND(precipitacao_total_dia_mm - precipitacao_dia_anterior, 2) 
        ELSE NULL 
    END AS variacao_precipitacao_d1,
    
    -- Categorias
    categoria_conforto_termico,
    
    CASE 
        WHEN precipitacao_total_dia_mm = 0 THEN 'SEM_CHUVA'
        WHEN precipitacao_total_dia_mm <= 5 THEN 'CHUVA_FRACA'
        WHEN precipitacao_total_dia_mm <= 25 THEN 'CHUVA_MODERADA'
        ELSE 'CHUVA_FORTE'
    END AS categoria_precipitacao,
    
    -- Auditoria
    CURRENT_TIMESTAMP() AS data_criacao,
    CURRENT_TIMESTAMP() AS data_atualizacao

FROM clima_com_tendencias
