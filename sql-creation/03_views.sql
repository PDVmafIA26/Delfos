-- =============================================================================
-- PROYECTO DELFOS — CAPA ORO
-- 03_views.sql : Vistas para facilitar consultas a otros gremios vía HTTP/GET
--
-- VISTAS DEFINIDAS:
--   v_anomalias_recientes       → Últimas anomalías con datos legibles (Reportes)
--   v_mercados_activos          → Estado actual de todos los mercados (Ingesta/Reportes)
--   v_flip_candidates           → Mercados cerca del umbral 0.5 (predicción de FLIP)
--   v_wallets_sospechosas       → Top wallets marcadas como sospechosas (Análisis)
--   v_volumen_live_vs_batch     → Comparativa volumen en vivo vs histórico (Detección)
--   v_resumen_diario            → Resumen ejecutivo del día para el informe de Airflow
--   v_anomalias_por_tipo        → Conteo de anomalías agrupadas por tipo y hora
--   v_mercado_detalle           → Vista completa de un mercado con sus tokens y stats
-- =============================================================================


-- =============================================================================
-- VISTA 1: Anomalías recientes (últimas 24h) con contexto legible
-- Usada por: Gremio de Reportes
-- =============================================================================
CREATE OR REPLACE VIEW v_anomalias_recientes AS
SELECT
    a.alert_id,
    a.category,
    a.sub_type,
    a.created_at,
    -- Campos comunes extraídos del payload JSON
    a.payload->>'question'      AS question,
    a.payload->>'slug'          AS slug,
    a.payload->>'asset_id'      AS asset_id,
    a.payload->>'user_id'       AS user_id,
    a.payload->>'change'        AS flip_direction,
    (a.payload->>'actual_price')::NUMERIC  AS actual_price,
    (a.payload->>'variation_pct')::NUMERIC AS variation_pct,
    (a.payload->>'ratio')::NUMERIC         AS volume_ratio,
    (a.payload->>'usd_detected')::NUMERIC  AS usd_detected,
    (a.payload->>'market_impact_pct')::NUMERIC AS market_impact_pct,
    a.payload->>'historical_pnl'   AS historical_pnl_level,
    (a.payload->>'age_minutes')::NUMERIC   AS account_age_minutes,
    (a.payload->>'ratio_vs_normal')::NUMERIC AS ratio_vs_normal,
    a.payload                   AS raw_payload  -- Payload completo por si hace falta
FROM anomalias a
WHERE a.created_at >= NOW() - INTERVAL '24 hours'
ORDER BY a.created_at DESC;

COMMENT ON VIEW v_anomalias_recientes IS
  'Anomalías de las últimas 24h con campos del payload JSON ya extraídos y tipados.';


-- =============================================================================
-- VISTA 2: Estado actual de todos los mercados activos
-- Usada por: Gremio de Ingesta (verificación), Gremio de Reportes (contexto)
-- =============================================================================
CREATE OR REPLACE VIEW v_mercados_activos AS
SELECT
    mm.condition_id,
    mm.id            AS market_id,
    mm.question,
    mm.slug,
    mm.liquidity,
    mm.volume,
    mm.volume_24h,
    e.event_id,
    e.question       AS event_question,
    -- Tokens asociados (YES y NO)
    ot.asset_id,
    ot.outcome_name,
    ot.price         AS current_price,
    -- Stats del batch
    mds.avg_price_24h,
    mds.main_sentiment_24h,
    mds.last_stable_price,
    mds.historical_vol_5min_avg,
    mds.calc_date    AS stats_date
FROM mercados_master mm
JOIN eventos e ON e.event_id = mm.event_id
LEFT JOIN outcome_tokens ot  ON ot.condition_id = mm.condition_id
LEFT JOIN market_daily_stats mds
    ON mds.asset_id = ot.asset_id
    AND mds.calc_date = CURRENT_DATE
WHERE e.active = TRUE
ORDER BY mm.volume_24h DESC NULLS LAST;

COMMENT ON VIEW v_mercados_activos IS
  'Todos los mercados activos con sus tokens, precios actuales y estadísticas del batch.';


-- =============================================================================
-- VISTA 3: Candidatos a FLIP (mercados en zona de peligro 0.40–0.60)
-- Usada por: Gremio de Análisis/Detección (predicción anticipada)
-- =============================================================================
CREATE OR REPLACE VIEW v_flip_candidates AS
SELECT
    ot.asset_id,
    ot.outcome_name,
    ot.price                     AS current_price,
    mds.avg_price_24h,
    mds.main_sentiment_24h       AS current_batch_sentiment,
    mm.question,
    mm.slug,
    mm.condition_id,
    mm.liquidity,
    -- Distancia al umbral 0.5
    ABS(ot.price - 0.5)          AS distance_to_flip,
    -- ¿Ya ha habido una alerta FLIP hoy?
    EXISTS (
        SELECT 1 FROM anomalias a
        WHERE a.sub_type = 'FLIP'
          AND a.payload->>'asset_id' = ot.asset_id
          AND a.created_at >= CURRENT_DATE
    ) AS flip_already_alerted_today
FROM outcome_tokens ot
JOIN mercados_master mm   ON mm.condition_id = ot.condition_id
LEFT JOIN market_daily_stats mds
    ON mds.asset_id = ot.asset_id
    AND mds.calc_date = CURRENT_DATE
WHERE ot.price BETWEEN 0.38 AND 0.62  -- Zona pre-flip
ORDER BY ABS(ot.price - 0.5) ASC;     -- Los más cercanos al límite primero

COMMENT ON VIEW v_flip_candidates IS
  'Mercados cuyo precio está entre 0.38 y 0.62: candidatos a cruzar el umbral de sentimiento.';


-- =============================================================================
-- VISTA 4: Wallets sospechosas con su historial y últimas actividades
-- Usada por: Gremio de Análisis
-- =============================================================================
CREATE OR REPLACE VIEW v_wallets_sospechosas AS
SELECT
    u.wallet_address,
    u.total_won,
    u.total_lost,
    u.net_pnl,
    u.total_position,
    u.first_seen_at,
    EXTRACT(EPOCH FROM (NOW() - u.first_seen_at)) / 3600  AS account_age_hours,
    usb.historical_pnl_level,
    usb.avg_bet_size,
    -- Número de alertas generadas por esta wallet
    COUNT(DISTINCT a.alert_id)                             AS total_alerts,
    -- Tipos de alertas
    array_agg(DISTINCT a.sub_type)                        AS alert_types,
    -- Última alerta
    MAX(a.created_at)                                      AS last_alert_at
FROM usuarios u
LEFT JOIN user_stats_batch usb ON usb.wallet_address = u.wallet_address
LEFT JOIN anomalias a ON a.payload->>'user_id' = u.wallet_address
WHERE u.es_sospechoso = TRUE
GROUP BY
    u.wallet_address, u.total_won, u.total_lost, u.net_pnl,
    u.total_position, u.first_seen_at,
    usb.historical_pnl_level, usb.avg_bet_size
ORDER BY total_alerts DESC, u.net_pnl DESC;

COMMENT ON VIEW v_wallets_sospechosas IS
  'Wallets marcadas como sospechosas con su perfil, historial y alertas acumuladas.';


-- =============================================================================
-- VISTA 5: Volumen en vivo vs batch (útil para detección manual de SPIKE)
-- Usada por: Gremio de Análisis / Dashboard de Reportes (gráfico de barras)
-- =============================================================================
CREATE OR REPLACE VIEW v_volumen_live_vs_batch AS
SELECT
    ot.asset_id,
    ot.outcome_name,
    mm.question,
    mm.slug,
    -- Volumen en los últimos 5 min (streaming)
    COALESCE(SUM(ltp.size) FILTER (WHERE ltp.traded_at >= NOW() - INTERVAL '5 minutes'), 0)
        AS vol_5min_live,
    -- Volumen en la última hora
    COALESCE(SUM(ltp.size) FILTER (WHERE ltp.traded_at >= NOW() - INTERVAL '1 hour'), 0)
        AS vol_1h_live,
    -- Promedio histórico del batch
    mds.historical_vol_5min_avg,
    -- Ratio live/batch
    CASE
        WHEN mds.historical_vol_5min_avg > 0
        THEN ROUND(
            COALESCE(SUM(ltp.size) FILTER (WHERE ltp.traded_at >= NOW() - INTERVAL '5 minutes'), 0)
            / mds.historical_vol_5min_avg, 2)
        ELSE NULL
    END AS spike_ratio,
    -- ¿Supera el umbral de 5x?
    CASE
        WHEN mds.historical_vol_5min_avg > 0
        THEN COALESCE(SUM(ltp.size) FILTER (WHERE ltp.traded_at >= NOW() - INTERVAL '5 minutes'), 0)
             / mds.historical_vol_5min_avg > 5
        ELSE FALSE
    END AS is_spike
FROM outcome_tokens ot
JOIN mercados_master mm     ON mm.condition_id = ot.condition_id
LEFT JOIN last_trade_price ltp ON ltp.asset_id = ot.asset_id
LEFT JOIN market_daily_stats mds
    ON mds.asset_id = ot.asset_id
    AND mds.calc_date = CURRENT_DATE
GROUP BY
    ot.asset_id, ot.outcome_name, mm.question, mm.slug, mds.historical_vol_5min_avg
ORDER BY spike_ratio DESC NULLS LAST;

COMMENT ON VIEW v_volumen_live_vs_batch IS
  'Comparativa de volumen en vivo (5min/1h) frente al histórico del batch. spike_ratio > 5 = alarma.';


-- =============================================================================
-- VISTA 6: Resumen ejecutivo diario (para el DAG de Airflow y el informe Telegram)
-- Usada por: Gremio de Orquestación (Airflow), Gremio de Mensajería
-- =============================================================================
CREATE OR REPLACE VIEW v_resumen_diario AS
SELECT
    CURRENT_DATE                                               AS report_date,
    -- Totales de anomalías del día
    COUNT(*) FILTER (WHERE sub_type = 'FLIP')                  AS total_flips,
    COUNT(*) FILTER (WHERE sub_type = 'SPIKE')                 AS total_spikes,
    COUNT(*) FILTER (WHERE sub_type = 'PRICE_VAR')             AS total_price_var,
    COUNT(*) FILTER (WHERE sub_type = 'WHALE_MOVE')            AS total_whales,
    COUNT(*) FILTER (WHERE sub_type = 'FLASH_ACC')             AS total_flash_accs,
    COUNT(*)                                                   AS total_anomalias,
    -- La anomalía de mayor impacto (por ratio de volumen)
    (
        SELECT a2.payload->>'question'
        FROM anomalias a2
        WHERE a2.created_at >= CURRENT_DATE
          AND a2.sub_type = 'SPIKE'
        ORDER BY (a2.payload->>'ratio')::NUMERIC DESC NULLS LAST
        LIMIT 1
    )                                                          AS top_spike_market,
    -- Wallet más activa del día
    (
        SELECT a3.payload->>'user_id'
        FROM anomalias a3
        WHERE a3.created_at >= CURRENT_DATE
          AND a3.category = 'USER'
        GROUP BY a3.payload->>'user_id'
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )                                                          AS most_active_suspicious_wallet,
    -- Mercados con alguna anomalía hoy (sin duplicados)
    COUNT(DISTINCT payload->>'slug') FILTER (WHERE category = 'MARKET')
                                                               AS markets_with_anomalies
FROM anomalias
WHERE created_at >= CURRENT_DATE;

COMMENT ON VIEW v_resumen_diario IS
  'Agregado diario de anomalías. Usado por Airflow para generar el informe y enviarlo a Telegram.';


-- =============================================================================
-- VISTA 7: Histórico de anomalías agrupadas por tipo y hora (para gráficos)
-- Usada por: Gremio de Reportes (Plotly — gráfico de barras / líneas de tiempo)
-- =============================================================================
CREATE OR REPLACE VIEW v_anomalias_por_tipo AS
SELECT
    date_trunc('hour', created_at)  AS hora,
    category,
    sub_type,
    COUNT(*)                         AS num_alertas
FROM anomalias
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY date_trunc('hour', created_at), category, sub_type
ORDER BY hora DESC, num_alertas DESC;

COMMENT ON VIEW v_anomalias_por_tipo IS
  'Serie temporal de anomalías agrupadas por hora y tipo. Última semana. Ideal para Plotly.';


-- =============================================================================
-- VISTA 8: Detalle completo de un mercado (útil para endpoint GET /market/:condition_id)
-- Usada por: API HTTP del gremio de Reportes
-- =============================================================================
CREATE OR REPLACE VIEW v_mercado_detalle AS
SELECT
    mm.condition_id,
    mm.id              AS market_id,
    mm.slug,
    mm.question,
    mm.image,
    mm.liquidity,
    mm.volume,
    mm.volume_24h,
    mm.volume_1w,
    mm.volume_1mo,
    mm.volume_1yr,
    mm.event_id,
    e.question         AS event_question,
    e.slug             AS event_slug,
    -- Tokens YES/NO
    jsonb_agg(
        jsonb_build_object(
            'asset_id',     ot.asset_id,
            'outcome',      ot.outcome_name,
            'price',        ot.price,
            'avg_24h',      mds.avg_price_24h,
            'sentiment',    mds.main_sentiment_24h,
            'last_stable',  mds.last_stable_price,
            'hist_vol_5min', mds.historical_vol_5min_avg
        )
        ORDER BY ot.outcome_name
    )                  AS tokens,
    -- Anomalías recientes en este mercado
    (
        SELECT COUNT(*) FROM anomalias a
        WHERE a.payload->>'slug' = mm.slug
          AND a.created_at >= CURRENT_DATE
    )                  AS anomalias_hoy
FROM mercados_master mm
JOIN eventos e ON e.event_id = mm.event_id
LEFT JOIN outcome_tokens ot  ON ot.condition_id = mm.condition_id
LEFT JOIN market_daily_stats mds
    ON mds.asset_id = ot.asset_id AND mds.calc_date = CURRENT_DATE
GROUP BY
    mm.condition_id, mm.id, mm.slug, mm.question, mm.image,
    mm.liquidity, mm.volume, mm.volume_24h, mm.volume_1w, mm.volume_1mo, mm.volume_1yr,
    mm.event_id, e.question, e.slug;

COMMENT ON VIEW v_mercado_detalle IS
  'Vista completa de cada mercado: datos maestros + tokens YES/NO + stats batch + anomalías hoy.';
