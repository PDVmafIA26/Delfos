-- =============================================================================
-- PROYECTO DELFOS — CAPA ORO
-- 04_test_data.sql : Datos de prueba y ejemplos de llamadas HTTP / consultas
--
-- PROPÓSITO: Probar los disparadores y vistas sin necesidad de datos reales.
-- EJECUTAR SOLO EN ENTORNO DE DESARROLLO, no en producción.
-- =============================================================================

-- =============================================================================
-- 1. DATOS DE PRUEBA BASE
-- =============================================================================

-- Evento de ejemplo
INSERT INTO eventos (event_id, question, slug, active, liquidity, volume, volume_24h)
VALUES
    ('evt_001', 'Will Trump win the 2024 US Election?', 'trump-2024-election', TRUE, 500000, 2000000, 50000),
    ('evt_002', 'Will Maduro be captured by 26th April?', 'maduro-captured-april', TRUE, 100000, 300000, 15000)
ON CONFLICT DO NOTHING;

-- Mercados (uno por evento en este ejemplo)
INSERT INTO mercados_master (id, condition_id, slug, question, liquidity, volume, volume_24h, event_id)
VALUES
    ('mkt_001', 'cond_001', 'trump-yes', 'Trump wins — YES market',  450000, 1800000, 45000, 'evt_001'),
    ('mkt_002', 'cond_002', 'trump-no',  'Trump wins — NO market',   50000,  200000,  5000,  'evt_001'),
    ('mkt_003', 'cond_003', 'maduro',    'Maduro captured — YES',    100000, 300000,  15000, 'evt_002')
ON CONFLICT DO NOTHING;

-- Outcome tokens (SIN campo size — va en last_trade_price)
INSERT INTO outcome_tokens (asset_id, condition_id, outcome_name, price)
VALUES
    ('ast_001', 'cond_001', 'YES', 0.42),
    ('ast_002', 'cond_002', 'NO',  0.58),
    ('ast_003', 'cond_003', 'YES', 0.10)
ON CONFLICT DO NOTHING;

-- Usuarios
INSERT INTO usuarios (wallet_address, total_won, total_lost, net_pnl, total_position, es_sospechoso, first_seen_at)
VALUES
    ('0xABCD1234', 150000, 50000,  100000, 200000, FALSE, NOW() - INTERVAL '30 days'),
    ('0xNEWWALLET', 0,      0,      0,       0,      FALSE, NOW() - INTERVAL '2 hours'),  -- Cuenta flash candidata
    ('0xWHALE001',  500000, 100000, 400000, 1000000, FALSE, NOW() - INTERVAL '180 days')
ON CONFLICT DO NOTHING;

-- Estadísticas batch (normalmente pobladas por Spark Batch)
INSERT INTO market_daily_stats (asset_id, calc_date, avg_price_24h, last_stable_price, historical_vol_5min_avg)
VALUES
    ('ast_001', CURRENT_DATE, 0.40, 0.40, 1000),   -- Sentimiento NO (< 0.5)
    ('ast_002', CURRENT_DATE, 0.60, 0.60, 800),    -- Sentimiento SI (>= 0.5)
    ('ast_003', CURRENT_DATE, 0.08, 0.09, 500)     -- Sentimiento NO
ON CONFLICT DO NOTHING;

-- Estadísticas de usuarios (normalmente pobladas por Spark Batch)
INSERT INTO user_stats_batch (wallet_address, historical_pnl_level, avg_bet_size, global_avg_new_user_volume)
VALUES
    ('0xABCD1234',  'MEDIUM', 5000,   50),
    ('0xNEWWALLET', 'LOW',    0,      50),
    ('0xWHALE001',  'HIGH',   50000,  50)
ON CONFLICT DO NOTHING;


-- =============================================================================
-- 2. PRUEBA DEL DISPARADOR FLIP
-- Condición: precio cruza 0.5 (era 0.42 en batch → sube a 0.54)
-- Resultado esperado: anomalía FLIP insertada + pg_notify emitido
-- =============================================================================
SELECT '--- TEST FLIP: precio cruza 0.5 ---' AS test;

INSERT INTO last_trade_price (asset_id, price, size)
VALUES ('ast_001', 0.54, 500);  -- Era 0.40 en batch (NO) → ahora 0.54 (SI) = FLIP!

-- Verificar
SELECT alert_id, category, sub_type, payload, created_at
FROM anomalias
WHERE sub_type = 'FLIP'
ORDER BY created_at DESC
LIMIT 1;


-- =============================================================================
-- 3. PRUEBA DEL DISPARADOR PRICE_VAR
-- Condición: ∆% = |0.54 - 0.40| / 0.40 * 100 = 35% > 20% → PRICE_VAR
-- (Nota: este INSERT también disparará FLIP si cruza 0.5)
-- =============================================================================
SELECT '--- TEST PRICE_VAR: variación > 20% ---' AS test;

INSERT INTO last_trade_price (asset_id, price, size)
VALUES ('ast_003', 0.13, 300);  -- Era 0.09 en batch → ahora 0.13 = +44% → PRICE_VAR

SELECT alert_id, category, sub_type,
       payload->>'variation_pct' AS variation_pct,
       payload->>'is_pre_flip'   AS is_pre_flip
FROM anomalias
WHERE sub_type = 'PRICE_VAR'
ORDER BY created_at DESC
LIMIT 1;


-- =============================================================================
-- 4. PRUEBA DEL DISPARADOR SPIKE
-- Condición: volumen 5min >> 5x media histórica (historical_vol_5min_avg = 1000)
-- =============================================================================
SELECT '--- TEST SPIKE: volumen 5min > 5x batch ---' AS test;

-- Insertar varios trades grandes en los últimos 5 minutos
INSERT INTO last_trade_price (asset_id, price, size, traded_at) VALUES
    ('ast_001', 0.54, 2000, NOW() - INTERVAL '4 minutes'),
    ('ast_001', 0.55, 1500, NOW() - INTERVAL '3 minutes'),
    ('ast_001', 0.56, 1800, NOW() - INTERVAL '2 minutes'),
    ('ast_001', 0.57, 500,  NOW() - INTERVAL '1 minute');
-- Suma 5min ≈ 5800 / 1000 (avg) = ratio 5.8 → SPIKE

SELECT alert_id, category, sub_type,
       payload->>'ratio'        AS ratio,
       payload->>'usd_detected' AS usd_detected
FROM anomalias
WHERE sub_type = 'SPIKE'
ORDER BY created_at DESC
LIMIT 1;


-- =============================================================================
-- 5. PRUEBA DEL DISPARADOR WHALE_MOVE
-- Condición: PnL realizado > $50,000 y historical_pnl_level = 'HIGH'
-- =============================================================================
SELECT '--- TEST WHALE_MOVE: posición masiva ---' AS test;

INSERT INTO trade_sospechosos (market_title, asset_id, status, realized_pnl, wallet_address)
VALUES ('Trump wins — YES market', 'ast_001', 'CLOSED', 75000, '0xWHALE001');

SELECT alert_id, category, sub_type,
       payload->>'user_id'            AS wallet,
       payload->>'realized_pnl'       AS realized_pnl,
       payload->>'market_impact_pct'  AS market_impact_pct,
       payload->>'historical_pnl'     AS pnl_level
FROM anomalias
WHERE sub_type = 'WHALE_MOVE'
ORDER BY created_at DESC
LIMIT 1;


-- =============================================================================
-- 6. PRUEBA DEL DISPARADOR FLASH_ACC
-- Condición: cuenta creada hace 2h (< 48h) apuesta $8500 >> avg_new = $50
-- =============================================================================
SELECT '--- TEST FLASH_ACC: cuenta nueva con apuesta enorme ---' AS test;

INSERT INTO trade_sospechosos (market_title, asset_id, status, realized_pnl, wallet_address)
VALUES ('Maduro captured — YES', 'ast_003', 'OPEN', 8500, '0xNEWWALLET');

SELECT alert_id, category, sub_type,
       payload->>'age_minutes'       AS age_minutes,
       payload->>'suspicious_volume' AS suspicious_volume,
       payload->>'ratio_vs_normal'   AS ratio_vs_normal
FROM anomalias
WHERE sub_type = 'FLASH_ACC'
ORDER BY created_at DESC
LIMIT 1;


-- =============================================================================
-- 7. VERIFICAR TODAS LAS ANOMALÍAS GENERADAS
-- =============================================================================
SELECT '--- RESUMEN: Todas las anomalías generadas ---' AS test;

SELECT
    alert_id,
    category,
    sub_type,
    created_at,
    payload->>'question' AS market,
    payload->>'slug'     AS slug,
    payload->>'user_id'  AS wallet
FROM anomalias
ORDER BY created_at DESC;


-- =============================================================================
-- 8. CONSULTAS A LAS VISTAS (llamadas GET equivalentes)
-- =============================================================================

-- Vista 1: Anomalías recientes (GET /api/anomalias/recientes)
SELECT * FROM v_anomalias_recientes LIMIT 10;

-- Vista 2: Mercados activos (GET /api/mercados)
SELECT condition_id, question, current_price, main_sentiment_24h, volume_24h
FROM v_mercados_activos
LIMIT 5;

-- Vista 3: Candidatos a FLIP (GET /api/mercados/flip-candidates)
SELECT question, current_price, current_batch_sentiment, distance_to_flip
FROM v_flip_candidates;

-- Vista 4: Wallets sospechosas (GET /api/usuarios/sospechosos)
SELECT wallet_address, historical_pnl_level, total_alerts, alert_types
FROM v_wallets_sospechosas;

-- Vista 5: Comparativa volumen (GET /api/mercados/volume-monitor)
SELECT question, outcome_name, vol_5min_live, historical_vol_5min_avg, spike_ratio, is_spike
FROM v_volumen_live_vs_batch
WHERE is_spike = TRUE;

-- Vista 6: Resumen diario (GET /api/reportes/resumen-diario)
SELECT * FROM v_resumen_diario;

-- Vista 7: Anomalías por tipo y hora — datos para gráfico Plotly
SELECT * FROM v_anomalias_por_tipo ORDER BY hora DESC LIMIT 20;

-- Vista 8: Detalle de un mercado específico (GET /api/mercados/cond_001)
SELECT condition_id, question, tokens, anomalias_hoy
FROM v_mercado_detalle
WHERE condition_id = 'cond_001';


-- =============================================================================
-- 9. CAMBIAR PARÁMETROS DE DETECCIÓN
-- =============================================================================
-- Subir el umbral de SPIKE de 5x a 8x:
UPDATE config SET value = '8.0', updated_at = NOW() WHERE key = 'spike_ratio_threshold';

-- Bajar umbral de variación de precio del 20% al 15%:
UPDATE config SET value = '0.15', updated_at = NOW() WHERE key = 'price_var_threshold';

-- Ver configuración actual:
SELECT key, value FROM config ORDER BY key;

-- Restaurar valores por defecto:
UPDATE config SET value = '5.0'  WHERE key = 'spike_ratio_threshold';
UPDATE config SET value = '0.20' WHERE key = 'price_var_threshold';
