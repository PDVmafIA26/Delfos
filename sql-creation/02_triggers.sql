-- =============================================================================
-- PROYECTO DELFOS — CAPA ORO
-- 02_triggers.sql : Disparadores automáticos de detección de anomalías
--
-- DISPARADORES DEFINIDOS:
--   1. trg_detect_flip       → FLIP de probabilidad (cruza barrera 0.5)
--   2. trg_detect_price_var  → Variación de precio > 20% vs batch anterior
--   3. trg_detect_spike      → Pico de volumen > 5x media histórica en 5 min
--   4. trg_detect_whale      → Ballena: wallet con gran impacto en el mercado
--   5. trg_detect_flash_acc  → Cuenta Flash: nueva (<48h) con apuesta anómala
--
-- SISTEMA DE NOTIFICACIÓN:
--   Cada disparador inserta en `anomalias` y hace pg_notify('anomaly_channel', payload::text)
--   El listener Python (notify_listener.py) recibe el NOTIFY y hace POST HTTP a Reportes.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_emit_anomaly(
    p_category TEXT,
    p_sub_type TEXT,
    p_payload  JSONB
) RETURNS UUID AS $$
DECLARE
    v_alert_id UUID;
    v_full_payload JSONB;
    v_notifications_enabled TEXT;
BEGIN
    SELECT value INTO v_notifications_enabled FROM config WHERE key = 'notifications_enabled';

    -- Insertar en la tabla de anomalías
    INSERT INTO anomalias (category, sub_type, payload)
    VALUES (p_category, p_sub_type, p_payload)
    RETURNING alert_id INTO v_alert_id;

    -- Construir el payload completo con metadatos para Reportes
    v_full_payload := jsonb_build_object(
        'alert_id',  v_alert_id,
        'category',  p_category,
        'sub_type',  p_sub_type,
        'payload',   p_payload,
        'timestamp', to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    );

    -- Emitir notificación asíncrona
    IF v_notifications_enabled IS DISTINCT FROM 'false' THEN
        PERFORM pg_notify('anomaly_channel', v_full_payload::TEXT);
    END IF;

    RETURN v_alert_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION fn_emit_anomaly IS
  'Inserta en anomalias y emite pg_notify para que el listener Python haga el POST HTTP a Reportes.';


-- =============================================================================
-- DISPARADOR 1: FLIP DE PROBABILIDAD
-- Se activa: INSERT en last_trade_price
-- Lógica: si el sentimiento actual (precio >= 0.5 → SI, < 0.5 → NO) difiere
--         del sentimiento histórico del batch → FLIP detectado
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_detect_flip()
RETURNS TRIGGER AS $$
DECLARE
    v_historical    market_daily_stats%ROWTYPE;
    v_outcome       outcome_tokens%ROWTYPE;
    v_market        mercados_master%ROWTYPE;
    v_current_sent  TEXT;
    v_hist_sent     TEXT;
    v_slug          TEXT;
    v_question      TEXT;
    v_change        TEXT;
    v_payload       JSONB;
BEGIN
    -- Obtener el outcome_token asociado a este trade
    SELECT * INTO v_outcome FROM outcome_tokens WHERE asset_id = NEW.asset_id;
    IF NOT FOUND THEN RETURN NEW; END IF;

    -- Obtener estadísticas históricas del batch más reciente para este asset
    SELECT * INTO v_historical
    FROM market_daily_stats
    WHERE asset_id = NEW.asset_id
    ORDER BY calc_date DESC
    LIMIT 1;

    -- Sin datos históricos no podemos comparar
    IF NOT FOUND OR v_historical.avg_price_24h IS NULL THEN RETURN NEW; END IF;

    -- Calcular sentimientos
    v_current_sent := CASE WHEN NEW.price >= 0.5 THEN 'SI' ELSE 'NO' END;
    v_hist_sent    := v_historical.main_sentiment_24h;

    -- ¿Ha cruzado la barrera del 0.5?
    IF v_current_sent = v_hist_sent THEN RETURN NEW; END IF;

    -- Obtener datos del mercado para el payload
    SELECT mm.slug, mm.question INTO v_slug, v_question
    FROM mercados_master mm
    WHERE mm.condition_id = v_outcome.condition_id
    LIMIT 1;

    v_change := CASE v_hist_sent || '_TO_' || v_current_sent
                    WHEN 'NO_TO_SI' THEN 'NO_TO_YES'
                    WHEN 'SI_TO_NO' THEN 'YES_TO_NO'
                    ELSE v_hist_sent || '_TO_' || v_current_sent
                END;

    v_payload := jsonb_build_object(
        'question',      v_question,
        'slug',          v_slug,
        'asset_id',      NEW.asset_id,
        'change',        v_change,
        'actual_price',  NEW.price,
        'batch_price',   v_historical.avg_price_24h,
        'calc_date',     v_historical.calc_date
    );

    PERFORM fn_emit_anomaly('MARKET', 'FLIP', v_payload);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_detect_flip ON last_trade_price;
CREATE TRIGGER trg_detect_flip
    AFTER INSERT ON last_trade_price
    FOR EACH ROW
    EXECUTE FUNCTION fn_detect_flip();

COMMENT ON FUNCTION fn_detect_flip IS
  'FLIP: detecta cuando un mercado cruza la barrera 0.5 (cambia de opinión mayoritaria).';


-- =============================================================================
-- DISPARADOR 2: VARIACIÓN DE PRECIO > 20%
-- Se activa: INSERT en last_trade_price
-- Lógica: ∆% = |precio_actual - last_stable_price| / last_stable_price * 100
--         Si ∆% > umbral (defecto 20%) → PRICE_VAR
--         is_pre_flip = true si el precio actual está cerca del límite 0.5
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_detect_price_var()
RETURNS TRIGGER AS $$
DECLARE
    v_historical     market_daily_stats%ROWTYPE;
    v_outcome        outcome_tokens%ROWTYPE;
    v_market         mercados_master%ROWTYPE;
    v_threshold      NUMERIC;
    v_delta_pct      NUMERIC;
    v_is_pre_flip    BOOLEAN;
    v_slug           TEXT;
    v_question       TEXT;
    v_payload        JSONB;
BEGIN
    -- Umbral configurable (defecto 20%)
    SELECT value::NUMERIC INTO v_threshold FROM config WHERE key = 'price_var_threshold';
    v_threshold := COALESCE(v_threshold, 0.20);

    -- Datos históricos
    SELECT * INTO v_historical
    FROM market_daily_stats
    WHERE asset_id = NEW.asset_id
    ORDER BY calc_date DESC
    LIMIT 1;

    IF NOT FOUND OR v_historical.last_stable_price IS NULL
                 OR v_historical.last_stable_price = 0 THEN
        RETURN NEW;
    END IF;

    -- Calcular variación porcentual
    v_delta_pct := ABS((NEW.price - v_historical.last_stable_price) / v_historical.last_stable_price) * 100;

    -- ¿Supera el umbral?
    IF v_delta_pct <= (v_threshold * 100) THEN RETURN NEW; END IF;

    -- Determinar si es pre-flip (precio entre 0.40 y 0.60, zona peligrosa)
    v_is_pre_flip := (NEW.price BETWEEN 0.40 AND 0.60);

    -- Datos del mercado
    SELECT * INTO v_outcome FROM outcome_tokens WHERE asset_id = NEW.asset_id;
    SELECT mm.slug, mm.question INTO v_slug, v_question
    FROM mercados_master mm
    WHERE mm.condition_id = v_outcome.condition_id LIMIT 1;

    v_payload := jsonb_build_object(
        'question',          v_question,
        'slug',              v_slug,
        'asset_id',          NEW.asset_id,
        'actual_price',      NEW.price,
        'last_stable_price', v_historical.last_stable_price,
        'variation_pct',     ROUND(v_delta_pct, 2),
        'is_pre_flip',       v_is_pre_flip,
        'calc_date',         v_historical.calc_date
    );

    PERFORM fn_emit_anomaly('MARKET', 'PRICE_VAR', v_payload);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_detect_price_var ON last_trade_price;
CREATE TRIGGER trg_detect_price_var
    AFTER INSERT ON last_trade_price
    FOR EACH ROW
    EXECUTE FUNCTION fn_detect_price_var();

COMMENT ON FUNCTION fn_detect_price_var IS
  'PRICE_VAR: variación brusca de precio > umbral (por defecto 20%) vs batch anterior.';


-- =============================================================================
-- DISPARADOR 3: PICO DE VOLUMEN (SPIKE)
-- Se activa: INSERT en last_trade_price
-- Lógica: suma el volumen (size) de los últimos 5 minutos para ese asset
--         Ratio = volumen_streaming_5min / historical_vol_5min_avg
--         Si Ratio > 5 → SPIKE
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_detect_spike()
RETURNS TRIGGER AS $$
DECLARE
    v_historical      market_daily_stats%ROWTYPE;
    v_outcome         outcome_tokens%ROWTYPE;
    v_market          mercados_master%ROWTYPE;
    v_vol_5min        NUMERIC;
    v_ratio           NUMERIC;
    v_ratio_threshold NUMERIC;
    v_slug            TEXT;
    v_question        TEXT;
    v_payload         JSONB;
BEGIN
    -- Umbral de ratio configurable (defecto x5)
    SELECT value::NUMERIC INTO v_ratio_threshold FROM config WHERE key = 'spike_ratio_threshold';
    v_ratio_threshold := COALESCE(v_ratio_threshold, 5.0);

    -- Datos históricos del batch
    SELECT * INTO v_historical
    FROM market_daily_stats
    WHERE asset_id = NEW.asset_id
    ORDER BY calc_date DESC
    LIMIT 1;

    IF NOT FOUND OR v_historical.historical_vol_5min_avg IS NULL
                 OR v_historical.historical_vol_5min_avg = 0 THEN
        RETURN NEW;
    END IF;

    -- Volumen acumulado en los últimos 5 minutos (ventana deslizante)
    SELECT COALESCE(SUM(size), 0) INTO v_vol_5min
    FROM last_trade_price
    WHERE asset_id  = NEW.asset_id
      AND traded_at >= NOW() - INTERVAL '5 minutes';

    -- Incluir el trade actual (que aún no está en la tabla al ejecutarse AFTER)
    v_vol_5min := v_vol_5min + NEW.size;

    -- Calcular ratio
    v_ratio := v_vol_5min / v_historical.historical_vol_5min_avg;

    IF v_ratio <= v_ratio_threshold THEN RETURN NEW; END IF;

    -- Datos del mercado
    SELECT * INTO v_outcome FROM outcome_tokens WHERE asset_id = NEW.asset_id;
    SELECT mm.slug, mm.question INTO v_slug, v_question
    FROM mercados_master mm
    WHERE mm.condition_id = v_outcome.condition_id LIMIT 1;

    v_payload := jsonb_build_object(
        'question',     v_question,
        'slug',         v_slug,
        'asset_id',     NEW.asset_id,
        'ratio',        ROUND(v_ratio, 2),
        'usd_detected', ROUND(v_vol_5min, 2),
        'hist_avg_5min', ROUND(v_historical.historical_vol_5min_avg, 2),
        'window_start', to_char(NOW() - INTERVAL '5 minutes', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    );

    PERFORM fn_emit_anomaly('MARKET', 'SPIKE', v_payload);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_detect_spike ON last_trade_price;
CREATE TRIGGER trg_detect_spike
    AFTER INSERT ON last_trade_price
    FOR EACH ROW
    EXECUTE FUNCTION fn_detect_spike();

COMMENT ON FUNCTION fn_detect_spike IS
  'SPIKE: pico de volumen en ventana 5min > 5x la media histórica (posible insider trading).';


-- =============================================================================
-- DISPARADOR 4: BALLENA (WHALE_MOVE)
-- Se activa: INSERT en trade_sospechosos
-- Lógica: un wallet con posición masiva (> $50k) y alto PnL histórico
--         impacto_estimado = exposición_usuario / liquidez_total * 100
--         Si impacto > 2% o volumen > $50k con PnL "HIGH" → WHALE
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_detect_whale()
RETURNS TRIGGER AS $$
DECLARE
    v_user_stats       user_stats_batch%ROWTYPE;
    v_usuario          usuarios%ROWTYPE;
    v_market_liquidity NUMERIC;
    v_impact_pct       NUMERIC;
    v_usd_threshold    NUMERIC;
    v_impact_threshold NUMERIC;
    v_outcome          outcome_tokens%ROWTYPE;
    v_market           mercados_master%ROWTYPE;
    v_payload          JSONB;
BEGIN
    IF NEW.wallet_address IS NULL OR NEW.realized_pnl IS NULL THEN RETURN NEW; END IF;

    -- Umbrales configurables
    SELECT value::NUMERIC INTO v_usd_threshold    FROM config WHERE key = 'whale_usd_threshold';
    SELECT value::NUMERIC INTO v_impact_threshold FROM config WHERE key = 'whale_impact_pct';
    v_usd_threshold    := COALESCE(v_usd_threshold, 50000);
    v_impact_threshold := COALESCE(v_impact_threshold, 2.0);

    -- Perfil histórico del usuario
    SELECT * INTO v_user_stats FROM user_stats_batch WHERE wallet_address = NEW.wallet_address;
    SELECT * INTO v_usuario    FROM usuarios           WHERE wallet_address = NEW.wallet_address;

    -- Calcular impacto en el mercado
    IF NEW.asset_id IS NOT NULL THEN
        SELECT * INTO v_outcome FROM outcome_tokens WHERE asset_id = NEW.asset_id;
        SELECT mm.liquidity INTO v_market_liquidity
        FROM mercados_master mm
        WHERE mm.condition_id = v_outcome.condition_id LIMIT 1;
    END IF;

    v_market_liquidity := COALESCE(v_market_liquidity, 1);  -- Evitar división por cero
    v_impact_pct := (ABS(NEW.realized_pnl) / NULLIF(v_market_liquidity, 0)) * 100;

    -- Reglas de detección
    IF NOT (
        ABS(NEW.realized_pnl) > v_usd_threshold
        OR v_impact_pct > v_impact_threshold
        OR (v_user_stats.historical_pnl_level = 'HIGH' AND ABS(NEW.realized_pnl) > v_usd_threshold / 2)
    ) THEN
        RETURN NEW;
    END IF;

    -- Marcar wallet como sospechosa
    UPDATE usuarios SET es_sospechoso = TRUE, updated_at = NOW()
    WHERE wallet_address = NEW.wallet_address;

    v_payload := jsonb_build_object(
        'user_id',            NEW.wallet_address,
        'market_title',       NEW.market_title,
        'asset_id',           NEW.asset_id,
        'realized_pnl',       ROUND(NEW.realized_pnl, 2),
        'market_impact_pct',  ROUND(COALESCE(v_impact_pct, 0), 2),
        'historical_pnl',     COALESCE(v_user_stats.historical_pnl_level, 'UNKNOWN'),
        'avg_bet_size',       COALESCE(v_user_stats.avg_bet_size, 0),
        'position_type',      NEW.status,
        'market_liquidity',   ROUND(v_market_liquidity, 2)
    );

    PERFORM fn_emit_anomaly('USER', 'WHALE_MOVE', v_payload);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_detect_whale ON trade_sospechosos;
CREATE TRIGGER trg_detect_whale
    AFTER INSERT ON trade_sospechosos
    FOR EACH ROW
    EXECUTE FUNCTION fn_detect_whale();

COMMENT ON FUNCTION fn_detect_whale IS
  'WHALE_MOVE: detecta wallets con impacto masivo en la liquidez del mercado (posible smart money).';


-- =============================================================================
-- DISPARADOR 5: CUENTA FLASH (FLASH_ACC)
-- Se activa: INSERT en trade_sospechosos
-- Lógica: cuenta con menos de 48h de antigüedad que apuesta más de lo normal
--         Regla: (NOW - first_seen_at < 48h) AND (apuesta > global_avg_new_user_volume)
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_detect_flash_acc()
RETURNS TRIGGER AS $$
DECLARE
    v_usuario         usuarios%ROWTYPE;
    v_user_stats      user_stats_batch%ROWTYPE;
    v_age_hours       NUMERIC;
    v_age_minutes     NUMERIC;
    v_flash_hours     NUMERIC;
    v_avg_new_vol     NUMERIC;
    v_ratio_vs_normal NUMERIC;
    v_payload         JSONB;
BEGIN
    IF NEW.wallet_address IS NULL THEN RETURN NEW; END IF;

    -- Umbral de horas configurable (defecto 48h)
    SELECT value::NUMERIC INTO v_flash_hours FROM config WHERE key = 'flash_hours';
    v_flash_hours := COALESCE(v_flash_hours, 48);

    -- Datos del usuario
    SELECT * INTO v_usuario   FROM usuarios        WHERE wallet_address = NEW.wallet_address;
    SELECT * INTO v_user_stats FROM user_stats_batch WHERE wallet_address = NEW.wallet_address;

    IF NOT FOUND OR v_usuario.first_seen_at IS NULL THEN RETURN NEW; END IF;

    -- Calcular antigüedad de la cuenta
    v_age_hours   := EXTRACT(EPOCH FROM (NOW() - v_usuario.first_seen_at)) / 3600;
    v_age_minutes := v_age_hours * 60;

    -- ¿La cuenta es suficientemente nueva?
    IF v_age_hours >= v_flash_hours THEN RETURN NEW; END IF;

    -- Volumen de referencia para cuentas nuevas
    v_avg_new_vol := COALESCE(v_user_stats.global_avg_new_user_volume, 50.0);

    -- ¿La apuesta supera la media de nuevos usuarios?
    IF ABS(NEW.realized_pnl) <= v_avg_new_vol THEN RETURN NEW; END IF;

    v_ratio_vs_normal := ROUND(ABS(NEW.realized_pnl) / NULLIF(v_avg_new_vol, 0), 1);

    -- Marcar como sospechosa
    UPDATE usuarios SET es_sospechoso = TRUE, updated_at = NOW()
    WHERE wallet_address = NEW.wallet_address;

    v_payload := jsonb_build_object(
        'user_id',          NEW.wallet_address,
        'age_hours',        ROUND(v_age_hours, 2),
        'age_minutes',      ROUND(v_age_minutes, 0),
        'suspicious_volume', ROUND(ABS(NEW.realized_pnl), 2),
        'avg_new_user_vol', ROUND(v_avg_new_vol, 2),
        'ratio_vs_normal',  v_ratio_vs_normal,
        'market_title',     NEW.market_title,
        'asset_id',         NEW.asset_id,
        'first_seen_at',    to_char(v_usuario.first_seen_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    );

    PERFORM fn_emit_anomaly('USER', 'FLASH_ACC', v_payload);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_detect_flash_acc ON trade_sospechosos;
CREATE TRIGGER trg_detect_flash_acc
    AFTER INSERT ON trade_sospechosos
    FOR EACH ROW
    EXECUTE FUNCTION fn_detect_flash_acc();

COMMENT ON FUNCTION fn_detect_flash_acc IS
  'FLASH_ACC: cuenta con < 48h de vida que apuesta mucho más de la media (posible cuenta pantalla).';


-- Inserta o actualiza market_daily_stats usando last_trade_price

CREATE OR REPLACE FUNCTION refresh_market_daily_stats()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO market_daily_stats (
        asset_id,
        calc_date,
        avg_price_24h,
        last_stable_price,
        updated_at
    )
    SELECT
        ltp.asset_id,
        CURRENT_DATE,
        
        -- Media de precios últimas 24h
        AVG(ltp.price)::NUMERIC(10,6) AS avg_price_24h,

        -- Último precio registrado
        (
            SELECT ltp2.price
            FROM last_trade_price ltp2
            WHERE ltp2.asset_id = ltp.asset_id
            ORDER BY ltp2.traded_at DESC
            LIMIT 1
        )::NUMERIC(10,6) AS last_stable_price,

        NOW()

    FROM last_trade_price ltp
    WHERE ltp.traded_at >= NOW() - INTERVAL '24 hours'
    GROUP BY ltp.asset_id

    ON CONFLICT (asset_id, calc_date)
    DO UPDATE SET
        avg_price_24h     = EXCLUDED.avg_price_24h,
        last_stable_price = EXCLUDED.last_stable_price,
        updated_at        = NOW();

END;
$$;


-- Inserta o actualiza user_stats_batch usando trade_sospechosos

CREATE OR REPLACE FUNCTION refresh_user_stats_batch()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN

    INSERT INTO user_stats_batch (
        wallet_address,
        historical_pnl_level,
        avg_bet_size,
        updated_at
    )
    SELECT
        ts.wallet_address,

        -- Clasificación según media de realized_pnl
        CASE
            WHEN AVG(ts.realized_pnl) >= 10000 THEN 'HIGH'
            WHEN AVG(ts.realized_pnl) >= 1000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS historical_pnl_level,

        AVG(ts.realized_pnl)::NUMERIC(20,4) AS avg_bet_size,

        NOW()

    FROM trade_sospechosos ts
    WHERE ts.wallet_address IS NOT NULL
    GROUP BY ts.wallet_address

    ON CONFLICT (wallet_address)
    DO UPDATE SET
        historical_pnl_level = EXCLUDED.historical_pnl_level,
        avg_bet_size         = EXCLUDED.avg_bet_size,
        updated_at           = NOW();

END;
$$;
-- =============================================================================
-- DISPARADOR EXTRA: Actualizar updated_at automáticamente en tablas clave
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar a tablas que tienen updated_at
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['eventos', 'mercados_master', 'outcome_tokens', 'usuarios', 'user_stats_batch', 'market_daily_stats'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_set_updated_at ON %I', t);
        EXECUTE format(
            'CREATE TRIGGER trg_set_updated_at BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()',
            t
        );
    END LOOP;
END $$;