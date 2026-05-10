-- =============================================================================
-- PROYECTO DELFOS — CAPA ORO
-- Definición de tablas (basado en diagrama E-R aprobado en clase)
-- Autor: Capa Oro
-- =============================================================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- Para UUID en anomalías
-- pg_net no está disponible en postgres:16-alpine y no es necesaria:
-- los triggers usan pg_notify (nativo de PostgreSQL), no HTTP directo.
-- CREATE EXTENSION IF NOT EXISTS "pg_net";


-- Eventos de Polymarket
CREATE TABLE IF NOT EXISTS eventos (
    event_id    TEXT PRIMARY KEY,
    question    TEXT,
    slug        TEXT,
    image       TEXT,
    active      BOOLEAN      DEFAULT TRUE,
    liquidity   NUMERIC(20,4) DEFAULT 0,
    volume      NUMERIC(20,4) DEFAULT 0,
    volume_24h  NUMERIC(20,4) DEFAULT 0,
    volume_1w   NUMERIC(20,4) DEFAULT 0,
    volume_1mo  NUMERIC(20,4) DEFAULT 0,
    volume_1yr  NUMERIC(20,4) DEFAULT 0,
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE eventos IS 'Eventos de Polymarket: un evento contiene uno o varios mercados.';

-- Mercados individuales
CREATE TABLE IF NOT EXISTS mercados_master (
    id           TEXT         NOT NULL,
    condition_id TEXT         NOT NULL,
    slug         TEXT,
    question     TEXT,
    image        TEXT,
    liquidity    NUMERIC(20,4) DEFAULT 0,
    volume       NUMERIC(20,4) DEFAULT 0,
    volume_24h   NUMERIC(20,4) DEFAULT 0,
    volume_1w    NUMERIC(20,4) DEFAULT 0,
    volume_1mo   NUMERIC(20,4) DEFAULT 0,
    volume_1yr   NUMERIC(20,4) DEFAULT 0,
    event_id     TEXT         REFERENCES eventos(event_id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (id, condition_id)
);

-- Índice para búsquedas por condition_id (FK desde outcome_tokens)
CREATE UNIQUE INDEX IF NOT EXISTS uq_mercados_condition_id ON mercados_master(condition_id);

COMMENT ON TABLE mercados_master IS 'Cada fila es un mercado/pregunta binaria dentro de un evento.';

-- Tokens de resultado
CREATE TABLE IF NOT EXISTS outcome_tokens (
    asset_id     TEXT PRIMARY KEY,
    condition_id TEXT         REFERENCES mercados_master(condition_id) ON DELETE CASCADE,
    outcome_name TEXT,
    price        NUMERIC(10,6),
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE outcome_tokens IS 'Tokens negociables por mercado. Price = probabilidad implícita (0-1). SIN size (last_trade_price).';

-- Último precio ejecutado por trade
CREATE TABLE IF NOT EXISTS last_trade_price (
    id         BIGSERIAL    PRIMARY KEY,
    asset_id   TEXT         NOT NULL REFERENCES outcome_tokens(asset_id) ON DELETE CASCADE,
    price      NUMERIC(10,6) NOT NULL,
    size       NUMERIC(20,4) NOT NULL,
    traded_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ltp_asset_time ON last_trade_price(asset_id, traded_at DESC);

COMMENT ON TABLE last_trade_price IS 'Serie histórica de trades. size = volumen en USD del trade individual.';

-- Usuarios / wallets
CREATE TABLE IF NOT EXISTS usuarios (
    wallet_address TEXT PRIMARY KEY,
    total_won      NUMERIC(20,4) DEFAULT 0,
    total_lost     NUMERIC(20,4) DEFAULT 0,
    net_pnl        NUMERIC(20,4) DEFAULT 0,
    total_position NUMERIC(20,4) DEFAULT 0,
    es_sospechoso  BOOLEAN       DEFAULT FALSE,
    first_seen_at  TIMESTAMPTZ   DEFAULT NOW(),
    created_at     TIMESTAMPTZ   DEFAULT NOW(),
    updated_at     TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE usuarios_staging AS TABLE usuarios WITH NO DATA;

COMMENT ON TABLE usuarios IS 'Wallets de Polymarket. first_seen_at: primera vez que la API detectó la cuenta.';

-- Top wallets por mercado
CREATE TABLE IF NOT EXISTS top_wallets (
    wallet_address TEXT         NOT NULL REFERENCES usuarios(wallet_address) ON DELETE CASCADE,
    condition_id   TEXT         NOT NULL REFERENCES mercados_master(condition_id) ON DELETE CASCADE,
    asset_id       TEXT         REFERENCES outcome_tokens(asset_id) ON DELETE SET NULL,
    amount         NUMERIC(20,4),
    recorded_at    TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (wallet_address, condition_id)
);

COMMENT ON TABLE top_wallets IS 'Posiciones grandes de wallets en mercados específicos.';

-- Trades sospechosos
CREATE TABLE IF NOT EXISTS trade_sospechosos (
    id             BIGSERIAL PRIMARY KEY,
    market_title   TEXT,
    asset_id       TEXT,
    status         TEXT,
    realized_pnl   NUMERIC(20,4),
    icon           TEXT,
    outcome        TEXT,
    wallet_address TEXT REFERENCES usuarios(wallet_address) ON DELETE SET NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    notified       BOOLEAN,

    CONSTRAINT uq_trade_sospechosos_wallet_asset UNIQUE (wallet_address, asset_id)
);

COMMENT ON TABLE trade_sospechosos IS 'Trades marcados como potencialmente anómalos por Spark.';

CREATE TABLE IF NOT EXISTS anomalias (
    alert_id   UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    category   TEXT         NOT NULL CHECK (category IN ('MARKET', 'USER')),
    sub_type   TEXT         NOT NULL CHECK (sub_type IN ('FLIP', 'SPIKE', 'WHALE_MOVE', 'PRICE_VAR', 'FLASH_ACC')),
    payload    JSONB        NOT NULL,
    created_at TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalias_created   ON anomalias(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalias_category  ON anomalias(category, sub_type);
CREATE INDEX IF NOT EXISTS idx_anomalias_payload   ON anomalias USING GIN (payload);

COMMENT ON TABLE anomalias IS
  'Tabla central de alertas. category: MARKET|USER. '
  'sub_type: FLIP|SPIKE|WHALE_MOVE|PRICE_VAR|FLASH_ACC. '
  'payload: JSON con los campos específicos de cada alerta.';

-- Estadísticas históricas de mercado calculadas por Spark Batch
CREATE TABLE IF NOT EXISTS market_daily_stats (
    asset_id                  TEXT         NOT NULL REFERENCES outcome_tokens(asset_id) ON DELETE CASCADE,
    calc_date                 DATE         NOT NULL DEFAULT CURRENT_DATE,
    avg_price_24h             NUMERIC(10,6),
    main_sentiment_24h        TEXT GENERATED ALWAYS AS (
                                  CASE WHEN avg_price_24h >= 0.5 THEN 'SI' ELSE 'NO' END
                              ) STORED,
    last_stable_price         NUMERIC(10,6),
    updated_at                TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (asset_id, calc_date)
);

COMMENT ON TABLE market_daily_stats IS
  'Estadísticas históricas por asset calculadas por Spark Batch. '
  'main_sentiment_24h se genera automáticamente desde avg_price_24h.';

-- Estadísticas históricas de usuarios calculadas por Spark Batch
CREATE TABLE IF NOT EXISTS user_stats_batch (
    wallet_address              TEXT PRIMARY KEY REFERENCES usuarios(wallet_address) ON DELETE CASCADE,
    historical_pnl_level        TEXT CHECK (historical_pnl_level IN ('HIGH', 'MEDIUM', 'LOW')),
    avg_bet_size                NUMERIC(20,4),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE user_stats_batch IS
  'Perfil histórico de usuarios calculado por Spark Batch. '
  'global_avg_new_user_volume: cuánto suele apostar un usuario nuevo normal (~$50).';


CREATE TABLE IF NOT EXISTS config (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO config (key, value) VALUES
    ('reporting_url',         'http://notifications:8000/notify'),
    ('flip_threshold',        '0.5'),
    ('spike_ratio_threshold', '5.0'),
    ('price_var_threshold',   '0.20'),
    ('whale_usd_threshold',   '50000'),
    ('whale_impact_pct',      '2.0'),
    ('flash_hours',           '48'),
    ('notifications_enabled', 'true')
ON CONFLICT (key) DO NOTHING;

COMMENT ON TABLE config IS 'Parámetros ajustables del sistema de detección.';