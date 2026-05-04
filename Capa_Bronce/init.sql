-- Eliminar tablas si existen para evitar conflictos al reiniciar
DROP TABLE IF EXISTS ANOMALIAS;
DROP TABLE IF EXISTS TRADE_SOSPECHOSOS;
DROP TABLE IF EXISTS LAST_TRADE_PRICE;
DROP TABLE IF EXISTS TOP_WALLETS;
DROP TABLE IF EXISTS OUTCOME_TOKENS;
DROP TABLE IF EXISTS USUARIOS;
DROP TABLE IF EXISTS MERCADOS_MASTER;
DROP TABLE IF EXISTS EVENTOS;

-- 1. Tabla EVENTOS
CREATE TABLE EVENTOS (
    eventId VARCHAR(255) PRIMARY KEY,
    question TEXT,
    slug VARCHAR(255),
    image TEXT,
    active BOOLEAN,
    liquidity DECIMAL(20, 2),
    volume DECIMAL(20, 2),
    volume24h DECIMAL(20, 2),
    volume1w DECIMAL(20, 2),
    volume1mo DECIMAL(20, 2),
    volume1yr DECIMAL(20, 2)
);

-- 2. Tabla MERCADOS_MASTER
CREATE TABLE MERCADOS_MASTER (
    id VARCHAR(255),
    conditionId VARCHAR(255),
    slug VARCHAR(255),
    question TEXT,
    image TEXT,
    liquidity DECIMAL(20, 2),
    volume DECIMAL(20, 2),
    volume24h DECIMAL(20, 2),
    volume1w DECIMAL(20, 2),
    volume1mo DECIMAL(20, 2),
    volume1yr DECIMAL(20, 2),
    eventId VARCHAR(255),
    PRIMARY KEY (id, conditionId),
    FOREIGN KEY (eventId) REFERENCES EVENTOS(eventId)
);

-- 3. Tabla USUARIOS
CREATE TABLE USUARIOS (
    wallet_address VARCHAR(255) PRIMARY KEY,
    total_won DECIMAL(20, 2),
    total_lost DECIMAL(20, 2),
    net_pnl DECIMAL(20, 2),
    total_position DECIMAL(20, 2),
    es_sospechoso BOOLEAN DEFAULT FALSE
);

-- 4. Tabla OUTCOME_TOKENS
CREATE TABLE OUTCOME_TOKENS (
    asset_id VARCHAR(255) PRIMARY KEY,
    condition_id VARCHAR(255),
    outcome_name VARCHAR(255),
    price DECIMAL(10, 6),
    size DECIMAL(20, 2)
);

-- 5. Tabla TOP_WALLETS (Relaciona carteras con tokens específicos)
CREATE TABLE TOP_WALLETS (
    wallet_address VARCHAR(255),
    conditionId VARCHAR(255),
    asset_id VARCHAR(255),
    amount DECIMAL(20, 2),
    PRIMARY KEY (wallet_address, conditionId),
    FOREIGN KEY (asset_id) REFERENCES OUTCOME_TOKENS(asset_id)
);

-- 6. Tabla LAST_TRADE_PRICE
CREATE TABLE LAST_TRADE_PRICE (
    idAutoIncremental SERIAL PRIMARY KEY,
    asset_id VARCHAR(255),
    price DECIMAL(10, 6),
    size DECIMAL(20, 2),
    FOREIGN KEY (asset_id) REFERENCES OUTCOME_TOKENS(asset_id)
);

-- 7. Tabla TRADE_SOSPECHOSOS
CREATE TABLE TRADE_SOSPECHOSOS (
    id_autoincremental SERIAL PRIMARY KEY,
    market_title TEXT,
    asset_id VARCHAR(255),
    status VARCHAR(50),
    realized_pnl DECIMAL(20, 2),
    wallet_address VARCHAR(255),
    FOREIGN KEY (asset_id) REFERENCES OUTCOME_TOKENS(asset_id),
    FOREIGN KEY (wallet_address) REFERENCES USUARIOS(wallet_address)
);

-- 8. Tabla ANOMALIAS
CREATE TABLE ANOMALIAS (
    alert_id SERIAL PRIMARY KEY,
    category VARCHAR(100),
    sub_type VARCHAR(100), -- Corregido de sub_tpye segun diagrama
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);