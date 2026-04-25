-- Crear tabla para anomalías detectadas por Spark
CREATE TABLE IF NOT EXISTS anomalias_detectadas (
    id SERIAL PRIMARY KEY,
    titulo TEXT,
    precio DOUBLE PRECISION,
    precio_medio DOUBLE PRECISION,
    nombre_ballena TEXT,
    volumen DOUBLE PRECISION,
    timestamp_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);