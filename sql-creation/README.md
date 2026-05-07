# Delfos — Capa Oro 🏛️

> **Gremio:** Capa Oro / Base de Datos  
> **Rama de trabajo:** `feature/capa-oro-schema-triggers`  
> **Tecnologías:** PostgreSQL 16, Python 3.11, Docker & Docker Compose

---

## Estructura de esta carpeta

```
sql-creation/
├── 01_schema.sql                # Tablas del diagrama E-R (se ejecuta automáticamente al arrancar Docker)
├── 02_triggers.sql              # 5 disparadores de detección de anomalías (se ejecuta automáticamente)
├── 03_views.sql                 # 8 vistas para consultas de otros gremios (se ejecuta automáticamente)
├── 04_test_data.sql             # Datos de prueba — SOLO para desarrollo local
├── 05_update_reporting_url.sql  # Script puntual: cambiar URL de reportes cuando el gremio confirme
├── docker-compose.local.yml     # Stack Docker completo para pruebas locales
├── Dockerfile.mock              # Imagen del servidor de reportes simulado (mock)
├── Dockerfile.listener          # Imagen del notify_listener (pg_notify → HTTP POST)
├── mock_reporting_server.py     # FastAPI que simula el endpoint del gremio de Reportes
├── notify_listener.py           # Servicio Python: escucha pg_notify y hace POST HTTP
├── requirements.txt             # Dependencias Python del notify_listener
└── .env.local                   # Variables de entorno de ejemplo (NO subir con tokens reales)
```

---

## Arquitectura de notificaciones

```
Spark Streaming / API Polymarket
        │
        ▼  INSERT
  last_trade_price          trade_sospechosos
        │                          │
        ├── trg_detect_flip        ├── trg_detect_whale
        ├── trg_detect_price_var   └── trg_detect_flash_acc
        └── trg_detect_spike
        │
        ▼  INSERT
     anomalias (tabla central)
        │
        ▼  pg_notify('anomaly_channel', json)
  notify_listener.py  ←── escucha en tiempo real
        │
        ▼  HTTP POST
  notifications:8000/notify   ← servicio real del gremio de Reportes
  (o mock_reporting_server durante pruebas locales → :8000/anomaly)
```

---

## Tablas creadas

| Tabla | Descripción |
|---|---|
| `eventos` | Eventos de Polymarket (contiene uno o varios mercados) |
| `mercados_master` | Mercados/preguntas individuales dentro de un evento |
| `outcome_tokens` | Tokens YES/NO negociables. `price` = probabilidad implícita (0–1) |
| `last_trade_price` | Serie histórica de trades. `size` = volumen USD del trade |
| `usuarios` | Wallets de Polymarket. `first_seen_at` = primera detección |
| `top_wallets` | Posiciones grandes de wallets en mercados concretos |
| `trade_sospechosos` | Trades marcados como anómalos por Spark para inspección |
| `anomalias` | Tabla central de alertas (FLIP, SPIKE, PRICE_VAR, WHALE_MOVE, FLASH_ACC) |
| `market_daily_stats` | Estadísticas históricas por asset, calculadas por Spark Batch |
| `user_stats_batch` | Perfil histórico de usuarios calculado por Spark Batch |
| `config` | Parámetros de detección ajustables sin tocar código |

---

## Disparadores definidos

| Trigger | Se activa en | Condición | Payload al gremio de Reportes |
|---|---|---|---|
| `trg_detect_flip` | INSERT en `last_trade_price` | Precio cruza barrera 0.5 vs sentimiento batch | `question, slug, change (NO_TO_YES / YES_TO_NO), actual_price` |
| `trg_detect_price_var` | INSERT en `last_trade_price` | Δ% > 20% vs `last_stable_price` | `variation_pct, is_pre_flip, actual_price` |
| `trg_detect_spike` | INSERT en `last_trade_price` | Volumen 5min / media_batch > 5x | `ratio, usd_detected, hist_avg_5min` |
| `trg_detect_whale` | INSERT en `trade_sospechosos` | PnL > $50k o impacto > 2% de liquidez | `user_id, market_impact_pct, historical_pnl, position_type` |
| `trg_detect_flash_acc` | INSERT en `trade_sospechosos` | Cuenta < 48h con apuesta >> media | `age_minutes, suspicious_volume, ratio_vs_normal` |

> Todos los umbrales son **configurables** en la tabla `config` sin tocar el código SQL.

---

## Vistas disponibles para otros gremios

| Vista | Descripción | Quién la usa |
|---|---|---|
| `v_anomalias_recientes` | Últimas 24h con campos del JSON ya extraídos y tipados | Reportes, Telegram |
| `v_mercados_activos` | Estado actual de todos los mercados activos con tokens y stats | Ingesta, Reportes |
| `v_flip_candidates` | Mercados con precio entre 0.38–0.62 (zona pre-flip) | Análisis/Detección |
| `v_wallets_sospechosas` | Wallets marcadas + historial de alertas acumuladas | Análisis |
| `v_volumen_live_vs_batch` | Comparativa volumen 5min en vivo vs media histórica batch | Detección, Plotly |
| `v_resumen_diario` | Resumen ejecutivo del día (totales por tipo de anomalía) | Airflow DAG, Telegram |
| `v_anomalias_por_tipo` | Serie temporal por hora y tipo (últimos 7 días) | Plotly (gráfico barras) |
| `v_mercado_detalle` | Vista completa: datos maestros + tokens JSON + stats + anomalías hoy | API REST Reportes |
| `v_anomalias_con_imagen` | Anomalías con `image_key` alineado al `config.py` de Reportes | Gremio de Reportes |

> `v_anomalias_con_imagen` se crea ejecutando `05_update_reporting_url.sql`.

---

## Prueba local rápida

### 1. Levantar el stack (desde `sql-creation/`)

```powershell
docker compose -f docker-compose.local.yml up --build -d
```

Arranca 3 contenedores:
- `delfos-postgres-local` — PostgreSQL 16 con el esquema ya cargado (puerto 5432)
- `delfos-mock-reporting` — Servidor mock que simula el endpoint de Reportes (puerto 8000)
- `delfos-notify-listener-local` — Listener pg_notify → HTTP POST al mock

### 2. Verificar que todo está bien

```powershell
# Estado de los contenedores
docker ps

# Tablas creadas
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\dt"

# Triggers y vistas
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\df fn_detect_*"
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\dv v_*"
```

### 3. Ejecutar datos de prueba y disparar anomalías

```powershell
# PowerShell — usar Get-Content en lugar de < (no soportado en PowerShell)
Get-Content 04_test_data.sql | docker exec -i delfos-postgres-local psql -U postgres -d delfos
```

Genera **18 anomalías** de los 5 tipos. En los logs del listener verás:
```
🔔 Anomalía detectada | category=MARKET | sub_type=FLIP | alert_id=...
✅ Anomalía enviada a Reportes | sub_type=FLIP | status=200
```

### 4. Consultar resultados

```powershell
# Todas las anomalías generadas
docker exec delfos-postgres-local psql -U postgres -d delfos -c "SELECT category, sub_type, created_at FROM anomalias ORDER BY created_at DESC;"

# Resumen del día
docker exec delfos-postgres-local psql -U postgres -d delfos -c "SELECT * FROM v_resumen_diario;"

# Estado del mock server (nº de alertas recibidas)
Invoke-WebRequest http://localhost:8000/health | Select-Object -ExpandProperty Content
```

### 5. Parar el stack

```powershell
# Parar y conservar datos
docker compose -f docker-compose.local.yml down

# Parar y borrar también el volumen de PostgreSQL (reinicio limpio)
docker compose -f docker-compose.local.yml down -v
```

---

## Ajustar umbrales de detección (sin tocar código)

```sql
-- Ver configuración actual
SELECT key, value FROM config ORDER BY key;

-- Cambiar umbral de SPIKE (por defecto x5)
UPDATE config SET value = '8.0' WHERE key = 'spike_ratio_threshold';

-- Cambiar umbral de variación de precio (por defecto 20%)
UPDATE config SET value = '0.15' WHERE key = 'price_var_threshold';

-- Desactivar notificaciones HTTP temporalmente (sigue insertando en anomalias, no hace POST)
UPDATE config SET value = 'false' WHERE key = 'notifications_enabled';

-- Cambiar URL del servicio de Reportes cuando el gremio lo confirme
UPDATE config SET value = 'http://notifications:8000/notify' WHERE key = 'reporting_url';
```

---

## Coordinación con otros gremios

### → Gremio de Spark (Ingesta / Bronce)

Vuestros jobs de Spark deben escribir en estas tablas para activar los triggers:

| Tabla destino | Cuándo escribir | Triggers que activa |
|---|---|---|
| `last_trade_price(asset_id, price, size)` | Streaming — cada trade | FLIP, PRICE_VAR, SPIKE |
| `trade_sospechosos(market_title, asset_id, status, realized_pnl, wallet_address)` | Streaming — trade sospechoso detectado | WHALE_MOVE, FLASH_ACC |
| `market_daily_stats` | Batch nocturno | Referencia histórica para los triggers |
| `user_stats_batch` | Batch nocturno | Perfil histórico de usuarios |

### → Gremio de Reportes / Notificaciones

Recibiréis un **POST HTTP** a `http://notifications:8000/notify` con este JSON:

```json
{
  "alert_id": "uuid-v4",
  "category": "MARKET",
  "sub_type": "FLIP",
  "payload": {
    "question": "Will Maduro be captured by 26th April?",
    "slug": "will-maduro-be-captured",
    "asset_id": "ast_003",
    "change": "NO_TO_YES",
    "actual_price": 0.54,
    "batch_price": 0.40
  },
  "timestamp": "2025-04-15T14:23:00Z"
}
```

**`sub_type` posibles:** `FLIP`, `SPIKE`, `PRICE_VAR`, `WHALE_MOVE`, `FLASH_ACC`

> ⚠️ **Nota de alineación con `models.py`:**  
> Vuestro modelo actualmente sólo define `FLIP`, `SUSPECT_USER` y `SUSPECT_TRADE`.  
> Nuestros tipos `WHALE_MOVE` y `FLASH_ACC` caen en la categoría `SUSPECT_USER`.  
> La vista `v_anomalias_con_imagen` ya incluye el campo `image_key` con el mapeo sugerido.  
> Pendiente acordar en reunión si añadís los nuevos sub_types o mapeamos en el listener.

También podéis hacer **GET directo a PostgreSQL** usando las vistas:
```sql
SELECT * FROM v_anomalias_recientes;
SELECT * FROM v_resumen_diario;
SELECT * FROM v_mercado_detalle WHERE condition_id = 'cond_xxx';
SELECT * FROM v_anomalias_por_tipo ORDER BY hora DESC;
```

### → Gremio de Orquestación (Airflow)

El DAG de informe diario puede consultar directamente:
```sql
SELECT * FROM v_resumen_diario;
SELECT * FROM v_anomalias_recientes WHERE created_at >= CURRENT_DATE;
```

---

## Variables de entorno del listener

| Variable | Valor local | Valor producción | Descripción |
|---|---|---|---|
| `POSTGRES_HOST` | `postgres` | `postgres` | Nombre del servicio en Docker network |
| `POSTGRES_DB` | `delfos` | `delfos` | Nombre de la base de datos |
| `POSTGRES_USER` | `postgres` | `postgres` | Usuario PostgreSQL |
| `POSTGRES_PASSWORD` | `delfos_dev_pass` | *secreto* | Cambiar en producción |
| `REPORTING_URL` | `http://mock-reporting:8000/anomaly` | `http://notifications:8000/notify` | Endpoint del gremio de Reportes |
| `NOTIFY_CHANNEL` | `anomaly_channel` | `anomaly_channel` | Canal pg_notify a escuchar |
| `HTTP_TIMEOUT` | `5` | `5` | Timeout HTTP en segundos |
| `HTTP_MAX_RETRIES` | `3` | `3` | Reintentos si Reportes falla |
| `RECONNECT_DELAY` | `5` | `5` | Segundos entre reconexiones a Postgres |

---

## Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| Puerto 5432 ocupado | PostgreSQL local instalado | Cambiar el puerto en `docker-compose.local.yml`: `"5433:5432"` |
| Listener no arranca | Postgres aún inicializando | `docker restart delfos-notify-listener-local` |
| Mock no recibe alertas | `notifications_enabled = false` | `UPDATE config SET value = 'true' WHERE key = 'notifications_enabled'` |
| `< archivo` no funciona en PowerShell | PowerShell no admite redirección stdin `<` | Usar `Get-Content archivo.sql \| docker exec -i ...` |
| Datos duplicados al re-ejecutar `04_test_data.sql` | `ON CONFLICT DO NOTHING` protege las tablas base, pero los INSERTs en `last_trade_price` y `trade_sospechosos` no tienen conflicto | Hacer `down -v` y `up --build` para empezar limpio |
