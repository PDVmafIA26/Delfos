#!/usr/bin/env python3
"""
PROYECTO DELFOS — CAPA ORO
notify_listener.py : Escucha notificaciones de PostgreSQL (pg_notify) y las
                     reenvía como POST HTTP al microservicio de Reportes.

CÓMO FUNCIONA:
  1. PostgreSQL dispara pg_notify('anomaly_channel', payload_json) desde los triggers.
  2. Este script escucha el canal 'anomaly_channel' en tiempo real (sin polling).
  3. Cuando llega una notificación, hace un POST HTTP al contenedor de Reportes.

DESPLIEGUE:
  Añadir como servicio en docker-compose.yml (ver README.md)
  Variables de entorno configurables (ver sección CONFIG).
"""

import json
import logging
import os
import select
import sys
import time

import psycopg2
import psycopg2.extensions
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
DB_CONFIG = {
    "dbname":   os.getenv("POSTGRES_DB",       "delfos"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "delfos_pass"),
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     os.getenv("POSTGRES_PORT",     "5432"),
}

REPORTING_URL      = os.getenv("REPORTING_URL",     "http://notifications:8000/anomaly")
NOTIFY_CHANNEL     = os.getenv("NOTIFY_CHANNEL",    "anomaly_channel")
RECONNECT_DELAY    = int(os.getenv("RECONNECT_DELAY", "5"))   
HTTP_TIMEOUT       = int(os.getenv("HTTP_TIMEOUT",    "5"))   
HTTP_MAX_RETRIES   = int(os.getenv("HTTP_MAX_RETRIES", "3")) 
POLL_TIMEOUT       = int(os.getenv("POLL_TIMEOUT",    "30"))

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("delfos.notify_listener")


# =============================================================================
# CLIENTE HTTP con reintentos automáticos
# =============================================================================
def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


http_session = build_http_session()


def post_to_reporting(payload: dict) -> bool:
    """Envía el payload de la anomalía al microservicio de Reportes vía HTTP POST."""
    try:
        response = http_session.post(
            url=REPORTING_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=HTTP_TIMEOUT,
        )
        if response.ok:
            log.info(
                "✅ Anomalía enviada a Reportes | sub_type=%s | alert_id=%s | status=%d",
                payload.get("sub_type"),
                payload.get("alert_id"),
                response.status_code,
            )
            return True
        else:
            log.warning(
                "⚠️  Reportes respondió con error | status=%d | body=%s",
                response.status_code,
                response.text[:200],
            )
            return False
    except requests.exceptions.ConnectionError:
        log.error("❌ No se pudo conectar con el servicio de Reportes en %s", REPORTING_URL)
    except requests.exceptions.Timeout:
        log.error("❌ Timeout al conectar con Reportes (timeout=%ds)", HTTP_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        log.error("❌ Error HTTP inesperado: %s", exc)
    return False


# =============================================================================
# LISTENER PRINCIPAL
# =============================================================================
def connect_db() -> psycopg2.extensions.connection:
    """Conecta a PostgreSQL en modo autocommit (requerido para LISTEN/NOTIFY)."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def listen_loop():
    """Bucle principal: conecta, escucha el canal y despacha notificaciones."""
    while True:
        conn = None
        try:
            log.info("Conectando a PostgreSQL (%s@%s:%s/%s)…",
                     DB_CONFIG["user"], DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
            conn = connect_db()
            cur  = conn.cursor()
            cur.execute(f"LISTEN {NOTIFY_CHANNEL};")
            log.info("Escuchando canal '%s'. Esperando anomalías de Delfos…", NOTIFY_CHANNEL)

            while True:
                readable, _, _ = select.select([conn], [], [], POLL_TIMEOUT)

                if not readable:
                    cur.execute("SELECT 1")
                    continue

                conn.poll()

                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    log.debug("📨 NOTIFY recibido | pid=%d | channel=%s", notify.pid, notify.channel)

                    try:
                        payload = json.loads(notify.payload)
                    except json.JSONDecodeError as exc:
                        log.error("❌ Payload JSON inválido: %s | raw=%s", exc, notify.payload[:300])
                        continue

                    log.info(
                        "🔔 Anomalía detectada | category=%s | sub_type=%s | alert_id=%s",
                        payload.get("category"),
                        payload.get("sub_type"),
                        payload.get("alert_id"),
                    )

                    post_to_reporting(payload)

        except psycopg2.OperationalError as exc:
            log.error("⚠️  Conexión a PostgreSQL perdida: %s", exc)
        except KeyboardInterrupt:
            log.info("🛑 Listener detenido por el usuario.")
            break
        except Exception as exc:
            log.exception("💥 Error inesperado: %s", exc)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        log.info("⏳ Reconectando en %d segundos…", RECONNECT_DELAY)
        time.sleep(RECONNECT_DELAY)


# =============================================================================
# ENTRADA
# =============================================================================
if __name__ == "__main__":
    log.info("Delfos Notify Listener arrancando…")
    log.info("Reporting URL : %s", REPORTING_URL)
    log.info("Canal NOTIFY  : %s", NOTIFY_CHANNEL)
    log.info("Base de datos : %s@%s/%s", DB_CONFIG["user"], DB_CONFIG["host"], DB_CONFIG["dbname"])
    listen_loop()
