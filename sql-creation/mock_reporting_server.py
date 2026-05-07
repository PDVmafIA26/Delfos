#!/usr/bin/env python3
"""
PROYECTO DELFOS — CAPA ORO
mock_reporting_server.py : Servidor HTTP falso para pruebas locales.

Imita el endpoint del gremio de Reportes/Notificaciones.
Recibe los POST de anomalías de notify_listener.py y los muestra en consola.

USO:
    pip install fastapi uvicorn
    python mock_reporting_server.py

    → Escucha en http://localhost:8000/anomaly
"""

import json
import sys
from datetime import datetime

try:
    import uvicorn
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
except ImportError:
    print("Instala las dependencias: pip install fastapi uvicorn")
    sys.exit(1)

app = FastAPI(title="Delfos Mock Reporting Server")

# Colores ANSI para la consola
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

# Mapa de colores por tipo de anomalía
COLOR_MAP = {
    "FLIP":       YELLOW,
    "SPIKE":      RED,
    "PRICE_VAR":  CYAN,
    "WHALE_MOVE": BLUE,
    "FLASH_ACC":  GREEN,
}

EMOJI_MAP = {
    "FLIP":       "🔄",
    "SPIKE":      "📈",
    "PRICE_VAR":  "📊",
    "WHALE_MOVE": "🐋",
    "FLASH_ACC":  "⚡",
    "MARKET":     "📉",
    "USER":       "👤",
}

received_count = 0


@app.post("/anomaly")
async def receive_anomaly(request: Request):
    global received_count
    received_count += 1

    try:
        body = await request.json()
    except Exception:
        raw = await request.body()
        body = {"raw": raw.decode()}

    sub_type = body.get("sub_type", "UNKNOWN")
    category = body.get("category", "?")
    alert_id = body.get("alert_id", "?")
    payload  = body.get("payload", {})
    color    = COLOR_MAP.get(sub_type, RESET)
    emoji    = EMOJI_MAP.get(sub_type, "🔔")

    now = datetime.now().strftime("%H:%M:%S")
    sep = "─" * 60

    print(f"\n{sep}")
    print(f"{BOLD}{color}[{now}] #{received_count} {emoji}  {category} / {sub_type}{RESET}")
    print(f"{sep}")
    print(f"  alert_id : {alert_id}")

    # Campos más informativos según el tipo
    if sub_type == "FLIP":
        print(f"  mercado  : {payload.get('question', '-')}")
        print(f"  cambio   : {payload.get('change', '-')}")
        print(f"  precio   : {payload.get('batch_price', '-')} → {payload.get('actual_price', '-')}")

    elif sub_type == "SPIKE":
        print(f"  mercado  : {payload.get('question', '-')}")
        print(f"  ratio    : {payload.get('ratio', '-')}x")
        print(f"  volumen  : ${payload.get('usd_detected', '-')}")

    elif sub_type == "PRICE_VAR":
        print(f"  mercado  : {payload.get('question', '-')}")
        print(f"  variación: {payload.get('variation_pct', '-')}%")
        print(f"  pre-flip : {payload.get('is_pre_flip', '-')}")

    elif sub_type == "WHALE_MOVE":
        print(f"  wallet   : {payload.get('user_id', '-')}")
        print(f"  impacto  : {payload.get('market_impact_pct', '-')}%")
        print(f"  pnl      : ${payload.get('realized_pnl', '-')}")
        print(f"  nivel    : {payload.get('historical_pnl', '-')}")

    elif sub_type == "FLASH_ACC":
        print(f"  wallet   : {payload.get('user_id', '-')}")
        print(f"  antigüedad: {payload.get('age_minutes', '-')} min")
        print(f"  volumen  : ${payload.get('suspicious_volume', '-')}")
        print(f"  ratio    : {payload.get('ratio_vs_normal', '-')}x vs media")

    print(f"\n  payload completo:")
    print(f"  {json.dumps(payload, ensure_ascii=False, indent=2).replace(chr(10), chr(10)+'  ')}")
    print(f"{sep}")

    return JSONResponse(
        status_code=200,
        content={
            "status":    "received",
            "alert_id":  alert_id,
            "sub_type":  sub_type,
            "server":    "mock_reporting"
        }
    )


@app.get("/health")
async def health():
    return {"status": "ok", "received": received_count}


@app.get("/")
async def root():
    return {
        "service": "Delfos Mock Reporting Server",
        "endpoints": {
            "POST /anomaly": "Recibe alertas del notify_listener",
            "GET  /health":  "Estado del servidor"
        },
        "received_so_far": received_count
    }


if __name__ == "__main__":
    print(f"\n{BOLD}{'='*60}")
    print("  Delfos — Mock Reporting Server")
    print(f"{'='*60}{RESET}")
    print(f"  Escuchando en : {CYAN}http://localhost:8000{RESET}")
    print(f"  Endpoint POST : {CYAN}http://localhost:8000/anomaly{RESET}")
    print(f"  Health check  : {CYAN}http://localhost:8000/health{RESET}")
    print(f"  Ctrl+C para parar\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
