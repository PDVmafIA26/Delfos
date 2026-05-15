"""
PROYECTO DELFOS — Data Ingestion
logger.py : Módulo centralizado de logging.

Uso en cualquier módulo:
    from logger import get_logger
    log = get_logger(__name__)

    log.info("Mensaje informativo")
    log.warning("Algo inesperado pero no crítico")
    log.error("Error recuperable")
    log.exception("Error con traceback completo")   # dentro de un except

Ficheros generados:
    logs/info.log   → niveles INFO y superiores (INFO, WARNING, ERROR, CRITICAL)
    logs/error.log  → solo ERROR y CRITICAL
    consola         → INFO y superiores (mismo formato)

Rotación automática:
    Cada fichero rota cuando alcanza 5 MB, conservando los últimos 5 ficheros.
    Ejemplo: info.log → info.log.1 → info.log.2 → ... → info.log.5
"""

import logging
import os
from logging.handlers import RotatingFileHandler

# ─── Carpeta de logs ──────────────────────────────────────────────────────────
_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

# ─── Rutas de ficheros ────────────────────────────────────────────────────────
_INFO_LOG  = os.path.join(_LOG_DIR, "info.log")
_ERROR_LOG = os.path.join(_LOG_DIR, "error.log")

# ─── Formato ─────────────────────────────────────────────────────────────────
# %(threadName)s: esencial en app multihilo (ThreadPoolExecutor) para saber
# qué hilo generó cada línea (MainThread, ThreadPoolExecutor-0_0, etc.)
_FMT = "%(asctime)s | %(levelname)-8s | %(threadName)s | %(name)s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# ─── Tamaño máximo de cada fichero antes de rotar ────────────────────────────
_MAX_BYTES = 5 * 1024 * 1024   # 5 MB
_BACKUP_COUNT = 5               # Mantener hasta 5 ficheros de histórico


def _build_root_logger() -> logging.Logger:
    """
    Construye y configura el logger raíz del proyecto (solo la primera vez).
    Devuelve el logger 'delfos' que actúa como padre de todos los módulos.
    """
    root = logging.getLogger("delfos")

    # Evitar añadir handlers duplicados si el módulo se importa varias veces
    if root.handlers:
        return root

    root.setLevel(logging.DEBUG)  # Captura todo; los handlers filtran por nivel
    formatter = logging.Formatter(_FMT, datefmt=_DATE_FMT)

    # ── Handler 1: info.log — INFO y superiores ───────────────────────────────
    info_handler = RotatingFileHandler(
        _INFO_LOG,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    # ── Handler 2: error.log — solo ERROR y CRITICAL ─────────────────────────
    error_handler = RotatingFileHandler(
        _ERROR_LOG,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # ── Handler 3: consola — INFO y superiores ────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root.addHandler(info_handler)
    root.addHandler(error_handler)
    root.addHandler(console_handler)

    # Redirigir warnings de librerías (urllib3, requests, etc.) al logger
    # Sin esto, los DeprecationWarning van a stderr y no quedan en los ficheros
    logging.captureWarnings(True)
    logging.getLogger("py.warnings").setLevel(logging.WARNING)

    # Silenciar el ruido de confluent-kafka en stderr
    # (mensajes como '%4|timestamp|GETPID|...' que bypass nuestro logger)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

    return root


# Inicializar el logger raíz al importar el módulo
_build_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Devuelve un logger hijo del logger raíz 'delfos'.

    Args:
        name: Nombre del módulo. Usar siempre __name__ para trazabilidad.

    Returns:
        logging.Logger configurado y listo para usar.

    Ejemplo:
        log = get_logger(__name__)
        log.info("Iniciando proceso...")
    """
    # Si el nombre ya empieza por 'delfos', lo usamos directamente
    if name.startswith("delfos"):
        return logging.getLogger(name)
    return logging.getLogger(f"delfos.{name}")
