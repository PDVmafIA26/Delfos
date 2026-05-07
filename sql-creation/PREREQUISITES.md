# Prerrequisitos e instalación — Capa Oro

## Software necesario

| Herramienta | Versión mínima | Comprobación |
|---|---|---|
| Docker Desktop (Windows) | 24.x con WSL2 | `docker --version` |
| Docker Compose | v2.x (incluido en Docker Desktop) | `docker compose version` |
| Git | 2.x | `git --version` |
| Python *(solo para ejecutar el listener sin Docker)* | 3.11+ | `python --version` |

> En **Linux/Mac**: instalar Docker Engine + Compose plugin en lugar de Docker Desktop.

---

## Instalación desde cero

### 1. Clonar el repositorio y situarse en la carpeta

```powershell
git clone <url-del-repo>
cd Delfos\sql-creation
```

### 2. Levantar el stack Docker

```powershell
docker compose -f docker-compose.local.yml up --build -d
```

La primera ejecución descarga las imágenes (~2–3 min). Las siguientes arrancan en ~30 segundos.  
Levanta 3 servicios:

| Contenedor | Puerto | Descripción |
|---|---|---|
| `delfos-postgres-local` | 5432 | PostgreSQL 16 con esquema ya cargado |
| `delfos-mock-reporting` | 8000 | Simula el endpoint del gremio de Reportes |
| `delfos-notify-listener-local` | — | Escucha pg_notify y hace POST HTTP al mock |

### 3. Verificar la inicialización

```powershell
# Contenedores en pie
docker ps

# Las 11 tablas del E-R
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\dt"

# Los 5 triggers de anomalías
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\df fn_detect_*"

# Las 8 vistas
docker exec delfos-postgres-local psql -U postgres -d delfos -c "\dv v_*"
```

### 4. Ejecutar los datos de prueba

> ⚠️ Solo en desarrollo. Nunca en producción.

```powershell
# PowerShell no admite < para stdin — usar Get-Content
Get-Content 04_test_data.sql | docker exec -i delfos-postgres-local psql -U postgres -d delfos
```

Resultado esperado: **18 anomalías** generadas (FLIP ×5, PRICE\_VAR ×6, SPIKE ×4, WHALE\_MOVE ×2, FLASH\_ACC ×1).

### 5. Verificar que las notificaciones HTTP llegaron

```powershell
# Logs del listener — debe mostrar ✅ para cada anomalía
docker logs delfos-notify-listener-local

# Estado del mock server (cuenta cuántas alertas recibió)
Invoke-WebRequest http://localhost:8000/health | Select-Object -ExpandProperty Content
```

### 6. Parar

```powershell
# Conservar datos
docker compose -f docker-compose.local.yml down

# Borrar datos también (reinicio limpio)
docker compose -f docker-compose.local.yml down -v
```

---

## Ejecutar el listener sin Docker (opcional)

Si prefieres lanzar el `notify_listener.py` directamente en tu máquina:

```powershell
cd sql-creation

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
$env:POSTGRES_HOST     = "localhost"
$env:POSTGRES_DB       = "delfos"
$env:POSTGRES_USER     = "postgres"
$env:POSTGRES_PASSWORD = "delfos_dev_pass"
$env:REPORTING_URL     = "http://localhost:8000/anomaly"

# Ejecutar
python notify_listener.py
```

---

## Dependencias Python del listener

Fichero: `requirements.txt`

```
psycopg2-binary==2.9.10   # Conexión a PostgreSQL y LISTEN/NOTIFY
requests==2.32.3           # HTTP POST al servicio de Reportes
urllib3==2.2.3             # Pool de conexiones HTTP con reintentos
```
