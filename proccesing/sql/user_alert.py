import psycopg2
import requests

# LA IDEA ES LLAMAR ESTO DESPUÉS DE EJECUTAR EL SCRIPT/FUNCIÓN QUE RECOGE LA INFORMACIÓN DE LOS USUARIOS

conn = psycopg2.connect(
    dbname="markets",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# 1. Obtain new suspects
cursor.execute("""
    SELECT wallet_address, total_won, total_position
    FROM usuarios
    WHERE es_sospechoso = TRUE
    AND alert_sent = FALSE
""")

rows = cursor.fetchall()

# 2. Send alerts
for wallet, won, positions in rows:
    message = f"""
Usuario sospechoso detectado
Wallet: {wallet}
Total ganado: {won}
Posiciones: {positions}
"""

    #HTTP API TELEGRAM

# 3. Mark as sent
cursor.execute("""
    UPDATE usuarios
    SET alert_sent = TRUE
    WHERE es_sospechoso = TRUE
    AND alert_sent = FALSE
""")

conn.commit()
cursor.close()
conn.close()