import time
import json
import psycopg2 # Para Postgres (Capa Oro)
from kafka import KafkaConsumer
from pymongo import MongoClient # Para Mongo (Capa Bronce/Plata)
from datetime import datetime

# Colores (MANTENIDOS)
verde = '\033[92m'
rojo = '\033[91m'
amarillo = '\033[93m'
turquesa = '\033[38;5;44m'
lima = '\33[38;5;46m'
reset = '\033[0m'

# 1. CONFIGURACIÓN MONGODB (Capa Bronce/Plata)
client = MongoClient("mongodb://admin:administrador@localhost:27017/")
db_bronce = client['capa_bronce']
db_plata = client['capa_plata']
col_bronce = db_bronce['historico_mercados']
col_plata = db_plata['mercados_limpios']
col_perfiles = db_bronce['bronce_perfiles']

# 2. CONFIGURACIÓN POSTGRES (Capa Oro - Nuevo Diagrama)[cite: 1, 3]
def conectar_postgres():
    try:
        return psycopg2.connect(
            host="localhost",
            database="capa_oro",
            user="admin",
            password="administrador"
        )
    except:
        return None

print(f"\n{amarillo}Iniciando Bridge Híbrido (Mongo + Postgres)...{reset}", flush=True)

# 3. CONEXIÓN KAFKA
while True:
    try:
        consumer = KafkaConsumer(
            'tech_markets', 'top_wallets',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='bridge-delfos-group'
        )
        break
    except Exception as e:
        print(f"{rojo}Reintentando conexión Kafka: {e}{reset}")
        time.sleep(2)

# BUCLE DE PROCESAMIENTO
for message in consumer:
    try:
        # --- CASO A: BALLENAS (USUARIOS) ---
        if message.topic == 'top_wallets':
            perfil = message.value
            # Guardar en Mongo (Tu lógica)
            col_perfiles.update_one({'address': perfil.get('address')}, {'$set': {**perfil, 'last_seen': datetime.now()}}, upsert=True)
            
            # Guardar en Postgres (Capa Oro - Tabla USUARIOS)
            pg_conn = conectar_postgres()
            if pg_conn:
                cur = pg_conn.cursor()
                cur.execute("INSERT INTO USUARIOS (wallet_address) VALUES (%s) ON CONFLICT DO NOTHING", (perfil.get('address'),))
                pg_conn.commit()
                cur.close()
                pg_conn.close()
            
            print(f"{turquesa}👤 Inteligencia: Perfil registrado (Mongo + SQL) -> {perfil.get('address')}{reset}")
            continue

        # --- CASO B: MERCADOS ---
        dato = message.value
        
        # 1. CAPA BRONCE (Mongo)
        dato['ingested_at'] = datetime.now().isoformat()
        col_bronce.insert_one(dato)
        
        # 2. LIMPIEZA (Tu lógica)
        try:
            precio_num = float(str(dato.get('price', 0)).replace(',', '.'))
            volumen_num = float(str(dato.get('volumen', 0)).replace(',', '.'))
        except:
            precio_num, volumen_num = 0.50, 0.0
        
        # 3. CAPA PLATA (Mongo)
        documento_plata = { "market_id": dato.get('market_id'), "precio": precio_num, "volumen": volumen_num, "fecha": datetime.now() }
        col_plata.insert_one(documento_plata)

        # 4. CAPA ORO (Postgres - Diagrama E-R)[cite: 1]
        pg_conn = conectar_postgres()
        if pg_conn:
            cur = pg_conn.cursor()
            # Insertar Evento y Mercado (Simplificado para el ejemplo)
            cur.execute("""
                INSERT INTO EVENTOS (eventId, question, volume) 
                VALUES (%s, %s, %s) ON CONFLICT (eventId) DO UPDATE SET volume = EXCLUDED.volume
            """, (dato.get('market_id'), dato.get('titulo'), volumen_num))
            
            cur.execute("""
                INSERT INTO MERCADOS_MASTER (id, conditionId, question, volume, eventId)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id, conditionId) DO NOTHING
            """, (dato.get('market_id'), dato.get('market_id'), dato.get('titulo'), volumen_num, dato.get('market_id')))
            
            pg_conn.commit()
            cur.close()
            pg_conn.close()

        print(f"{verde}✔ Híbrido OK {reset}| {turquesa}{dato.get('titulo', '')[:30]}...{reset}", flush=True)

    except Exception as e:
        print(f"{rojo}❌ Error en Bridge: {e}{reset}")