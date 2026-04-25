import json
import os
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
import redis
from dotenv import load_dotenv

# COLORES 
verde = "\033[0;32m"
azul = "\033[0;34m"
turquesa = "\033[0;36m"
amarillo = "\033[1;33m"
rojo = "\033[0;31m"
lima = "\033[1;32m"
reset = "\033[0m"

# Carga de entorno
load_dotenv()

# Configuración de Logging profesional
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def iniciar_bridge():
    print(f"\n{azul}════════════════════════════════════════════════════════════{reset}")
    print(f"{azul}   SISTEMA DE ENRUTAMIENTO INTELIGENTE - CAPA BRONCE{reset}")
    print(f"{azul}════════════════════════════════════════════════════════════{reset}\n")

    try:
        # 1. Conexión a MongoDB (Persistencia Bronce)
        mongo_client = MongoClient(os.getenv('MONGO_URI', 'mongodb://admin:administrador@localhost:27017/'))
        db = mongo_client[os.getenv('MONGO_DB', 'capa_bronce')]
        coleccion_raw = db['raw_polymarket_data']
        print(f"{lima}\nMongoDB Conectado (Almacén Bronce){reset}")

        # 2. Conexión a Redis (Alertas en Tiempo Real)
        r = redis.Redis(host='localhost', port=6379, db=0)
        print(f"{lima}Redis Conectado (Bus de Alertas){reset}")

        # 3. Consumidor de Kafka
        consumer = KafkaConsumer(
            os.getenv('KAFKA_TOPIC', 'tech_markets'),
            bootstrap_servers=[os.getenv('KAFKA_SERVER', 'localhost:9092')],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='delfos-bridge-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"{lima}Kafka suscrito al tópico: {amarillo}{os.getenv('KAFKA_TOPIC')}{reset}")
        print(f"{turquesa}\nEscuchando flujo de datos...{reset}\n")

        # 4. Bucle de Enrutamiento Basado en Headers
        for message in consumer:
            data = message.value
            # Extraemos el tipo de evento de los headers de Kafka
            headers = dict(message.headers)
            event_type = headers.get('event_type', b'generic').decode()

            # A. Persistencia Obligatoria en Bronce (Mongo)
            coleccion_raw.insert_one(data)

            # B. Enrutamiento Lógico
            if event_type == 'price_update':
                # Enviamos a Redis para el Dashboard
                r.publish('price_updates', json.dumps(data))
                print(f"{amarillo}[PRECIO]{reset} {data.get('titulo')[:40]}... -> {amarillo}{data.get('precio')}{reset}")

            elif event_type == 'whale_info' or 'nombre_ballena' in data:
                # Alerta de Ballena
                r.set('ultima_ballena', json.dumps(data))
                print(f"{rojo}\n[BALLENA]{reset} Detectada en: {amarillo}{data.get('titulo')[:40]}{reset}")
            
            else:
                print(f"{turquesa}[DATOS]{reset} Procesado Market ID: {data.get('market_id')}")

    except Exception as e:
        print(f"{rojo}\nERROR EN EL BRIDGE: {e}{reset}")
        logger.error(f"Error crítico: {e}")

if __name__ == "__main__":
    iniciar_bridge()