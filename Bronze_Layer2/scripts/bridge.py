import time
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import kafka.errors

# Colores
verde = '\033[92m'
amarillo = '\033[93m'
lima = '\33[38;5;46m'
reset = '\033[0m'

# CONFIGURACIÓN MONGODB
# Conectamos usando el nombre del servicio en Docker
client = MongoClient("mongodb://admin:administrador@mongodb:27017/")
db = client['capa_bronce']
coleccion = db['historico_mercados']

# CONEXIÓN SEGURA A KAFKA
print(f"{amarillo}Esperando a Kafka y MongoDB...{reset}", flush=True)

while True:
    try:
        consumer = KafkaConsumer(
            'tech_markets',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')), # Ahora decodificamos JSON
            auto_offset_reset='earliest'
        )
        break
    except Exception:
        time.sleep(2)

print(f"{lima}Pasarela MongoDB activada.{reset}", flush=True)

# BUCLE DE PROCESAMIENTO
for message in consumer:
    try:
        dato_json = message.value
        
        # Añadimos metadatos útiles para Sistemas
        dato_json['ingested_at'] = datetime.now().isoformat()
        
        # Insertamos en MongoDB
        coleccion.insert_one(dato_json)
        
        print(f"{verde}Evento guardado en MongoDB{reset}", flush=True)

    except Exception as e:
        print(f"Error: {e}", flush=True)