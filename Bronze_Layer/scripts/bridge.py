import time
import sys
from kafka import KafkaConsumer
from boto3 import Session
from datetime import datetime
import kafka.errors

# Colores 
rojo = '\033[91m'
verde = '\033[92m'
amarillo = '\033[93m'
lima = '\33[38;5;46m'
reset = '\033[0m'

# CONFIGURACIÓN MINIO 
session = Session(aws_access_key_id='admin', aws_secret_access_key='administrador')
s3 = session.resource('s3', endpoint_url='http://minio:9000')
BUCKET_NAME = 'capa-bronce'

# CONEXIÓN SEGURA A KAFKA 
print(f"{amarillo}\nEsperando a que Kafka esté listo...{reset}", flush=True)

while True:
    try:
        # Usamos el puerto 29092 que es la red interna de Docker
        consumer = KafkaConsumer(
            'tech_markets',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=None, 
            auto_offset_reset='earliest'
        )
        break # Si conecta con éxito, salimos del bucle
    except kafka.errors.NoBrokersAvailable:
        time.sleep(2) # Espera 2 segundos antes de reintentar

print(f"\n{lima}Pasarela activada. Destino: {reset}{verde}{BUCKET_NAME}{reset}\n", flush=True)

# BUCLE DE PROCESAMIENTO 
for message in consumer:
    try:
        # Recibimos el Parquet directo como bytes
        archivo_parquet_bytes = message.value
        ahora = datetime.now()
        categoria = "markets_data" 

        # Estructura de carpetas Hive (year/month/day)
        ruta = (f"{categoria}/"
                f"year={ahora.year}/"
                f"month={ahora.month:02d}/"
                f"day={ahora.day:02d}/"
                f"{ahora.strftime('%H%M%S')}_{message.offset}.parquet")

        # Subida directa a MinIO
        s3.Object(BUCKET_NAME, ruta).put(Body=archivo_parquet_bytes)
        print(f"{verde}✅ Parquet movido ->{reset} {ruta}", flush=True)

    except Exception as e:
        print(f"\n{rojo}¡¡Error!!: {e}{reset}", flush=True)