import json
import time
import requests
import random
import re
from kafka import KafkaProducer

# Configuración
KAFKA_TOPIC = 'tech_markets'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def iniciar_ingesta():
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=20"
    res = requests.get(url)
    lista_mercados = res.json()

    while True:
        for m in lista_mercados:
            try:
                # Extracción y limpieza de precio
                raw_price = m.get('outcomePrices') or m.get('probabilities', "[0.5]")
                solo_numeros = re.findall(r"[-+]?\d*\.\d+|\d+", str(raw_price))
                precio_actual = float(solo_numeros[0]) if solo_numeros else 0.50

                # Formato exacto que espera el grupo de ORO
                payload = {
                    "market_id": m.get('conditionId') or m.get('id'),
                    "titulo": m.get('question', 'Mercado Polymarket'),
                    "precio": precio_actual,        # <--- ORO lo necesita así
                    "volumen": float(m.get('volume', 0)),
                    "timestamp_evento": int(time.time()), # <--- ORO lo necesita así
                    "categoria": "tech"
                }
                
                producer.send(KAFKA_TOPIC, value=payload)
                print(f"📦 Enviado a Kafka: {payload['titulo'][:30]}...")
                time.sleep(0.5)
            except:
                continue