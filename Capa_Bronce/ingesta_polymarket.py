import json
import time
import os
import requests  
import random
import re 
from kafka import KafkaProducer
from dotenv import load_dotenv

# COLORES 
verde, turquesa, rojo, amarillo, lima, reset = '\033[92m', '\033[38;5;44m', '\033[91m', '\033[93m', '\33[38;5;46m', '\033[0m'

load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'polymarket_data')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')

# 1. CATEGORÍAS (Ampliamos la búsqueda para que encuentre datos sí o sí)
CATEGORIES = ["politics", "geopolitics", "tech", "finance", "economy"]

# 2. CONEXIÓN CON KAFKA
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"\n{lima}✅ Conectado a Kafka exitosamente{reset}")
except Exception as e:
    print(f"\n{rojo}❌ Error Kafka: {e}{reset}")
    exit()

def iniciar_ingesta():
    print(f"\n{verde}🚀 Iniciando ingesta para MongoDB...{reset}")
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

    try:
        # Aumentamos el límite para tener más variedad
        url_markets = "https://gamma-api.polymarket.com/markets?active=true&limit=100"
        res = requests.get(url_markets, headers=headers, timeout=15)
        lista_mercados = res.json()
    except Exception as e:
        print(f"{rojo}⚠️ Error de red: {e}. Reintentando...{reset}")
        time.sleep(5)
        return iniciar_ingesta()

    while True:
        random.shuffle(lista_mercados)
        mensajes_enviados = 0
        
        for m in lista_mercados:
            try:
                # --- MEJORA DEL FILTRADO ---
                # Buscamos en varios campos para no perder mercados
                texto_busqueda = f"{m.get('groupName', '')} {m.get('category', '')} {m.get('question', '')}".lower()
                
                cat_asignada = None
                for c in CATEGORIES:
                    if c in texto_busqueda:
                        cat_asignada = c
                        break
                
                # Si sigue sin encontrar, asignamos "politics" por defecto para que no se quede vacío
                if not cat_asignada:
                    cat_asignada = "politics"

                m_id = m.get('conditionId') or m.get('id')
                if not m_id: continue

                # Limpieza de precios
                raw_price = m.get('outcomePrices') or m.get('probabilities')
                precio_actual = "0.50"
                if raw_price:
                    solo_numeros = re.findall(r"[-+]?\d*\.\d+|\d+", str(raw_price))
                    if solo_numeros: precio_actual = solo_numeros[0]
                
                payload = {
                    "categoria": cat_asignada,
                    "market_id": m_id,
                    "titulo": m.get('question', 'Mercado'),
                    "precio": precio_actual,
                    "volumen": m.get('volume', 0),
                    "timestamp_evento": int(time.time()),
                    "fuente": "Polymarket_Live"
                }
                
                # ENVÍO A KAFKA
                producer.send(KAFKA_TOPIC, value=payload)
                mensajes_enviados += 1
                
                print(f"{turquesa}📦 [{cat_asignada.upper()}] ->{reset} {amarillo}Precio: {precio_actual}{reset} | {payload['titulo'][:45]}")
                time.sleep(0.2)

            except:
                continue
        
        if mensajes_enviados == 0:
            print(f"{rojo}⚠️ No se encontraron mercados. Revisando filtros...{reset}")
        
        print(f"\n{lima}--- CICLO COMPLETADO - ENVIADOS: {mensajes_enviados} ---{reset}\n")
        time.sleep(2)

if __name__ == "__main__":
    iniciar_ingesta()