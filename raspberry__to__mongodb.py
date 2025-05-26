import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
from datetime import datetime

# Configuración MongoDB 
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'Proyecto_proma'
MONGO_COLLECTION_TEMPERATURA = 'temperatura_st'
MONGO_COLLECTION_GALGA = 'galga'

# Conexión a MongoDB 
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client[MONGO_DB]
collection_temperatura = db[MONGO_COLLECTION_TEMPERATURA]
collection_galga = db[MONGO_COLLECTION_GALGA]

# Configuración MQTT
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
TOPICS = [("sensortilebox/temperatura", 0), ("medidas/galga", 0)]

# Funciones MQTT
def on_connect(client, userdata, flags, rc):
    print(f"Conectado al broker MQTT con código: {rc}")
    client.subscribe(TOPICS)
    print("Suscrito a los tópicos:")
    for topic, _ in TOPICS:
        print(f"  - {topic}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic
        print(f"[{topic}] Mensaje recibido: {payload}")

        timestamp = payload.get("timestamp", datetime.utcnow().isoformat())

        if topic == "sensortilebox/temperatura":
            data = {
                "sensor": "sensor1",
                "value": float(payload["temperatura"]),
                "timestamp": timestamp
            }
            collection_temperatura.insert_one(data)
            print("Guardado en MongoDB [temperatura_st]")

        elif topic == "medidas/galga":
            data = {
                "fecha_hora": payload.get("fecha_hora", timestamp),
                "codigo_pieza": payload.get("codigo_pieza", "desconocido"),
                "medida": payload.get("medida", "sin_nombre"),
                "valor": float(payload["valor"])
            }
            collection_galga.insert_one(data)
            print("Guardado en MongoDB [galga]")

    except Exception as e:
        print(f"Error procesando mensaje en tópico {msg.topic}: {e}")


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_forever()