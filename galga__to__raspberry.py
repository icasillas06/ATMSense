import time
import json
import paho.mqtt.client as mqtt
import os

RUTA_TXT = "/mnt/galga/medidas.txt"
MQTT_BROKER = "XXX.XX.XXX.XXX"
MQTT_PORT = 1883
MQTT_TOPIC = "medidas/galga"

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def leer_y_enviar_txt_con_buffer(path, espera_sin_cambios=3):
    ultima_posicion = 0
    buffer_lineas = []
    ultima_modificacion = 0

    while True:
        try:
            mod_time = os.path.getmtime(path)
            if mod_time != ultima_modificacion:
                ultima_modificacion = mod_time
                with open(path, "r", encoding="utf-8") as f:
                    f.seek(ultima_posicion)
                    nuevas_lineas = f.readlines()
                    ultima_posicion = f.tell()

                for linea in nuevas_lineas:
                    linea_limpia = linea.replace('\x00', '').strip()
                    if linea_limpia:
                        try:
                            json.loads(linea_limpia)  
                            buffer_lineas.append(linea_limpia)
                        except json.JSONDecodeError:
                            print(f"Línea inválida descartada: {repr(linea_limpia)}")

                tiempo_espera = 0
            else:
                tiempo_espera += 1
                if tiempo_espera >= espera_sin_cambios and buffer_lineas:
                    for linea in buffer_lineas:
                        doc = json.loads(linea)
                        client.publish(MQTT_TOPIC, json.dumps(doc))
                        print(f"Enviado: {doc}")
                    buffer_lineas = [] 
                    tiempo_espera = 0

        except FileNotFoundError:
            print("Archivo no encontrado, esperando...")
            ultima_posicion = 0
            buffer_lineas = []

        time.sleep(1)

if __name__ == "__main__":
    leer_y_enviar_txt_con_buffer(RUTA_TXT)