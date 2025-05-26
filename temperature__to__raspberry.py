import asyncio
import datetime
import struct
import json
import paho.mqtt.client as mqtt
from bleak import BleakClient

# MAC de Sensor.FileBox
DEVICE_ADDRESS = "DF:XX:XX:XX:XX:XX"
#UUID de temperatura
TEMPERATURE_UUID = "00040000-0001-11e1-ac36-0002a5d5c51b"  

#Acceso a broker MQTT
MQTT_BROKER = "XXXX"
MQTT_PORT = 1883
MQTT_TOPIC = "sensortilebox/temperatura"

mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

def parse_temperature(data):
    if len(data) < 4:
        return None, None

    _, temp_raw = struct.unpack_from('<Hh', data)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temperature = temp_raw / 10.0

    return timestamp, temperature

def notification_handler(sender, data):
    timestamp, temperature = parse_temperature(data)
    if timestamp is not None:
        payload = {
            "timestamp": timestamp,
            "temperatura": temperature
        }
        print(f"{sender}: {payload}")
        mqtt_client.publish(MQTT_TOPIC, json.dumps(payload))

async def main():
    async with BleakClient(DEVICE_ADDRESS) as client:
        print(f"Conectado a {DEVICE_ADDRESS}")
        await client.start_notify(TEMPERATURE_UUID, notification_handler)
        print(f"Suscrito a UUID: {TEMPERATURE_UUID}")
        print("Esperando datos... ")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await client.stop_notify(TEMPERATURE_UUID)
            print("Desconectado")

if __name__ == "__main__":
    asyncio.run(main())