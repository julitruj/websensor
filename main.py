from paho.mqtt import client as mqtt_client
import random
import time
import requests

broker = 'broker.emqx.io'
port = 1883
topic = "dht11sensor"

client_id = f'python-mqtt-{random.randint(0, 1000)}' # genera un ID de cliente aleatorio
username = 'emqx'
password = 'public'

http_server_url = 'http://127.0.0.1:8000/' # URL del servidor HTTP local

def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties=None): # Esta función se ejecuta cuando el cliente se conecta al broker MQTT.
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc) # Si la conexión falla, imprime un mensaje de error.
    
    client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2) # Crea un cliente MQTT.
    client.username_pw_set(username, password)
    client.on_connect = on_connect # Se define la función on_connect como la función que se ejecutará cuando el cliente se conecte al broker.
    client.connect(broker, port) # Se conecta al broker MQTT.
    return client


def subscribe(client: mqtt_client): # Esta función se encarga de suscribir al cliente a un tópico MQTT.
    def on_message(client, userdata, msg):
        payload = msg.payload.decode()
        print(f"Received `{payload}` from `{msg.topic}` topic") # Imprime el mensaje recibido y el tópico al que se suscribió.
        
        try:
            response = requests.post(http_server_url, json={'data': payload}) # Publica el mensaje recibido en el servidor HTTP local.
            response.raise_for_status()  # Si la solicitud HTTP falla, se lanza una excepción.
            print("Message payload published to the local HTTP server")
        except requests.exceptions.RequestException as e:
            print(f"Failed to publish message payload to the local HTTP server: {e}") # Si la solicitud HTTP falla, imprime un mensaje de error.

    client.subscribe(topic) # Se suscribe al tópico MQTT.
    client.on_message = on_message # Se define la función on_message como la función que se ejecutará cuando se reciba un mensaje.

def run():
    client = connect_mqtt() # Se conecta al broker MQTT.
    subscribe(client)
    client.loop_forever() # Se inicia el bucle de eventos del cliente MQTT.

if __name__ == '__main__': # Si el script se ejecuta directamente, se llama a la función run.
    run()


