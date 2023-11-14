from kafka import KafkaProducer
import time
from config import *

producer = KafkaProducer(bootstrap_servers=server_addr+':9092')
topic = topic_name

def send_message(message):
    producer.send(topic, message.encode('utf-8'))

messages = ["Hola mundo", "Este es un ejemplo de conteo de palabras", "Kafka con Python"]

for msg in messages:
    send_message(msg)
    print(f"Mensaje enviado: {msg}")
    time.sleep(1)

producer.close()