from kafka import KafkaProducer, KafkaConsumer
import time
from config import *

producer = KafkaProducer(bootstrap_servers=server_addr+':9092')
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers= server_addr + ':9092',
    auto_offset_reset='earliest'
)

topic = topic_name

def send_message(message):
    producer.send(topic, message.encode('utf-8'))


while True:
    message = input("Mensaje: ")
    if message == 'exit':
        break
    send_message(message)
    print(f"Mensaje enviado: {message}")

for message in consumer:
    msg = message.value.decode('utf-8')
    print(f"Mensaje recibido: {msg}")

consumer.close()