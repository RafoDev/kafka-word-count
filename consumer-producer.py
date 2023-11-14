from kafka import KafkaProducer, KafkaConsumer
from threading import Thread
from config import *
import time

producer = KafkaProducer(bootstrap_servers=server_addr)

# Configuración del consumidor
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=server_addr,
    auto_offset_reset='latest'
)


def send_message(message):
    producer.send(topic_name, message.encode('utf-8'))


def receive_messages():
    for message in consumer:
        print(
            f"\nMensaje recibido: {message.value.decode('utf-8')}\nTú: ", end='')


receiver_thread = Thread(target=receive_messages)
receiver_thread.daemon = True
receiver_thread.start()

print("Bienvenido al chat. Escribe 'exit' para salir.")

try:
    while True:
        message = input("Tú: ")
        if message.lower() == 'exit':
            break
        send_message(message)
except KeyboardInterrupt:
    pass
finally:
    producer.close()
    consumer.close()
