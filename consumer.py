from kafka import KafkaConsumer
from collections import Counter
from config import *

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers= server_addr + ':9092',
    auto_offset_reset='earliest'
)

word_count = Counter()

def count_words(text):
    words = text.split()
    word_count.update(words)

for message in consumer:
    msg = message.value.decode('utf-8')
    print(f"Mensaje recibido: {msg}")
    count_words(msg)
    print(f"Conteo actual: {word_count}")

consumer.close()
