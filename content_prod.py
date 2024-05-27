import json 
from time import sleep
from kafka import KafkaProducer
import random

KAFKA_TOPIC_NAME = "movielens_content"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

kafka_producer_object = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
for i in range(2):
    id = random.randint(1, 330000)
    kafka_producer_object.send(KAFKA_TOPIC_NAME, value={"userId" : id})
    sleep(0.2)