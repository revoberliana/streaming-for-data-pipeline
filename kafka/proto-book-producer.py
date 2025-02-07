from confluent_kafka import Producer
import uuid
import json
from time import sleep
from faker import Faker
import book_pb2  # Your generated Protobuf file
from dotenv import load_dotenv
import os

load_dotenv()

# Set up Kafka producer
producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_HOST', 'localhost:9092'),
})

fake = Faker()

def create_message():
    return {
        'book_id': str(uuid.uuid4()),
        'title': fake.name(),
        'author': fake.name(),
        'price': fake.random_int(min=10, max=100),
    }

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce messages
while True:
    msg = create_message()
    serialized_msg = book_pb2.Book(**msg).SerializeToString()  # Serialize using protobuf
    producer.produce('revo-kafka', serialized_msg, callback=delivery_report)
    producer.flush()  # Ensure delivery of the message
    sleep(1)
