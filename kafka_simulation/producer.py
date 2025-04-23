
# Simulates Kafka streaming transactions

from time import sleep
from kafka import KafkaProducer
import json
import random
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

products = ['Laptop', 'Phone', 'Tablet', 'Camera']

while True:
    message = {
        'product': random.choice(products),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(100, 1500), 2),
        'timestamp': datetime.utcnow().isoformat()
    }
    producer.send('ecommerce-transactions', message)
    print("Sent:", message)
    sleep(1)
