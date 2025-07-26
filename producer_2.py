from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

service_name = "payment-service"
levels = ['INFO', 'DEBUG', 'ERROR']



while True:
    log = {
        'service': service_name,
        'level': random.choices(levels, weights=[0.5, 0.1, 0.4])[0],
        'message': 'Payment event'
    }
    producer.send('logs', value=log)
    time.sleep(0.5)
    

producer.flush()
