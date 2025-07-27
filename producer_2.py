from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

service_name = "payment-service"
levels = ['INFO', 'DEBUG', 'ERROR']



while True:
    log = {
        'timestamp' : datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'service': service_name,
        'level': random.choices(levels, weights=[0.4, 0.1, 0.5])[0],
        'message': 'Payment event'
    }
    producer.send('logs', value=log)
    time.sleep(3)
    

producer.flush()
