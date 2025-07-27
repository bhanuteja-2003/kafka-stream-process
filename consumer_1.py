from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
from datetime import datetime

consumer = KafkaConsumer(
    'error_logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='error-log-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


k = 3
t = 10


recent_errors = defaultdict(deque) 
services_with_k_errors_within_t = set()
time_window = t # I am using already in seconds value

for message in consumer:
    
    log = message.value
    service = log['service']
    current_timestamp = datetime.strptime(log['timestamp'], "%Y-%m-%dT%H:%M:%S")

    service_deque = recent_errors[service]

    while service_deque and ((current_timestamp - service_deque[0]).total_seconds() > time_window):
        service_deque.popleft()

    service_deque.append(current_timestamp)

    if len(service_deque) >= k:
        services_with_k_errors_within_t.add(service)
        print(f"The services with {k} or more errors within {t} seconds - {services_with_k_errors_within_t}")

    
