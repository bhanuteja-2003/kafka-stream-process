from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='log-admin'
)

topic_list = [
    NewTopic(name="logs", num_partitions=3, replication_factor=1)
]

admin_client.create_topics(new_topics=topic_list, validate_only=False)
print("Kafka topic 'logs' created.")
