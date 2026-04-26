# create_kafka_topic.py
# Creates a Kafka topic using kafka-python (for local dev)

from kafka.admin import KafkaAdminClient, NewTopic
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "weather-history-events"
PARTITIONS = 3
REPLICATION = 1

admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
try:
    topic = NewTopic(name=TOPIC_NAME, num_partitions=PARTITIONS, replication_factor=REPLICATION)
    admin.create_topics([topic])
    print(f"Created topic: {TOPIC_NAME}")
except Exception as e:
    print(f"Topic may already exist or error: {e}")
finally:
    admin.close()
