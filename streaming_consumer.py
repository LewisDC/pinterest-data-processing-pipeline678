from kafka import KafkaConsumer
from json import loads

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'pinapi'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',  # Start consuming from the latest available offset
    enable_auto_commit=False,       # Disable auto commit
    value_deserializer=lambda x: loads(x.decode('utf-8'))  # Deserialize JSON-encoded messages
)

# Consume messages from the Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")

# Close the Kafka consumer
# consumer.close()