from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'pinapi'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',  # Start consuming from the earliest available offset
    enable_auto_commit=True,       # Automatically commit offsets
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None # Deserialize JSON-encoded messages
)

# Consume messages from the Kafka topic
for message in consumer:
    try:
        # Attempt to decode the message
        decoded_message = message.value
        print(f"Received message: {decoded_message}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Close the Kafka consumer
# consumer.close()