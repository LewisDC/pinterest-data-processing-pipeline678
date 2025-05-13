from kafka import KafkaConsumer
import boto3
import json
import yaml

class Batch_consumer:
    '''
    
    '''

    def __init__(self):
        pass

    def read_credentials(self, yaml_file_path):
        """Read and parse AWS S3 credentials from a YAML file."""

        try:
            # Open yaml file with database credentials
            with open(yaml_file_path, 'r') as yaml_file:
                creds = yaml.safe_load(yaml_file)  
            return creds
        except yaml.YAMLError as e:
            # Return an error message if it fails
            print(f"Error reading YAML file: {e}")
            return None

    def configure_kafka_consumer(self, kafka_topic, kafka_broker='localhost:9092'):
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',  # Start consuming from the earliest available offset
            enable_auto_commit=True,       # Automatically commit offsets
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None # Deserialize JSON-encoded messages
        )
        consumer.subscribe(topics=kafka_topic)
        return consumer

    def configure_s3_client(self, access_key, secret_key, region):
        # Initialise AWS S3 client
        self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
        return self.s3_client

    def save_message_to_s3(self, s3_client, bucket_name, message):
        try:
            # Attempt to decode the message
            decoded_message = message.value
            print(f"Received message: {decoded_message}")

            # Save the message as a JSON file in the S3 bucket
            json_data = json.dumps(decoded_message)
            file_key = f"{message.timestamp}-{message.offset}.json"  # Using timestamp and offset for a unique key
            s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=file_key)

            print(f"Saved message to S3: {file_key}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")


if __name__ == '__main__':
    # Create an instance of the Batch_consumer class
    consumer = Batch_consumer()
    # Access credentials file
    yaml_file_path = 'creds.yaml'
    creds = consumer.read_credentials(yaml_file_path)
    # Kafka configuration
    KAFKA_BROKER = 'localhost:9092'
    KAFKA_TOPIC = 'pinapi'
    # AWS 
    AWS_ACCESS_KEY = creds['aws_access_key']
    AWS_SECRET_KEY = creds['aws_secret_key']
    AWS_REGION = creds['aws_region']
    S3_BUCKET_NAME = creds['S3_bucket']

    kafka_consumer = consumer.configure_kafka_consumer(KAFKA_TOPIC, KAFKA_BROKER)
    s3_client = consumer.configure_s3_client(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION)

    # Consume messages from the Kafka topic and save them to the AWS S3 bucket
    for message in kafka_consumer:
        consumer.save_message_to_s3(s3_client, S3_BUCKET_NAME, message)