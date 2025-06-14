from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'pinapi'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

@app.post("/pin/")
def get_db_row(item: Data):
    # Convert the Pydantic model to a dictionary
    data = dict(item)

    # Send the data to Kafka
    producer.send(topic = KAFKA_TOPIC, value = data)

    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
