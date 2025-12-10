import json
import time
from confluent_kafka import Producer

config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "producer-sen"
}
producer = Producer(config)

def load_json_list(path):
    with open(path, "r") as f:
        return json.load(f)

def send(topic, msg):
    producer.produce(topic, json.dumps(msg).encode("utf-8"))
    producer.poll(0)

if __name__ == "__main__":
    data = load_json_list("../dataset/json/sen.json")

    for record in data:
        send("sen_hora", record)
        print(f"[SEN] enviado: {record}")
        time.sleep(0.05)

    producer.flush()
