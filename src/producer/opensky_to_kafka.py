import json
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

TOPIC = "opensky_raw_states"
KAFKA_BOOTSTRAP = "kafka:9092"

BBOX = {
    "lamin": 40.5,
    "lomin": 46.0,
    "lamax": 55.5,
    "lomax": 87.5
}

def fetch_states():
    url = "https://opensky-network.org/api/states/all"
    r = requests.get(url, params=BBOX, timeout=20)
    r.raise_for_status()
    return r.json()

def run_once(producer: KafkaProducer):
    data = fetch_states()
    msg = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "bbox": BBOX,
        "payload": data
    }

    future = producer.send(TOPIC, json.dumps(msg).encode("utf-8"))
    record_md = future.get(timeout=20)
    producer.flush()

    states_count = len(data.get("states") or [])
    print(f"Sent to Kafka topic={record_md.topic} partition={record_md.partition} offset={record_md.offset}; states={states_count}")


def main(loop_seconds: int = 60):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    while True:
        run_once(producer)
        time.sleep(loop_seconds)

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    run_once(producer)

