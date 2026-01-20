import json
import os
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = "end-to-end-de-pipeline"
TOPIC_ID = "air-quality"
WAQI_TOKEN = "e6f1c78090e8f66480f4e601c2cfbf1a28289dea"
CITY = "hyderabad"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_waqi(request):
    url = f"https://api.waqi.info/feed/{CITY}/?token={WAQI_TOKEN}"

    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()

    message = {
        "source": "waqi",
        "city": CITY,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "event_time": payload["data"]["time"]["iso"],
        "raw_payload": payload
    }

    publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )

    return "Published", 200
