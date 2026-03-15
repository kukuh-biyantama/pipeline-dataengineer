from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta, timezone

# Kafka configuration
TOPIC = "transactions"

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sources = ["mobile", "web", "pos"]


# Generate valid event
def generate_valid_event():
    return {
        "user_id": f"U{random.randint(1000,9999)}",
        "amount": random.randint(1000,500000),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "source": random.choice(sources)
    }


# Invalid events
invalid_events = [

    # negative amount
    {
        "user_id": "U9001",
        "amount": -500,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "source": "mobile"
    },

    # amount too large
    {
        "user_id": "U9002",
        "amount": 99999999,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "source": "web"
    },

    # invalid timestamp
    {
        "user_id": "U9003",
        "amount": 20000,
        "timestamp": "invalid_timestamp",
        "source": "pos"
    },

    # unknown source
    {
        "user_id": "U9004",
        "amount": 15000,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "source": "telegram"
    }
]


# Late events (older than watermark 3 minutes)
late_events = [

    {
        "user_id": "U8001",
        "amount": 10000,
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat().replace("+00:00","Z"),
        "source": "mobile"
    },

    {
        "user_id": "U8002",
        "amount": 20000,
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat().replace("+00:00","Z"),
        "source": "web"
    },

    {
        "user_id": "U8003",
        "amount": 30000,
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=7)).isoformat().replace("+00:00","Z"),
        "source": "pos"
    }
]


# Duplicate events
duplicate_event = {
    "user_id": "U7777",
    "amount": 12000,
    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
    "source": "mobile"
}


events_pool = invalid_events + late_events


print("Starting Kafka Producer...")
print("Sending events every 1–2 seconds\n")


while True:

    r = random.random()

    # 60% valid events
    if r < 0.6:
        event = generate_valid_event()

    # 20% invalid events
    elif r < 0.8:
        event = random.choice(invalid_events)

    # 10% late events
    elif r < 0.9:
        event = random.choice(late_events)

    # 10% duplicate events
    else:
        event = duplicate_event

    producer.send(TOPIC, event)
    producer.flush()

    print("Sent event:", event)

    time.sleep(random.randint(1, 2))