import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode()
)

users = ["user_1", "user_2", "user_3"]

while True:
    user = random.choice(users)

    event = {
        "user": user,
        "action": "click",
        "timestamp": time.time()
    }

    producer.send(
        "events_topic",
        key=user,
        value=event
    )

    print(f"Sent event for {user}")

    time.sleep(5)