import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "events_topic",
    bootstrap_servers="localhost:29092",
    group_id="events_group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode())
)

event_counter = {}

for message in consumer:
    user = message.value["user"]

    if user not in event_counter:
        event_counter[user] = 0

    event_counter[user] += 1

    print("Partition:", message.partition)
    print("User:", user)
    print("Current Count:", event_counter)
    print("-" * 40)