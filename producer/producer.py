from confluent_kafka import Producer
import json

# This creates the Kafka producer and tells it where Kafka is.
# bootstrap.servers is basically asking "Where can I find at least one Kafka broker so I can connect to the Kafka cluster?"
# In our case, I exposed Kafka port 9092. So we use localhost:9092.
producer = Producer({
    "bootstrap.servers" : "localhost:9092"
})

#Create a python dict to simulate a network event
event = {
    "event_type": "Interface DOWN",
    "device_id": "SRX-4600",
    "interface": "ge-0/0/1",
    "status": "DOWN",
    "timestamp": "2026-09-22"
}

#Transform the python dict into a json str and encode it into bytes
event_in_bytes = json.dumps(event).encode(encoding="utf-8")

# Sends our event in bytes to the Kafka topic we created earlier "network-events".
producer.produce(
    topic="network-events",
    value=event_in_bytes   
)

# Now this is important because Kafka producers works ASYNCHRONOUSLY. 
# produce() doesn't necessarily mean that the message has reached Kafka. It's more like it queued the message to be sent.
# Flush() waits until pending messages have actually been delivered before python exits.
producer.flush()

print("Message sent")