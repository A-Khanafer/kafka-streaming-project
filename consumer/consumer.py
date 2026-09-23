#Consumer will read events from Kafka.
from confluent_kafka import Consumer
# We need the Json Lib because Kafka doesn't understand Python dictionaries. KAFKA stores bytes.
import json


# Creating and instance of Consumer. The Dictionary inside contains its configuration.
consumer = Consumer({
    # Kafka Broker address like we said before since a python app RUNNING OUTSIDE DOCKKER is talking to Kafka we use localhost:9092
    "bootstrap.servers": "localhost:9092",
    # This is the Consumer Group is tells Kafka which consumer group this consumer belongs to.
    "group.id" : "network-monitor", 
    # This is basically where to start when there is no offset. It means if Kafka does not have a valid stored offset for this consumer group, start from the earliest available message.
    "auto.offset.reset" : "earliest", 
})

# I want messages from this topic (network-events)
# Also notice how this a list... That means we could subscribe to several topics...
consumer.subscribe(["network-events"]) 

# Checkpoint to see if the Consumer started succesfully
print("Waiting for events...")

# We did a try block, because if you see at the end we finish with finally : consumer.close()
# We want Kafka to be properly closed when the program exits.
try:
    while True:
        # Most important Line in the Loop...
        # pool() asks the Kafka consumer -> Give me a message if one is available.
        # The 1.0 is the timeout in seconds
        msg = consumer.poll(1.0)

        # When we receive nothing from the consumer the msg = None but a returned Kafka message can also represent an error condition.
        # 
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        # There's quite a lot happening on this one line.
        # msg.value() gets the Kafka message payload.
        # We .decode("utf-8") the message since we might receive it in bytes.
        # and finally json.loads() transforms it into a Python object
        # In our case it is a dictionary.
        event = json.loads(msg.value().decode(encoding="utf-8"))

        print("Received event:")
        print("Device: " + event["device_id"])
        print("Event Type: " + event["event_type"])
        print("Interface: " +event["interface"])
        print("\nKafka:")
        print("Topic: " + msg.topic())
        print("Partition: " + str(msg.partition()))
        print("Offset: " + str(msg.offset()))

finally:
    # This properly shuts down the Kafka consumer.
    consumer.close()
