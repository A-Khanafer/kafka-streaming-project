from confluent_kafka import Producer

# This creates the Kafka producer and tells it where Kafka is.
# bootstrap.servers is basically asking "Where can I find at least one Kafka broker so I can connect to the Kafka cluster?"
# In our case, I exposed Kafka port 9092. So we use localhost:9092.
producer = Producer({
    "bootstrap.servers" : "localhost:9092"
})

# Sends "hellows kafka" to the Kafka topic we created earlier "network-events".
producer.produce(
    topic="network-events",
    value="hellows kafka"   
)

# Now this is important because Kafka producers works ASYNCHRONOUSLY. 
# produce() doesn't necessarily mean that the message has reached Kafka. It's more like it queued the message to be sent.
# Flush() waits until pending messages have actually been delivered before python exits.
producer.flush()

print("Message sent")