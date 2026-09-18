# Today I am cloning the github repo on my desktop.

```
Simple... git clone https://github.com/A-Khanafer/kafka-streaming-project.git #into my desired folder.
```

and we are ready to go!

## So, after doing the consumer this is what I understood.

(Look at the code before reading that part for an easier time understanding)

Creating and instance of `Consumer`. The Dictionary inside contains its configuration. It takes a Dictionary as a config.

```python
consumer = Consumer({...})
```


Kafka Broker address like we said before since a python app RUNNING OUTSIDE DOCKKER is talking to Kafka we use `localhost:9092`

```python
"bootstrap.servers": "localhost:9092",
```

But what I learned on top of this is that it does not necessarily mean Kafka has only one server. In a real Kafka cluster you could provide several:

```python
"bootstrap.servers": "broker1:9092,broker2:9092,broker3:9092"
```

This could represent three Kafka brokers running on: 3 physical servers / 3 VM / 3 docker containers
What multiple Kafka brokers can give us is scalability and fault tolerance.
Imagine a topic: 

```text
    Partition 0
    Partition 1
    Partition 2
```

Kafka can distribute those partitions so that the workload doesn't go trough one machine. It can replicate the partitions so that if one the broker dies Kafka can promote one of the replicas.

So this : `"bootstrap.servers": "broker1:9092,broker2:9092,broker3:9092"` does not mean that we are sending every message to all thress brokers.
It's more like we are giving the producer/consumer multiple entrances to the Kafka cluster.

So `bootstrap` is a good name. In out case since we supplied one broker if that broker happened to go down when we start our app the app couldn't initially discover the cluster.
Giving several addresses makes startup more resilient.


This is the Consumer Group is tells Kafka which consumer group this consumer belongs to.

```python
"group.id" : "network-monitor",
```

```text
Consumer
    |
    v
group = network-monitor
```

Really simple, the name is abritrary we can chose whatever we want. But basically it should describe what the services does.

The important thing that we need to keep in mind is that Kafka tracks consumption per consumer group...
Kafka can remember something like -> `network-monitor` has processed up to offset 3
Another app could use : `audit-service` and Kafka would track its progress independently.

But this is brief since consumer groups are one of Kafka's most important concepts (based on Chatgpt)
                                                        

This is basically where to start when there is no offset. It means if Kafka does not have a valid stored offset for this consumer group, start from the earliest available message.

```python
"auto.offset.reset" : "earliest",
```

Another possible setting is: `"auto.offset.reset": "latest"` which would tell a new consumer group to start around the newest messages and wait for new ones.


I want messages from this topic (`network-events`)...

```python
consumer.subscribe(["network-events"])
```

Also notice how this a list... That means we could subscribe to several topics...


Now for the loop we went with a `while True:` because consumer are generally long-running services.
In the loop we poll kafka with `msg = consumer.poll(1.0)`
`pool()` asks the Kafka consumer -> Give me a message if one is available. The 1.0 is the timeout in seconds

CONCEPTUALLY (Chatgpt)

```text
    consumer
    |
    | poll(1.0)
    v
    Kafka

    "Anything available?"

    Kafka:
        yes -> return message
        no  -> return None after timeout
```

Now when we `poll()` from the consumer and we receive nothing the `msg` might be `None` or non-empty.
If it's None we just try again. But if it is non-empty we can not assume we have a good payload. It might be an error. So if it is and error we print the error message and we loop back.


```python
event = json.loads(msg.value().decode("utf-8"))
```

There's quite a lot happening on this one line. msg.value() gets the Kafka message payload.
We .decode("utf-8") the message since we might receive it in bytes

```text
        bytes
        ↓ decode
        string
```

Finally json.loads() transforms it into a Python object. Usually in our case it will a dictionary.
So the full transformation is:

```text
Kafka
    b'{"device": "router-01"}' # The b means it's in bytes.
            ↓
        .decode()
            ↓
    '{"device": "router-01"}'
            ↓
        json.loads()
            ↓
    {"device": "router-01"}
Python dictionary
```


`consumer.close()` properly shuts down the Kafka consumer.
This matters because the consumer isn't just a Python object sitting by itself. It participates in a Kafka consumer group.
When it leaves, Kafka needs to know:
    `network-monitor` consumer has left
Kafka can then adjust which consumers are responsible for which partitions.
That becomes especially important when we will have multiple consumers.


## THE ENTIRE LOGIC :

```text
Create consumer
      ↓
Connect to Kafka
      ↓
Subscribe to topic
      ↓
┌──────────────────┐
│ Poll for message │◄──────┐
└────────┬─────────┘       │
         ↓                 │
   No message? ────────────┤
         ↓ no              │
      Error? ──────────────┤
         ↓ no              │
    Get message            │
         ↓                 │
    Decode bytes           │
         ↓                 │
    Parse JSON             │
         ↓                 │
   Python dictionary       │
         ↓                 │
      Print it ────────────┘
```

The most important thing to understand at this stage is that Kafka isn't calling our consumer. Our consumer is asking data from Kafka in a loop.


## Now I tried running the whole thing and got and error...

Basically after some debuggin I found out that my producer isn't sending a dict... it's sending a string "hellows kafka".
So I can't `json.loads()` a string. And I wrote `json.load()` which is different then `json.loads()` since load tries to open a file, while the other one loads does what we want...
