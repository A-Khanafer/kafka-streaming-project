# Day 04

Simple code today, but some theory and testing.

## Producer

First, I wanted to stop sending `"hellows kafka"` and send a real event.

So I created a Python dictionary:

```python
event = {
    "event_type": "Interface DOWN",
    "device_id": "SRX-4600",
    "interface": "ge-0/0/1",
    "status": "DOWN",
    "timestamp": "2026-09-22"
}
```

Since Kafka only accepts bytes to send via the producer, I needed to encode my event dictionary. We did this with:

```python
json.dumps(event).encode(encoder="utf-8")
```

`json.dumps()` takes our Python dictionary and transforms it into a valid JSON string. `encode()` transforms our JSON string into bytes.

That was for our producer.

## Consumer

I did not really change anything, but I did a lot of testing. At first, I was wrong: `json.loads()` works with a string, but `"hellows kafka"` was not a valid JSON string, which is why it was not working.

What I did was load the decoded bytes sent from our producer:

```python
json.loads(msg.value().decode(encoding="utf-8"))
```

What I realised is that I can keep sending events through Kafka and the consumer keeps listening. The message that the producer sends contains its topic, partition, and offset.

Even when I restart the consumer, since we configured the consumer with:

```python
"auto.offset.reset": "earliest"
```

it keeps track of the offset.

When a JSON object is malformed and the consumer tries to load the malformed event, the consumer crashes. That would be a good case for exception handling, since we are not going to stop everything because of one malformed Kafka event.
