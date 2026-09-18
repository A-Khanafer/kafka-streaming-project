# Step 1:

I created a producer and a consumer that are going to use Kafka to respectively send events and read events.
─────────────────────────────────────

```text
producer.py
    |
    | localhost:9092
    v
┌────────────────────────────────────┐
│ Docker                             │
│                                    │
│ Kafka broker                       │
│                                    │
│ :9092 <──────── exposed to host    │
│ :29092 <─────── Docker-internal    │
│ :29093 <─────── controller         │
└────────────────────────────────────┘
    ^
    | localhost:9092
    |
consumer.py
```

Now there are multiple ports with Kafka. but each has a purpose.


- PORT `9092` : Let's application that are not inside docker reach it. (My producer and consumer)
- PORT `29092`: If I spin up another container later it will communicate with Kafka via `broker:29092` not `localhost` because I named the Kafka service `broker` in my Docker Compose file, so Docker uses `broker` as the network name other containers can use to reach Kafka. 
- PORT `29093`: This is Kafka's internal cluster-management communication. If later in the project I have more nodes the controller will be the one to coordinate things like cluster metadata and leadership.

Then I started by creating the `docker-compose` file to spin up Kafka.
Made sure everything was running correctly and then I created a topic inside Kafka. (`network-events`)

- A topic is the named stream where Kafka stores messages.

## -> CREATION OF THE TOPIC :

```
    docker exec broker /opt/kafka/bin/kafka-topics.sh \
     --create \
     --topic network-events \
     --bootstrap-server localhost:9092 \
     --partitions 1 \ #I chose 1 partition
     --replication-factor 1 #means there is only one copy of the data

```

```text
     PARTITIONS : They are the streams where the data is saved. (Think of it like a list or an array. If you want to acces the first event (event-1) you will need to fetch it from partition-0 via offset 0)
     ─────────────────────────────────────
     OFFSETS    :  0    1    2    3    4
     Partition 0: e-1, e-2, e-3, e-4, e-5
     Partition 1: e-6, e-7, e-8, e-9, e-10

     event = (topic, partition, offset) so:
        e-1 = (network-events, 0, 0)
        e-9 = (network-events, 1, 3)

     A partition is basically an ordered append-only list of events, and each partition has its own offsets starting at 0.
     The way Kafka sends events in a partition is not well represented we will get into it later.
```

## -> VERIFIED SUCCESFUL TOPIC CREATION :

```
    docker exec broker /opt/kafka/bin/kafka-topics.sh \
     --list \
     --bootstrap-server localhost:9092
```

Ran :

```
git init #Initialize the git repo.
      git add . #Adds every file into the staged changes
      git commit -m "Initial Kafka project setup" #Commits to git -m is for a message

      #Create and empty github project and connect it
      git branch -M main 
      git remote add origin https://github.com/A-Khanafer/kafka-streaming-project.git
      git push -u origin main
```
