Today my goal was to handle any type of malformed JSON that could come from my producer so that my consumer doesn't crash. So I added simple try-catch logic and everything worked as normal.

BUT I realised i did some mistakes on my day 4 report but we are learning :)

So first of all `"auto.offset.reset": "earliest"` doesn't necessarily means that my consumer "remembers" where we stopped.

It only answers -> Where it should start if this consumer group does NOT have a valud committed offset...

Kafka remembering progress is related to committed offsets for our `group.id`.

**Our Config:**

```python
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id" : "network-monitor", 
    "auto.offset.reset" : "earliest", 
})
```

This means that Kafka can effectively track something like:

```text
network-monitor:
network-events
Partition 0
Committed offset: 7
```

Then if our consumer restarts... Kafka will check the commited offset and will start from there.

`auto.offset.reset = earliest` only comes into play if there is no usable committed offset!!!!!

Also there is something our current is doing without being explicitely said ... -> `enable.auto.commit = True` is the default setting in confluent-kafka. So Kafka periodically commits our consumer's offsets automatically. The default interval is about five seconds... When we think about it can give something interesting to experiment with in the future.

So after adding try/catch logic so that my consumer doesn't crash. I was curious about the commits in Kafka...

So basically there is 3 different ideas:

- **CONSUMED**: Consumer received the message from Kafka
- **PRODUCED**: Our app succesfully did something with it.
- **COMMITTED**: Kafka stired the consumer group's progress.

So here is the flow:

```text
Kafka | offset 10 ---> Consumer receives event (CONSUMED) ---> json.loads() --X app crashes (NOT SUCCESFULLY PROCESSED)
```

The question that emerges is did offset 10 already been comitted???

Wit automatic commits, it could be. Confluent specifically warns that automatic offset storage/committing can result in the latest offset being committed before app procession has fully completed.

That is the beginning of understanding delivery guarantees and why offset management matters!

So I tested with malformed events and everything worked. But I also stopped my consumer and tried to see from which offset it will start just to you know see the commit Kafka idea with my own eyes :)

Anyway I also learned some cool information about docker. I can access Kafka CLI. My first instinct was to download something but it isn't necessary...

Since Kafka is running inside a container inside Docker so the Kafka CLI tools are also inside the broker container!

```text
ahmad@admin:~/Documents/kafka-streaming-project$ docker compose exec broker bash

broker:/$ ls
__cacert_entrypoint.sh  bin  dev  etc  home  lib  lib64  media  mnt  opt  proc  root  run  sbin  srv  sys  tmp  usr  var

broker:/$ ls /opt/kafka/bin
connect-distributed.sh        kafka-cluster.sh                 kafka-delegation-tokens.sh  kafka-jmx.sh                  kafka-replica-verification.sh      kafka-streams-application-reset.sh  trogdor.sh
connect-mirror-maker.sh       kafka-configs.sh                 kafka-delete-records.sh     kafka-leader-election.sh      kafka-run-class.sh                 kafka-streams-groups.sh             windows
connect-plugin-path.sh        kafka-console-consumer.sh        kafka-dump-log.sh           kafka-log-dirs.sh             kafka-server-start.sh              kafka-topics.sh
connect-standalone.sh         kafka-console-producer.sh        kafka-e2e-latency.sh        kafka-metadata-quorum.sh       kafka-share-groups.sh               kafka-verifiable-consumer.sh
kafka-acls.sh                 kafka-console-share-consumer.sh  kafka-features.sh           kafka-metadata-shell.sh        kafka-share-consumer-perf-test.sh  kafka-streams-application-reset.sh  trogdor.sh
kafka-broker-api-versions.sh  kafka-consumer-groups.sh         kafka-get-offsets.sh        kafka-producer-perf-test.sh   kafka-storage.sh                    kafka-verifiable-producer.sh
kafka-client-metrics.sh       kafka-consumer-perf-test.sh      kafka-groups.sh             kafka-reassign-partitions.sh  kafka-verifiable-consumer.sh
kafka-console-producer.sh     kafka-dump-log.sh               kafka-log-dirs.sh           kafka-replica-verification.sh kafka-server-start.sh             kafka-topics.sh
kafka-consumer-groups.sh      kafka-get-offsets.sh             kafka-producer-perf-test.sh kafka-storage.sh              kafka-verifiable-producer.sh

broker:/$ /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker:29092 \
    --describe \
    --group network-monitor

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                  HOST            CLIENT-ID
network-monitor network-events  0          16              16              0               rdkafka-341f74fd-242a-4ac1-8d46-b9ff0f626132 /172.18.0.1     rdkafka
broker:/$ exit
exit
```

So this a is an exact copy paste of what I did.

First I ran a new bash process inside the already running broker container. (Every container has its own filesystem view, which is why I tried 'ls'). I was curious about seeing them.

Then `ls /opt/kafka/bin` was to see the Kafka CLI scripts that are installed inside the container.

And I ran a script with:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker:29092 \
    --describe \
    --group network-monitor
```

But I went ahead and tried understanding how docker works...

So first of all when we do `docker compose up` it spins up the container which is basically an isolated environment and inside it there is one main process. I our case, it is Kafka.

But when we ran `docker compose exec broker bash`. Docker starts another process inside the broker container.

And I was also wondering that if I spin up multiple containers of Kafka I am going to have so much useless files (when I did ls I saw them). But Docker does something clever so that it does not duplicate the entire filesystem for every container.

Docker images are built from shared read-only layers.

So the Kafka image has like the linux base layer, the Java layer, the Kafka layer. If we create 3 containers from the same Kafka image all 3 can reuse the same underlying image layers.

Docker isn't going to copy the whole Kafka installalation 3 times. Each container gets only a small writable layer on top. This is called a copy-on-write layered filesystem. If Container 1 modifies a file, Docker is going to store the changed version in Container 1's writable layer rather than modifying the shared image. So the extra disk usage comes mainly from what each container writes. That distinction is rather quite important since Kafka writes actual data.

In our current Compose file we have:

```yaml
KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

That data is currently being written into the container's writable filesystem. If we delete the container, the data will disappear with it. Later we could introduce a Docker volume! (It is basically persistent data even if the container is deleted the data stays alive).

So simple reminder for me...

**Image:**

> it's the reusable template/filesystem

**Container:**

> it's the isolated runtime env created from the Image

**Volume:**

> it's the persitent storage kept seperately from the Container.

Now back to the cript I ran...

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker:29092 \   #---> Tells the CLI to connect to the Kafka cluster (The script needs the address of at least one Kafka broker and remeber we are using broker:29092 since we are inside the container)
    --describe \   #---> Tells Kafka to show us the details about the consumer group.
    --group network-monitor   #---> We are specifying which consumer group we are interested in.
```

Now for the result:

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                  HOST            CLIENT-ID
network-monitor network-events  0          16              16              0               rdkafka-341f74fd-242a-4ac1-8d46-b9ff0f626132 /172.18.0.1     rdkafka
```

- **GROUP**: network-monitor (The group we wanted to check)
- **TOPIC**: network-events (Told us which topic the consumer group is consuming from)
- **PARTITION**: 0 (Kafka tracks consumer offsets per partition, not just per topic. If we later we had 3 partitions, this command could show three rows)
- **CURRENT-OFFSET**: 16 (This is the committed position of the network-monitor consumer group for partition 0. IT REPRESENTS THE NEXT OFFSET TO BE CONSUMED)
- **LOG-END-OFFSET**: 16 (This tells us where the Kafka partition currently ends)
- **LAG**: 0 (This tells us how far behind the consumer group is. LAG = LOG-END-OFFSET - CURRENT-OFFSET)

This is actually very important since in a real network monitoring system. If telemetry arrive faster than our service can process it, we would start to see consumer lag increasing.

- **CONSUMER-ID = rdkafka-341f74fd-...** (This identifies the specific currently running consumer instance.The Python library confluent-kafka uses librdkafka underneath, which is why we see rdkafka. Kafka needs to distinguish individual consumers because later we might have multiple consumers.)
- **HOST : /172.18.0.1** (This is the network address Kafka sees that consumer connecting from. Because Docker networking sits between them, Kafka sees a Docker-side address)
- **CLIENT-ID = rdkafka** (This identifies the type/name of Kafka client connecting. Now the default shows up, but we can change that inside the consumer config.)