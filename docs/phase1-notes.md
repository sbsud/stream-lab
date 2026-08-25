# Acceptance tests

|Config|vwap (ACME 10:00-10-01)|Late records counted|
|---|---|---|
|Processing time||n/a|
|Event time 10s watermark slack|||
|Event time 60s watermark slack|||


**Success** Three numbers differ and the difference shrinks as the water mark slack increases. If the Processing time and event time agree then there is something wrong with the lateness injector and the code cannot be trusted.


# Problems
- **kraft/Kafka config** : Not a problem as such, but the working broker config that I arrived by trial and error at is as follows

```
services:
  broker:
    image: confluentinc/cp-kafka:7.8.0
    hostname: kafka
    container_name: kafka_cont
    ports:
      - 9092:9092
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,EXTERNAL://0.0.0.0:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,EXTERNAL://localhost:9092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT'
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'  
      KAFKA_JMX_PORT: 9999
    volumes:
      - kafka-data:/var/lib/kafka/data

volumes:
  kafka-data:

```

Also running `docker compose` without the `-d` helped debug errors interactively and supply the necessary config.

- Adding a `.env` file in the root directory did not result in environment variables defined there being picked up. I had to use a `--env-file` flag like below.
```
docker compose --env-file .env -f platform/docker-compose.yaml up -d
```

While bringing up the schema registry faced issues with the bootstrap being rejected due to the fact that I was using a name `INTERNAL` as the internal name of the broker and using the same as a protocol while bootstapping the registry (`SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`). The registry's bootstrap accepts an entry from a set of protocols (PLAINTEXT, SASL_PLAINTEXT, SSL or SASL_SSL). I confused that with using the internal name of the broker listener. 

The schema registry startup then started failing with a timeout. It would read the `_schemas` topic from the broker successfully and then timeout when trying to join the consumer group. The Join  consumer group depends on the group coordinator existing, the `__consumer_offsets` partition has elected a leader. This did not happen because I had spun up a single broker and the default replication factor is 3. This was fixed when I set the replication factor to 1(`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`). 

Here are some of the debug commands that were used for this.
```
  docker compose --env-file .env -f platform/docker-compose.yaml exec broker \
  env | grep -iE "OFFSETS_TOPIC_REPLICATION|TRANSACTION_STATE"
```
expecting
```
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
```
Optionally check the whole kafka config to check if something else is misapplied.
```
docker compose --env-file .env -f platform/docker-compose.yaml exec broker \
  env | grep -i kafka | sort
```

Describe the `__consumer_offsets` topic to check if the topic exists correctly. 
```
docker compose --env-file .env -f platform/docker-compose.yaml exec broker \
  kafka-topics --bootstrap-server broker:29092 \
  --describe --topic __consumer_offsets
```
Should show 50 lines(partitions) that look like this.
```
Topic: __consumer_offsets	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
```
Note the Leader: 1 and Replicas:1.
Leader: 1 means broker 1 owns it and it can serve. Leader: none or Leader: -1 means no broker could take it.

Flink taskmanagers do not need an external port-mapping as the taskmanager communicates with the Job manager via RPC and the JM UI shows the tasks statuses for jobs deployed at the TM.