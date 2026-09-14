# Acceptance tests

|Config|VWAP(CCCC, 15:09–15:10)|Late records counted|
|---|---|---|
|Processing time|502.96|n/a|
|Event time 10s watermark slack|501.19|72 out of 773 dropped|
|Event time 60s watermark slack|509.59|0 dropped|


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

Using the KafkaAvroSerializer with the schema-registry config while producing messages allows a registry-configured consumer to deserialize the messages from the kafka topic. Using a custom serializer results in valid Avro, but the registry-aware consumer was not able to retrieve the schema-registry framing.

chose double for phase 1; the correct financial type is Avro decimal logical type → BigDecimal → Flink DECIMAL(18,4); the reason is accumulation error in summation, not the cosmetic display expansion

Compiles-in-IDE-fails-in-build — two-arg Random.nextInt(origin, bound) is JDK 17+, the build was pinned to Java 8, IDE used the project SDK. "Compiles in IDE, fails in Maven" = different language levels.. Determinism verified by diffing two runs — identical except event_time, which is correct because event time is wall-clock and shouldn't be seeded. lateness and field values draw from one sequence, so reproducibility holds within a config but not across configs.

## Phase 1 acceptance test — final result

- Acceptance test symbol/window: `CCCC`, window `2026-09-14 15:09:00–15:10:00`
- True membership for that window (via kcat against the raw topic): 773 records
- Processing-time VWAP: 502.96 (late-record concept doesn't apply — n/a)
- Event-time, 10s slack: VWAP 501.19, captured 701 of 773 records, 72 dropped
- Event-time, 60s slack: VWAP 509.59, captured all 773 records, 0 dropped
- Late-record count per window is not exposed directly by Flink SQL; derived by adding `COUNT(*)` to each windowed query and subtracting from an independently-counted kcat total filtered to that symbol and event-time range

## Partition/parallelism mismatch (watermark stall)

- A topic created with fewer partitions than the job's parallelism leaves some source subtasks permanently unassigned — not idle, structurally empty, since there's no partition behind them
- A subtask that has never processed a record never produces a watermark at all, since a watermark generator has nothing to compute from
- `GROUP BY` executes as local pre-aggregate → hash shuffle (by key) → global aggregate; because a hash shuffle can route any key to any downstream subtask, every global subtask keeps a permanent input channel from every local subtask
- A downstream operator's combined watermark is the minimum across all its input channels, including ones that have never delivered anything — so 3 permanently-silent channels freeze every global subtask, for every key, not just the ones fed by the empty partitions
- `idle-timeout` doesn't fix this case — it's for a real partition that's temporarily quiet; there's no data source behind a genuinely unassigned subtask to time out from
- Fix: recreate the topic with partition count ≥ job parallelism
- `PROCTIME` windows are unaffected by this entire mechanism — they fire on each subtask's own wall-clock timer, with no watermark propagation or cross-subtask coordination at all, which is why the same broken 1-partition topic produced correct proctime output but frozen event-time output

## Concurrent jobs and task slots

- Running N Flink jobs concurrently needs (parallelism × N) ≤ total task slots in the cluster, or the excess jobs sit in `RESTARTING` with `NoResourceAvailableException` — a scheduling failure, not a data/query problem
- Fix for running several small comparison queries on a small cluster: `SET 'parallelism.default' = '1'` per session before creating that session's table
- Lowering parallelism doesn't affect correctness for a small topic — one subtask can read multiple Kafka partitions fine, just serially instead of in parallel

## Operational gotchas

- Schema Registry: the default RF-3 replication factor for `__consumer_offsets` can cause silent timeouts in single-broker lab setups — a non-obvious failure mode worth documenting early
- Rate pacing: absolute nanosecond scheduling is more accurate than fixed-gap sleep for high-rate load generation; determinism can be verified by diffing two runs
- Kafka topic deletion is asynchronous (`--delete` marks for removal, doesn't block until gone) — an immediate `--create` can hit `TopicExistsException`; poll `--list` until the topic disappears, or just recreate the whole platform volume for a clean slate
- `WATERMARK FOR <col>` requires `<col>` to be `TIMESTAMP`/`TIMESTAMP_LTZ`-typed, not a raw numeric epoch field — an epoch-millis `BIGINT` needs a computed column via `TO_TIMESTAMP_LTZ(col, 3)` first
- A table can carry both a `PROCTIME()` computed column and a watermarked event-time column at once; different queries can window on either — but each query becomes an independent Kafka consumer job, and two sharing one `group.id` concurrently will split partitions between them rather than each reading the full topic
- Flink SQL's default catalog is in-memory and session-scoped — a `CREATE TABLE` from one CLI invocation doesn't exist in the next; every session that needs a table must recreate it
- `sql-client.sh -f <file>` is reliable for running `.sql` files; the interactive `SOURCE <file>` command is fragile and should be avoided
- Backlog replay under `PROCTIME` can collapse a long original production timeline into one processing-time window, since `proc_time` reflects when Flink reads a record, not when it was produced, and Flink reads a Kafka backlog far faster than the original production rate
