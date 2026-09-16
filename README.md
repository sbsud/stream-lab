# stream-lab

## Intent

This is a local lab that I created to explore semantics of streaming applications. This is largely informed by the work on streaming that I had done many years ago. However it is not an attempt to reproduce that work in any way. This lab is intended to consolidate that work and go beyond. It should be possible to reproduce the numbers generated here on local or rentable hardware depending on the stage of completion of this lab. No content of this lab is AI generated. The overall intent is learning and exploration.

## Where this stands

The current phase rebuilds event-time correctness for a windowed aggregation — VWAP on a simulated trade stream, compared under processing time and event time with two different watermark slacks. The write-up for this phase is here: `[link once published]`.

Each phase adds modules beside the ones before it rather than rewriting them, so `loadgen`, the Avro schema, and the platform compose file are meant to stay stable as the lab grows. Folders for later phases (`sinks/`, `recon/`) exist as stubs with a one-line README until they're built out.

## Prerequisites

- Docker and Docker Compose
- JDK 17 or later, for building and running `loadgen`
- Maven
- `make`

## Quickstart

```bash
git clone https://github.com/sbsud/stream-lab.git
cd stream-lab
make up      # brings up Kafka (KRaft), Schema Registry, and Flink
```

Running the actual comparison takes three separate Flink SQL client sessions, one per notion of time — each `SELECT` is a long-running, blocking statement, and the three need to run side by side so their outputs can be compared. In three separate terminals:

```bash
make fsqli #interactive Flink SQL session
```
OR 
```bash
docker compose -f platform/docker-compose.yaml exec jobmanager ./bin/sql-client.sh
```

In each session, 
- Set parallelism to 1 `SET 'parallelism.default' = '1';` as the task manager slots have been set to 4 and we have 3 sessions running. *Not setting this will result in `parallelism.default` to be 4 and will result in only on of the queries below to run, with others erroring due to lack of available slots.* Setting parallelism to 1 is just a quick hack to run on my laptop.
- Run the matching table DDL 
- Query from `jobs/event-time-vwap` — one for processing time, one for event time with 10s watermark slack, one for event time with 60s slack. 

Each table needs its own `properties.group.id` and `'scan.startup.mode' = 'earliest-offset'`, since Flink's default catalog is in-memory and doesn't persist across sessions. The `SELECT` queries count records per symbol and window.

With all three sessions running and waiting, start the producer in a fourth terminal:

```bash
make load_build #runs a mvn clean install on the load codebase.
make load    # runs the producer against lab.yaml's settings
```

The three sessions will start filling in with counts as records arrive. Pick a common symbol and window across all three, and the VWAP and record counts should differ — that's the comparison this phase is built to show.

`make down` tears everything down. `make nuke` to get rid of volumes. If something looks half-broken mid-run, one of these would be the fastest way back to a clean state.

All the switchable settings — rate, symbol set, lateness fraction, watermark slack — live in `lab.yaml`. Nothing about running this lab needs a cloud account or an API key.

## Ports - reference

- Kafka broker => 9092
- Schema registry => 8081
- Flink Job manager => 8082
- Prometheus => 9090
- Grafana => 3000

## Repo layout

- **`loadgen/`** — the Java producer. Config-driven, deterministic under a fixed seed, meant to be reused as the lab grows.
- **`schemas/`** — the Avro record definitions.
- **`platform/`** — the Docker Compose setup: Kafka in KRaft mode, Schema Registry, Flink.
- **`jobs/`** — the Flink SQL for each phase — table DDL and queries.
- **`sinks/`, `recon/`** — stubs for phases not built yet.
- **`docs/`** — running notes from building each phase, including the failures hit along the way.

## Setting expectations

If a phase's write-up says something was a prototype, or that a result was incomplete, that's stated on purpose. The numbers in this repo are meant to be reproducible on the hardware the README says they were reproducible on — if you clone this and can't get the same or similar result, let me know.
