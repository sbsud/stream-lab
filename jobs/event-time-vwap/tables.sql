-------------------------------------------------------------------------------------
--A reference of table DDLs that were used in this lab. As of now manually executed.
-------------------------------------------------------------------------------------

-- Table 1: proctime
CREATE TABLE trades_proc (
    symbol STRING, quantity BIGINT, price DOUBLE, side STRING, event_time BIGINT,
    proc_time AS PROCTIME()
) WITH (
    'connector'='kafka', 
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-vwap-proctime-2026-09-15_20-51',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'raw', 
    'key.fields' = 'symbol',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);

-- Table 2: event-time, 10s slack
CREATE TABLE trades_evt10 (
    symbol STRING, quantity BIGINT, price DOUBLE, side STRING, event_time BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'connector'='kafka', 
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-vwap-event10s-2026-09-15_20-51',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'raw', 
    'key.fields' = 'symbol',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);

-- Table 3: event-time, 60s slack
CREATE TABLE trades_evt60 (
    symbol STRING, quantity BIGINT, price DOUBLE, side STRING, event_time BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '60' SECOND
) WITH (
    'connector'='kafka', 
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-vwap-event60s-2026-09-15_20-51',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'raw', 
    'key.fields' = 'symbol',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);