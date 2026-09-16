---------------------------------------------------------------------------
-- A Reference of Event time SELECT queries. As of now to be run manually.
---------------------------------------------------------------------------

-- Runs on the trades_evt10 table(tables.sql) - Watermark with 10s slack
SELECT symbol, COUNT(*) as sum_count, SUM(price * quantity) / SUM(quantity) as vwap, window_start, window_end, CURRENT_TIMESTAMP AS emitted_at
FROM TABLE(TUMBLE(TABLE trades_evt10, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, symbol;

-- Runs on the trades_evt60 table(tables.sql) - Watermark with 60s slack
SELECT symbol, COUNT(*) as sum_count, SUM(price * quantity) / SUM(quantity) AS vwap, window_start, window_end, CURRENT_TIMESTAMP AS emitted_at
FROM TABLE(TUMBLE(TABLE trades_evt60, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, symbol;

