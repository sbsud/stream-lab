--SELECT query that calculates the vwap using processing time windows. As of now to be run manually.
SELECT symbol, SUM(price * quantity) / SUM(quantity), window_start, window_end, CURRENT_TIMESTAMP AS emitted_at
FROM TABLE(
    TUMBLE(TABLE trades_proc, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol;
