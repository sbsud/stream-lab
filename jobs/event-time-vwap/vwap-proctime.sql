SELECT symbol, SUM(price * quantity) / SUM(quantity), window_start, window_end, CURRENT_TIMESTAMP
FROM TABLE(
    TUMBLE(TABLE trades_proc, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol;