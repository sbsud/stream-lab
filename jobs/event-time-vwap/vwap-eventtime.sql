SELECT symbol, SUM(price * quantity) / SUM(quantity), window_start, window_end, CURRENT_TIMESTAMP
FROM TABLE(
    TUMBLE(TABLE trades_evt10, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol;

SELECT symbol, COUNT(*), SUM(price * quantity) / SUM(quantity), window_start, window_end
FROM TABLE(TUMBLE(TABLE trades_evt10, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, symbol;


SELECT symbol, SUM(price * quantity) / SUM(quantity), window_start, window_end, CURRENT_TIMESTAMP
FROM TABLE(
    TUMBLE(TABLE trades_evt60, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol;

SELECT symbol, COUNT(*), SUM(price * quantity) / SUM(quantity), window_start, window_end
FROM TABLE(TUMBLE(TABLE trades_evt60, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, symbol;