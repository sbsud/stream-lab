# Acceptance tests

|Config|vwap (ACME 10:00-10-01)|Late records counted|
|---|---|---|
|Processing time||n/a|
|Event time 10s watermark slack|||
|Event time 60s watermark slack|||


**Success** Three numbers differ and the difference shrinks as the water mark slack increases. If the Processing time and event time agree then there is something wrong with the lateness injector and the code cannot be trusted.