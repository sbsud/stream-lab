```bash
python3 -c "
import datetime
start = datetime.datetime(2026,9,14,15,9,0, tzinfo=datetime.timezone.utc)
end   = datetime.datetime(2026,9,14,15,10,0, tzinfo=datetime.timezone.utc)
print(int(start.timestamp()*1000), int(end.timestamp()*1000))
"
```


```bash
kcat -C -b localhost:9092 -t trades -r http://localhost:8081 -s value=avro -o beginning -e -q \
  | grep '"symbol": "CCCC"' \
  | python3 -c "
import sys, json
start, end = 1789398540000, 1789398600000  # paste the two numbers from above
n = 0
for line in sys.stdin:
    rec = json.loads(line)
    if start <= rec['event_time'] < end:
        n += 1
print(n)
"
```
response(true_count) : 773

late records dropped = true_count (kcat)  −  COUNT(*) (that config's query)

Slack 10 seconds
late records dropped = 773 - 701 = 72

Slack 60 seconds
late records dropped = 773 - 773 = 0