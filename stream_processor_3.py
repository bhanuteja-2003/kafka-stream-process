import faust
from datetime import datetime
import statistics
from collections import defaultdict

class Log(faust.Record):
    timestamp: str
    service: str
    level: str
    message: str

app = faust.App(
    'error-stream-processor-3',
    broker='kafka://localhost:9092',
    store='memory://',
    web_port=6077
)

error_topic = app.topic('error_logs', value_type=Log)

# Store recent timestamps per service in memory , this is volatile
service_logs = defaultdict(list)

@app.agent(error_topic)
async def process_error_logs(logs):
    async for log in logs:
        ts = datetime.fromisoformat(log.timestamp)
        service_logs[log.service].append(ts)

@app.timer(interval=10.0)
async def print_mean_errors():
    for service, timestamps in service_logs.items():
        timestamps.sort()
        if len(timestamps) < 2:
            print(f"{service}: Not enough logs to calculate mean interval.")
            continue

        diffs = [
            (timestamps[i] - timestamps[i - 1]).total_seconds()
            for i in range(1, len(timestamps))
        ]
        mean_diff = statistics.mean(diffs)
        print(f"{service}: Mean time difference = {mean_diff:.2f} seconds")

if __name__ == '__main__':
    app.main()
