import faust
import heapq

class Log(faust.Record):
    timestamp: str
    service: str
    level: str
    message: str

app = faust.App(
    'error-stream-processor-2',
    broker='kafka://localhost:9092',
    store='memory://',
    web_port=6067
)

log_topic = app.topic('logs', value_type=Log)
error_topic = app.topic('error_logs', value_type=Log)




@app.agent(log_topic)
async def process_logs(stream):
    async for log in stream:
        if log.level == 'ERROR':
            await error_topic.send(value=log)

if __name__ == '__main__':
    app.main()


