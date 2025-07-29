import faust
import heapq

class Log(faust.Record):
    timestamp: str
    service: str
    level: str
    message: str

app = faust.App(
    'error-stream-processor-1',
    broker='kafka://localhost:9092',
    store='memory://'
)

log_topic = app.topic('logs', value_type=Log)
error_counts = app.Table('error_counts', default=int, partitions=3)

min_heap = []

@app.agent(log_topic)
async def process_logs(stream):
    async for log in stream:
        if log.level == 'ERROR':
            error_counts[log.service] += 1
            
            min_heap.clear()
            for svc, count in error_counts.items():
                
                if len(min_heap) < 3:
                    heapq.heappush(min_heap, (count, svc))
                else:
                    
                    if count > min_heap[0][0]:
                        heapq.heappop(min_heap)
                        heapq.heappush(min_heap, (count, svc))

            # Sort heap in descending order for display (largest first)
            top_services = sorted(min_heap, key=lambda x: x[0], reverse=True)

            print("\nTop 3 services with most ERRORs :")
            for count, svc in top_services:
                print(f"{svc}: {count} errors")

if __name__ == '__main__':
    app.main()





