import faust
import heapq

class Log(faust.Record):
    service: str
    level: str
    message: str

app = faust.App(
    'error-stream-processor',
    broker='kafka://localhost:9092',
    store='memory://'
)

log_topic = app.topic('logs', value_type=Log)
# error_counts = app.Table('error_counts', default=int) 
error_counts = app.Table('error_counts', default=int, partitions=3)

min_heap = []

@app.agent(log_topic)
async def process_logs(stream):
    async for log in stream:
        print(f"The log : {log.service}:{log.level}:{log.message}")
        if log.level == 'ERROR':
            error_counts[log.service] += 1
            
            min_heap.clear()
            for svc, count in error_counts.items():
                # If heap has less than 3 elements, push directly
                if len(min_heap) < 3:
                    heapq.heappush(min_heap, (count, svc))
                else:
                    # If current count is greater than the smallest in heap, replace
                    if count > min_heap[0][0]:
                        heapq.heappop(min_heap)
                        heapq.heappush(min_heap, (count, svc))

            # Sort heap in descending order for display (largest first)
            top_services = sorted(min_heap, key=lambda x: x[0], reverse=True)

            print("\n🔴 Top 3 services with most ERRORs :")
            for count, svc in top_services:
                print(f"{svc}: {count} errors")

if __name__ == '__main__':
    app.main()

# @app.timer(interval=10.0)
# async def show_top_services():
#     min_heap = []

#     for svc, count in error_counts.items():
#         # If heap has less than 3 elements, push directly
#         if len(min_heap) < 3:
#             heapq.heappush(min_heap, (count, svc))
#         else:
#             # If current count is greater than the smallest in heap, replace
#             if count > min_heap[0][0]:
#                 heapq.heappop(min_heap)
#                 heapq.heappush(min_heap, (count, svc))

#     # Sort heap in descending order for display (largest first)
#     top_services = sorted(min_heap, key=lambda x: x[0], reverse=True)

#     print("\n🔴 Top 3 services with most ERRORs (via Min Heap):")
#     for count, svc in top_services:
#         print(f"{svc}: {count} errors")



