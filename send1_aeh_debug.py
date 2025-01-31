import asyncio
import os
import socket
import time
from datetime import datetime
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.exceptions import EventHubError

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

async def run():
    while True:
        # Resolve DNS for Event Hub endpoint
        endpoint = EVENT_HUB_CONNECTION_STR.split(";")[0].split("=")[1].replace("sb://", "").replace("/", "")
        print(f"Resolving DNS for Event Hub endpoint: {endpoint}", flush=True)
        start_dns_time = time.time()
        resolved_ips = socket.gethostbyname_ex(endpoint)
        end_dns_time = time.time()
        print(f"Resolved IPs: {resolved_ips[2]}", flush=True)
        print(f"DNS resolution time: {end_dns_time - start_dns_time:.2f} seconds", flush=True)

        # Create a producer client to send messages to the event hub.
        start_producer_time = time.time()
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
        )
        end_producer_time = time.time()
        print(f"Producer client creation time: {end_producer_time - start_producer_time:.2f} seconds", flush=True)

        async with producer:
            try:
                # Create a batch.
                start_batch_time = time.time()
                event_data_batch = await producer.create_batch()
                end_batch_time = time.time()
                print(f"Batch creation time: {end_batch_time - start_batch_time:.2f} seconds", flush=True)

                # Get current datetime
                now = datetime.now()
                date_str = now.strftime("%y%m%d")
                time_str = now.strftime("%H%M%S")

                # Add events to the batch with datetime stamp.
                start_add_time = time.time()
                event_data_batch.add(EventData(f"First event {date_str} {time_str}"))
                event_data_batch.add(EventData(f"Second event {date_str} {time_str}"))
                event_data_batch.add(EventData(f"Third event {date_str} {time_str}"))
                end_add_time = time.time()
                print(f"Event addition time: {end_add_time - start_add_time:.2f} seconds", flush=True)

                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)
                print("Events successfully sent.", flush=True)
            except EventHubError as e:
                print(f"Failed to send events: {e}", flush=True)

        # Sleep for 60 seconds before repeating the operation
        await asyncio.sleep(60)

# Run the async function
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())