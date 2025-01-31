import os
import socket
import time
from datetime import datetime
from confluent_kafka import Producer

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

def get_bootstrap_servers(connection_str):
    # Extract the hostname from the connection string
    for part in connection_str.split(";"):
        if part.startswith("Endpoint"):
            return part.split("=")[1].replace("sb://", "").replace("/", "")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}", flush=True)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]", flush=True)

def run():
    while True:
        # Resolve DNS for Event Hub endpoint
        endpoint = get_bootstrap_servers(EVENT_HUB_CONNECTION_STR)
        print(f"Resolving DNS for Event Hub endpoint: {endpoint}", flush=True)
        start_dns_time = time.time()
        resolved_ips = socket.gethostbyname_ex(endpoint)
        end_dns_time = time.time()
        print(f"Resolved IPs: {resolved_ips[2]}", flush=True)
        print(f"DNS resolution time: {end_dns_time - start_dns_time:.2f} seconds", flush=True)

        # Create a producer client to send messages to the event hub.
        start_producer_time = time.time()
        producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': '$ConnectionString',
            'sasl.password': EVENT_HUB_CONNECTION_STR
        })
        end_producer_time = time.time()
        print(f"Producer client creation time: {end_producer_time - start_producer_time:.2f} seconds", flush=True)

        try:
            # Produce messages to the event hub with datetime value.
            now = datetime.now()
            date_str = now.strftime("%y%m%d")
            time_str = now.strftime("%H%M%S")

            start_add_time = time.time()
            producer.produce(EVENT_HUB_NAME, key="key1", value=f"First event with Kafka {date_str} {time_str}", callback=delivery_report)
            producer.produce(EVENT_HUB_NAME, key="key2", value=f"Second event with Kafka {date_str} {time_str}", callback=delivery_report)
            producer.produce(EVENT_HUB_NAME, key="key3", value=f"Third event with Kafka {date_str} {time_str}", callback=delivery_report)
            end_add_time = time.time()
            print(f"Event addition time: {end_add_time - start_add_time:.2f} seconds", flush=True)

            # Wait for any outstanding messages to be delivered and delivery reports to be received.
            producer.flush()
            print("Events successfully sent.", flush=True)
        except Exception as e:
            print(f"Failed to send events: {e}", flush=True)

        # Sleep for 60 seconds before repeating the operation
        time.sleep(60)

# Run the function
if __name__ == "__main__":
    run()