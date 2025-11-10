import json
import time
import random
from kafka import KafkaProducer
import os
import logging

def main():
    # Kafka broker connection settings
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"BOOTSTRAP_SERVERS: {bootstrap_servers}")
    topic_name = "user_events"

    # List of possible event types and lotteries
    lotteries = ["Lotto 6aus49", "Euro Jackpot", 'Keno']
    event_types = ['clicked', 'fill_out_the_order', 'register','pay']

    # Create the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    logging.info(f"Publishing lottery data to Kafka topic '{topic_name}'...")
    logging.info("Press Ctrl+C to stop.")

    try:
        while True:
            # Pick a random lottery name
            lottery_name = random.choice(lotteries)

            # Generate a random event type
            event_type = random.choice(event_types)

            # Current timestamp in milliseconds
            timestamp = int(time.time() * 1000)

            # Construct the event as a Python dictionary
            event = {
                "lottery_name": lottery_name,
                "event_type": event_type,
                "timestamp": timestamp
            }

            # Send the event to the specified Kafka topic
            producer.send(topic_name, event)
            producer.flush()  # Ensure the message is immediately pushed

            print(f"Sent event: {event}")

            # Sleep for sometime (adjust to control message rate)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        logging.error("\nStopping lottery data publisher.")
    finally:
        producer.close()
        logging.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
