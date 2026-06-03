"""
Kafka Producer for Log Events

Produces synthetic log events to a Kafka topic for streaming consumption.
"""

import json
import time
import sys
import argparse
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

DEFAULT_API_VERSION = (3, 5, 0)

try:
    from scripts.log_generator import SyntheticLogGenerator
except ModuleNotFoundError:
    # Allow running as `python scripts/kafka_producer.py` from project root.
    from log_generator import SyntheticLogGenerator


def create_kafka_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """
    Create a Kafka producer.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        KafkaProducer instance
    """
    try:
        compression_type = 'snappy'
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            api_version=DEFAULT_API_VERSION,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            compression_type=compression_type
        )
        print(f"Connected to Kafka: {bootstrap_servers} (compression: {compression_type})")
        return producer
    except Exception as e:
        # If optional snappy libs are unavailable, retry without compression.
        if "snappy" in str(e).lower():
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(','),
                api_version=DEFAULT_API_VERSION,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type=None
            )
            print(f"Connected to Kafka: {bootstrap_servers} (compression: none)")
            return producer

        print(f"Failed to connect to Kafka: {e}")
        raise


def send_log_batch(producer: KafkaProducer,
                   topic: str,
                   logs: list,
                   delay_ms: int = 100):
    """
    Send a batch of logs to Kafka.
    
    Args:
        producer: KafkaProducer instance
        topic: Kafka topic to send to
        logs: List of log records
        delay_ms: Delay between batches in milliseconds
    """
    for log in logs:
        try:
            future = producer.send(topic, value=log)
            # Wait for the send to complete
            record_metadata = future.get(timeout=10)
            print(f"Sent log to {record_metadata.topic}[{record_metadata.partition}] "
                  f"at offset {record_metadata.offset}")
        except KafkaError as e:
            print(f"Failed to send log: {e}")
    
    if delay_ms > 0 and logs:
        time.sleep(delay_ms / 1000.0)


def produce_logs(bootstrap_servers: str,
                topic: str,
                num_batches: int,
                batch_size: int = 100,
                delay_ms: int = 100):
    """
    Continuously produce logs to Kafka.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Target Kafka topic
        num_batches: Number of batches to produce
        batch_size: Size of each batch
        delay_ms: Delay between batches in milliseconds
    """
    # Create producer
    producer = create_kafka_producer(bootstrap_servers)
    
    # Initialize log generator
    services = ["auth-service", "api-gateway", "payment-service", "user-service", "notification-service"]
    hosts = ["host-01", "host-02", "host-03", "host-04", "host-05"]
    
    generator = SyntheticLogGenerator(
        services=services,
        hosts=hosts,
        anomaly_percentage=0.15,
        seed=None  # Use different seeds for variety
    )
    
    print(f"Starting log production to Kafka topic '{topic}'")
    print(f"Producing {num_batches} batches of {batch_size} logs each")
    print("-" * 60)
    
    try:
        for batch_num in range(num_batches):
            print(f"\nBatch {batch_num + 1}/{num_batches}")
            batch = generator.generate_batch(batch_size)
            send_log_batch(producer, topic, batch, delay_ms)
    
    except KeyboardInterrupt:
        print("\nProduction stopped by user")
    
    finally:
        producer.flush()
        producer.close()
        print("\nProducer closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce synthetic logs to Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="raw-logs",
                        help="Kafka topic to produce to (default: raw-logs)")
    parser.add_argument("--batches", type=int, default=10,
                        help="Number of batches to produce (default: 10)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Size of each batch (default: 100)")
    parser.add_argument("--delay", type=int, default=100,
                        help="Delay between batches in ms (default: 100)")
    
    args = parser.parse_args()
    
    try:
        produce_logs(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            num_batches=args.batches,
            batch_size=args.batch_size,
            delay_ms=args.delay
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
