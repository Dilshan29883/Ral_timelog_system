"""
Kafka Consumer for Log Events

Consumes and displays log events from a Kafka topic for verification and debugging.
"""

import json
import sys
import argparse
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

DEFAULT_API_VERSION = (3, 5, 0)


def create_kafka_consumer(bootstrap_servers: str = "localhost:9092",
                         topic: str = "raw-logs",
                         group_id: str = "log-consumer-group") -> KafkaConsumer:
    """
    Create a Kafka consumer.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic to consume from
        group_id: Consumer group ID
        
    Returns:
        KafkaConsumer instance
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(','),
            api_version=DEFAULT_API_VERSION,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )
        print(f"Connected to Kafka: {bootstrap_servers}")
        print(f"Consuming from topic: {topic}")
        print(f"Consumer group: {group_id}")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        raise


def consume_logs(bootstrap_servers: str,
                topic: str,
                group_id: str,
                num_messages: int = None,
                timeout_ms: int = 5000):
    """
    Consume and display logs from Kafka.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic to consume from
        group_id: Consumer group ID
        num_messages: Number of messages to consume (None = infinite)
        timeout_ms: Timeout for message consumption in milliseconds
    """
    consumer = create_kafka_consumer(bootstrap_servers, topic, group_id)
    
    print(f"\nStarting consumption (timeout: {timeout_ms}ms)...")
    print("-" * 60)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            # Parse and display the log
            log = message.value
            timestamp = datetime.fromisoformat(log.get('timestamp', ''))
            
            print(f"\n[{message.partition}:{message.offset}] {timestamp.strftime('%H:%M:%S.%f')[:-3]}")
            print(f"  Service: {log.get('service', 'N/A')}")
            print(f"  Host: {log.get('host', 'N/A')}")
            print(f"  Level: {log.get('log_level', 'N/A')}")
            print(f"  Message: {log.get('message', 'N/A')}")
            print(f"  Status: {log.get('status_code', 'N/A')}")
            print(f"  Response Time: {log.get('response_time_ms', 'N/A')}ms")
            
            if num_messages and message_count >= num_messages:
                print(f"\nConsumed {message_count} messages. Stopping.")
                break
    
    except KeyboardInterrupt:
        print(f"\n\nConsumption stopped by user after {message_count} messages")
    
    finally:
        consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume synthetic logs from Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="raw-logs",
                        help="Kafka topic to consume from (default: raw-logs)")
    parser.add_argument("--group", default="log-consumer-group",
                        help="Consumer group ID (default: log-consumer-group)")
    parser.add_argument("--messages", type=int,
                        help="Number of messages to consume (default: infinite)")
    parser.add_argument("--timeout", type=int, default=5000,
                        help="Timeout in ms (default: 5000)")
    
    args = parser.parse_args()
    
    try:
        consume_logs(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group,
            num_messages=args.messages,
            timeout_ms=args.timeout
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
