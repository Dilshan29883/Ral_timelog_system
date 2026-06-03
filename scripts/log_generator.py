"""
Log Generator Module

Generates realistic synthetic log data for testing and demonstration purposes.
Includes support for:
- Realistic log records with typical attributes
- Anomalous events (errors, high latencies, failures)
- Batch and streaming output modes
- Configurable parameters for volume and variety
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Generator, Optional
from faker import Faker
import logging

logger = logging.getLogger(__name__)


class SyntheticLogGenerator:
    """Generate synthetic log records for real-time log monitoring."""
    
    # HTTP status codes
    SUCCESS_CODES = [200, 201, 204]
    CLIENT_ERROR_CODES = [400, 401, 403, 404, 422]
    SERVER_ERROR_CODES = [500, 502, 503, 504]
    
    # Log levels
    LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    
    # Common error messages
    ERROR_MESSAGES = [
        "Database connection timeout",
        "Authentication failed",
        "Authorization denied",
        "Resource not found",
        "Invalid request payload",
        "Internal server error",
        "Service temporarily unavailable",
        "Request timeout",
        "Concurrent modification detected",
        "Memory allocation failed",
        "File descriptor limit exceeded",
        "Network unreachable",
        "Deadlock detected in transaction",
        "Constraint violation in database",
        "External service call failed"
    ]
    
    # Normal messages
    NORMAL_MESSAGES = [
        "Request processed successfully",
        "User logged in",
        "Data retrieved",
        "Record created",
        "Operation completed",
        "Cache hit",
        "Background job started",
        "Configuration reloaded",
        "Health check passed",
        "Connection established",
        "Batch processing completed",
        "Index optimization finished"
    ]
    
    def __init__(self,
                 services: List[str],
                 hosts: List[str],
                 anomaly_percentage: float = 0.15,
                 seed: Optional[int] = None):
        """
        Initialize the log generator.
        
        Args:
            services: List of service names
            hosts: List of host names
            anomaly_percentage: Percentage of records that should be anomalies (0.0-1.0)
            seed: Random seed for reproducibility
        """
        self.services = services
        self.hosts = hosts
        self.anomaly_percentage = anomaly_percentage
        self.faker = Faker()
        
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        
        logger.info(f"Log generator initialized with {len(services)} services and {len(hosts)} hosts")
    
    def generate_normal_log(self, base_time: datetime) -> Dict[str, Any]:
        """Generate a normal (non-anomalous) log record."""
        request_id = self.faker.uuid4()
        response_time = random.randint(50, 500)  # 50-500ms
        
        return {
            'timestamp': base_time.isoformat(),
            'service': random.choice(self.services),
            'host': random.choice(self.hosts),
            'log_level': random.choices(
                ['DEBUG', 'INFO', 'WARNING'],
                weights=[0.3, 0.6, 0.1]
            )[0],
            'message': random.choice(self.NORMAL_MESSAGES),
            'request_id': request_id,
            'response_time_ms': response_time,
            'status_code': random.choices(
                self.SUCCESS_CODES,
                weights=[0.7, 0.2, 0.1]
            )[0]
        }
    
    def generate_error_log(self, base_time: datetime) -> Dict[str, Any]:
        """Generate an error/anomalous log record."""
        request_id = self.faker.uuid4()
        
        # Vary the type of error
        error_type = random.choices(
            ['high_latency', 'client_error', 'server_error', 'exception'],
            weights=[0.3, 0.3, 0.3, 0.1]
        )[0]
        
        if error_type == 'high_latency':
            response_time = random.randint(3000, 15000)  # 3-15 seconds
            status_code = 200
            log_level = 'WARNING'
            message = f"High latency detected: {response_time}ms"
        
        elif error_type == 'client_error':
            response_time = random.randint(50, 300)
            status_code = random.choice(self.CLIENT_ERROR_CODES)
            log_level = 'WARNING'
            message = random.choice(self.ERROR_MESSAGES)
        
        elif error_type == 'server_error':
            response_time = random.randint(100, 2000)
            status_code = random.choice(self.SERVER_ERROR_CODES)
            log_level = 'ERROR'
            message = random.choice(self.ERROR_MESSAGES)
        
        else:  # exception
            response_time = random.randint(100, 1000)
            status_code = 500
            log_level = 'CRITICAL'
            message = f"Exception: {random.choice(self.ERROR_MESSAGES)}"
        
        return {
            'timestamp': base_time.isoformat(),
            'service': random.choice(self.services),
            'host': random.choice(self.hosts),
            'log_level': log_level,
            'message': message,
            'request_id': request_id,
            'response_time_ms': response_time,
            'status_code': status_code
        }
    
    def generate_log_record(self, base_time: datetime) -> Dict[str, Any]:
        """
        Generate a single log record.
        
        Randomly decides between normal and anomalous records based on anomaly_percentage.
        
        Args:
            base_time: Base timestamp for the record
            
        Returns:
            Dictionary representing a log record
        """
        if random.random() < self.anomaly_percentage:
            return self.generate_error_log(base_time)
        else:
            return self.generate_normal_log(base_time)
    
    def generate_batch(self,
                       num_records: int,
                       start_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Generate a batch of log records.
        
        Args:
            num_records: Number of records to generate
            start_time: Starting timestamp (defaults to now)
            
        Returns:
            List of log records
        """
        if start_time is None:
            start_time = datetime.utcnow()
        
        records = []
        for i in range(num_records):
            # Distribute records over a 1-minute window
            time_offset = (i / num_records) * 60
            record_time = start_time + timedelta(seconds=time_offset)
            record = self.generate_log_record(record_time)
            records.append(record)
        
        return records
    
    def stream_logs(self,
                    num_records: int,
                    batch_size: int = 100,
                    delay_ms: int = 100) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream log records in batches, simulating real-time generation.
        
        Args:
            num_records: Total number of records to generate
            batch_size: Size of each batch
            delay_ms: Delay between batches in milliseconds
            
        Yields:
            Batches of log records
        """
        records_generated = 0
        start_time = datetime.utcnow()
        
        while records_generated < num_records:
            batch_records = min(batch_size, num_records - records_generated)
            batch_start = start_time + timedelta(seconds=records_generated * 60 / num_records)
            batch = self.generate_batch(batch_records, batch_start)
            
            records_generated += batch_records
            
            yield batch
            
            # Delay between batches
            if records_generated < num_records:
                time.sleep(delay_ms / 1000.0)
        
        logger.info(f"Stream generation completed: {records_generated} records")


def save_logs_to_file(logs: List[Dict[str, Any]], file_path: str, append: bool = False):
    """
    Save log records to a file in JSONL format (one JSON per line).
    
    Args:
        logs: List of log records
        file_path: Path to output file
        append: If True, append to existing file; if False, overwrite
    """
    mode = 'a' if append else 'w'
    
    try:
        with open(file_path, mode) as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
        logger.info(f"Saved {len(logs)} logs to {file_path}")
    except IOError as e:
        logger.error(f"Error saving logs to {file_path}: {e}")
        raise


def load_logs_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load log records from a JSONL file.
    
    Args:
        file_path: Path to input file
        
    Returns:
        List of log records
    """
    logs = []
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    logs.append(json.loads(line))
        logger.info(f"Loaded {len(logs)} logs from {file_path}")
        return logs
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON in {file_path}: {e}")
        return []


if __name__ == "__main__":
    """Example usage of the log generator."""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize generator
    services = ["auth-service", "api-gateway", "payment-service", "user-service", "notification-service"]
    hosts = ["host-01", "host-02", "host-03", "host-04", "host-05"]
    
    generator = SyntheticLogGenerator(
        services=services,
        hosts=hosts,
        anomaly_percentage=0.15,
        seed=42
    )
    
    # Generate sample logs
    print("Generating 100 sample logs...")
    sample_logs = generator.generate_batch(100)
    
    # Save to file
    save_logs_to_file(sample_logs, "data/sample/sample_logs.txt")
    
    # Print first few records
    print("\nFirst 3 sample records:")
    for log in sample_logs[:3]:
        print(json.dumps(log, indent=2))
