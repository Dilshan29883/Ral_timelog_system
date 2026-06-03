"""
Utility functions for log processing and data handling.

This module provides helper functions for:
- Configuration loading
- Spark session management
- Data validation and preprocessing
- Alert generation
"""

import json
import yaml
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_json_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to JSON configuration file
        
    Returns:
        Dictionary containing configuration
    """
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing configuration
    """
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in configuration file: {e}")
        raise


def get_spark_session(app_name: str, config: Dict[str, Any]):
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        config: Configuration dictionary
        
    Returns:
        Configured SparkSession object
    """
    from pyspark.sql import SparkSession
    
    spark_config = config.get('spark', {})
    
    builder = (SparkSession.builder
               .appName(app_name)
               .master(spark_config.get('master', 'local[*]')))
    
    # Add driver and executor configurations
    if 'driver' in spark_config:
        driver = spark_config['driver']
        if 'memory' in driver:
            builder.config('spark.driver.memory', driver['memory'])
    
    if 'executor' in spark_config:
        executor = spark_config['executor']
        if 'memory' in executor:
            builder.config('spark.executor.memory', executor['memory'])
        if 'cores' in executor:
            builder.config('spark.executor.cores', executor['cores'])
    
    # SQL configurations
    if 'sql' in spark_config:
        sql = spark_config['sql']
        if 'shuffle_partitions' in sql:
            builder.config('spark.sql.shuffle.partitions', sql['shuffle_partitions'])
    
    spark = builder.getOrCreate()
    
    # Set log level
    log_level = spark_config.get('log_level', 'WARN')
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def validate_log_record(record: Dict[str, Any]) -> bool:
    """
    Validate that a log record has required fields.
    
    Args:
        record: Log record dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = [
        'timestamp', 'service', 'host', 'log_level',
        'message', 'request_id', 'response_time_ms', 'status_code'
    ]
    
    for field in required_fields:
        if field not in record:
            logger.warning(f"Missing required field: {field}")
            return False
    
    return True


def preprocess_log_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocess a log record for analysis.
    
    Applies:
    - Timestamp parsing and normalization
    - Log level normalization
    - Message tokenization and cleanup
    - Type conversion for numeric fields
    
    Args:
        record: Raw log record
        
    Returns:
        Preprocessed log record
    """
    processed = record.copy()
    
    # Normalize log level
    if 'log_level' in processed:
        processed['log_level'] = processed['log_level'].upper()
    
    # Ensure numeric fields
    if 'response_time_ms' in processed:
        try:
            processed['response_time_ms'] = float(processed['response_time_ms'])
        except (ValueError, TypeError):
            processed['response_time_ms'] = 0.0
    
    if 'status_code' in processed:
        try:
            processed['status_code'] = int(processed['status_code'])
        except (ValueError, TypeError):
            processed['status_code'] = 0
    
    # Clean message
    if 'message' in processed:
        processed['message'] = str(processed['message']).strip()
    
    return processed


def is_error_event(record: Dict[str, Any], error_keywords: List[str] = None) -> bool:
    """
    Determine if a log record represents an error or failure event.
    
    Args:
        record: Log record
        error_keywords: List of keywords to detect errors (default: standard error keywords)
        
    Returns:
        True if record is an error event, False otherwise
    """
    if error_keywords is None:
        error_keywords = ["error", "failure", "failed", "exception", "critical"]
    
    # Check log level
    log_level = record.get('log_level', '').upper()
    if log_level in ['ERROR', 'CRITICAL', 'FATAL']:
        return True
    
    # Check message for error keywords
    message = str(record.get('message', '')).lower()
    if any(keyword.lower() in message for keyword in error_keywords):
        return True
    
    # Check status code (4xx and 5xx are errors)
    status_code = record.get('status_code', 200)
    if status_code >= 400:
        return True
    
    return False


def detect_anomaly(record: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Detect anomalies in a log record.
    
    Args:
        record: Log record
        thresholds: Dictionary of anomaly thresholds
            - error_rate_threshold: Max acceptable error rate
            - response_time_threshold_ms: Max acceptable response time
            - status_codes: List of anomalous status codes
        
    Returns:
        Dictionary with anomaly flags and details
    """
    anomalies = {
        'is_anomaly': False,
        'reasons': [],
        'severity': 'NORMAL'
    }
    
    response_time_threshold = thresholds.get('response_time_threshold_ms', 5000)
    if record.get('response_time_ms', 0) > response_time_threshold:
        anomalies['is_anomaly'] = True
        anomalies['reasons'].append('HIGH_RESPONSE_TIME')
    
    if is_error_event(record):
        anomalies['is_anomaly'] = True
        anomalies['reasons'].append('ERROR_EVENT')
        anomalies['severity'] = 'HIGH'
    
    status_code = record.get('status_code', 200)
    if status_code >= 500:
        anomalies['is_anomaly'] = True
        anomalies['reasons'].append('SERVER_ERROR')
        anomalies['severity'] = 'CRITICAL'
    
    return anomalies


def generate_alert(record: Dict[str, Any], anomaly_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Generate an alert if conditions warrant it.
    
    Args:
        record: Log record
        anomaly_info: Anomaly detection results
        
    Returns:
        Alert dictionary or None if no alert needed
    """
    if not anomaly_info.get('is_anomaly'):
        return None
    
    alert = {
        'timestamp': datetime.utcnow().isoformat(),
        'alert_type': 'LOG_ANOMALY',
        'severity': anomaly_info.get('severity', 'NORMAL'),
        'service': record.get('service'),
        'host': record.get('host'),
        'reasons': anomaly_info.get('reasons', []),
        'relevant_fields': {
            'log_level': record.get('log_level'),
            'status_code': record.get('status_code'),
            'response_time_ms': record.get('response_time_ms'),
            'message': record.get('message')
        }
    }
    
    return alert


def ensure_directory_exists(path: str) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


class MetricsAccumulator:
    """Accumulate and compute metrics from log streams."""
    
    def __init__(self):
        self.total_records = 0
        self.error_count = 0
        self.warning_count = 0
        self.by_service = {}
        self.by_host = {}
        self.by_status_code = {}
        self.response_times = []
    
    def add_record(self, record: Dict[str, Any]):
        """Add a record and accumulate metrics."""
        self.total_records += 1
        
        # Count by log level
        log_level = record.get('log_level', '').upper()
        if log_level == 'ERROR':
            self.error_count += 1
        elif log_level == 'WARNING':
            self.warning_count += 1
        
        # Count by service
        service = record.get('service', 'unknown')
        self.by_service[service] = self.by_service.get(service, 0) + 1
        
        # Count by host
        host = record.get('host', 'unknown')
        self.by_host[host] = self.by_host.get(host, 0) + 1
        
        # Count by status code
        status_code = record.get('status_code', 0)
        self.by_status_code[status_code] = self.by_status_code.get(status_code, 0) + 1
        
        # Collect response times
        response_time = record.get('response_time_ms', 0)
        self.response_times.append(response_time)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get accumulated metrics summary."""
        import statistics
        
        summary = {
            'total_records': self.total_records,
            'error_count': self.error_count,
            'error_rate': self.error_count / self.total_records if self.total_records > 0 else 0,
            'warning_count': self.warning_count,
            'by_service': self.by_service,
            'by_host': self.by_host,
            'by_status_code': self.by_status_code,
        }
        
        if self.response_times:
            summary['response_time_stats'] = {
                'min': min(self.response_times),
                'max': max(self.response_times),
                'mean': statistics.mean(self.response_times),
                'median': statistics.median(self.response_times),
                'stdev': statistics.stdev(self.response_times) if len(self.response_times) > 1 else 0
            }
        
        return summary
