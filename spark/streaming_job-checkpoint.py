"""
Spark Structured Streaming Job for Real-Time Log Processing

This module implements the streaming pipeline for continuous log processing with:
- Real-time filtering and error detection
- Windowed aggregations
- Anomaly detection and alerting
- Multiple output modes (console, file, Kafka)
"""

import json
import sys
import os

# Add project root to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, max as spark_max, min as spark_min,
    when, to_timestamp, lit, round as spark_round, concat_ws,
    from_json, split, collect_list
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from spark.utils import load_json_config


def get_log_schema() -> StructType:
    """Define the schema for log records."""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("service", StringType(), True),
        StructField("host", StringType(), True),
        StructField("log_level", StringType(), True),
        StructField("message", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("response_time_ms", DoubleType(), True),
        StructField("status_code", LongType(), True),
    ])


def read_logs_from_file_stream(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read logs from a file using the file source (simulates streaming).
    
    Args:
        spark: SparkSession object
        file_path: Directory path containing log files
        
    Returns:
        Streaming DataFrame
    """
    schema = get_log_schema()
    
    # Read JSON files with schema
    df = (spark.readStream
          .format("json")
          .schema(schema)
          .option("maxFilesPerTrigger", 1)
          .load(file_path))
    
    # Parse timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return df


def read_logs_from_kafka(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """
    Read logs from Kafka stream.
    
    Args:
        spark: SparkSession object
        bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
        topic: Kafka topic to read from
        
    Returns:
        Streaming DataFrame
    """
    schema = get_log_schema()
    
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrap_servers)
          .option("subscribe", topic)
          .option("startingOffsets", "latest")
          .load())
    
    # Parse JSON from Kafka value
    df = df.select(
        from_json(col("value").cast("string"), schema).alias("log")
    ).select("log.*")
    
    # Parse timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return df


def filter_error_events(df: DataFrame) -> DataFrame:
    """Filter DataFrame to include only error and warning events."""
    error_condition = (
        (col("log_level").isin(["ERROR", "CRITICAL", "FATAL"])) |
        (col("status_code") >= 400) |
        col("message").rlike("(?i)(error|failure|failed|exception|critical)")
    )
    
    return df.filter(error_condition)


def aggregate_by_service_windowed(df: DataFrame, 
                                  window_duration: str = "1 minute",
                                  slide_duration: str = "10 seconds") -> DataFrame:
    """
    Aggregate metrics by service with sliding windows.
    
    Args:
        df: Input DataFrame
        window_duration: Size of the window (e.g., "1 minute")
        slide_duration: Slide interval (e.g., "10 seconds")
        
    Returns:
        Windowed aggregation DataFrame
    """
    df = (df
          .groupBy(window(col("timestamp"), window_duration, slide_duration), col("service"))
          .agg(
              count(lit(1)).alias("event_count"),
              avg(col("response_time_ms")).alias("avg_response_time_ms"),
              spark_max(col("response_time_ms")).alias("max_response_time_ms"),
              spark_min(col("response_time_ms")).alias("min_response_time_ms"),
              count(when(col("status_code") >= 500, 1)).alias("server_error_count"),
              count(when(col("status_code") >= 400, 1)).alias("client_error_count")
          )
          .withColumn("error_rate",
                      spark_round((col("server_error_count") + col("client_error_count")) / col("event_count"), 4))
          )
    
    return df


def aggregate_by_log_level_windowed(df: DataFrame,
                                    window_duration: str = "1 minute",
                                    slide_duration: str = "10 seconds") -> DataFrame:
    """
    Aggregate events by log level with sliding windows.
    
    Args:
        df: Input DataFrame
        window_duration: Size of the window
        slide_duration: Slide interval
        
    Returns:
        Windowed aggregation DataFrame
    """
    df = (df
          .groupBy(window(col("timestamp"), window_duration, slide_duration), col("log_level"))
          .agg(
              count(lit(1)).alias("level_count"),
              collect_list(col("service")).alias("services")
          )
          )
    
    return df


def detect_real_time_anomalies(df: DataFrame,
                               response_time_threshold: float = 5000,
                               error_rate_threshold: float = 0.15) -> DataFrame:
    """
    Detect anomalies in windowed aggregations.
    
    Args:
        df: Windowed aggregation DataFrame (from aggregate_by_service_windowed)
        response_time_threshold: Max acceptable avg response time (ms)
        error_rate_threshold: Max acceptable error rate
        
    Returns:
        DataFrame with anomaly flags and alert information
    """
    df = (df
          .withColumn("high_latency", col("avg_response_time_ms") > response_time_threshold)
          .withColumn("high_error_rate", col("error_rate") > error_rate_threshold)
          .withColumn("is_anomaly", col("high_latency") | col("high_error_rate"))
          .withColumn("anomaly_severity",
                      when(col("error_rate") > 0.3, "CRITICAL")
                      .when(col("error_rate") > error_rate_threshold, "HIGH")
                      .when(col("high_latency"), "MEDIUM")
                      .otherwise("LOW"))
          .withColumn("processing_timestamp", lit(datetime.utcnow()))
          )
    
    return df


def format_for_console_output(df: DataFrame) -> DataFrame:
    """Format DataFrame for readable console output."""
    return (df
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("service"),
                col("event_count"),
                spark_round(col("avg_response_time_ms"), 2).alias("avg_response_ms"),
                spark_round(col("error_rate"), 4).alias("error_rate"),
                col("anomaly_severity"),
                col("is_anomaly")
            ))


def run_streaming_pipeline(config_path: str, 
                          source: str = "file",
                          log_dir: Optional[str] = None,
                          output_mode: str = "append",
                          checkpoint_dir: Optional[str] = None):
    """
    Run the real-time log monitoring streaming pipeline.
    
    Args:
        config_path: Path to configuration JSON file
        source: Data source type ("file" or "kafka")
        log_dir: Directory with log files (for file source)
        output_mode: Spark streaming output mode ("append", "update", "complete")
        checkpoint_dir: Directory for checkpoint storage
    """
    # Load configuration
    config = load_json_config(config_path)
    
    # Create Spark session with Kafka support if needed
    builder = SparkSession.builder \
        .appName("RealTimeLogMonitoring-Streaming") \
        .master(config['spark'].get('master', 'local[*]'))
    
    # Add Kafka packages if Kafka source is used
    if source == "kafka":
        builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    
    spark = (builder
             .config("spark.sql.shuffle.partitions", config['spark'].get('shuffle_partitions', 4))
             .config("spark.hadoop.io.native.lib.available", "false")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel(config['spark'].get('log_level', 'WARN'))
    
    print("=" * 60)
    print("Real-Time Log Monitoring System - Streaming Pipeline")
    print("=" * 60)
    print(f"Source: {source}")
    print(f"Output Mode: {output_mode}")
    
    # Read from source
    if source == "file":
        if not log_dir:
            raise ValueError("log_dir must be specified for file source")
        print(f"Reading from: {log_dir}")
        df = read_logs_from_file_stream(spark, log_dir)
    
    elif source == "kafka":
        kafka_config = config.get('kafka', {})
        bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        topic = kafka_config.get('producer', {}).get('topic', 'raw-logs')
        print(f"Reading from Kafka: {bootstrap_servers}, topic: {topic}")
        df = read_logs_from_kafka(spark, bootstrap_servers, topic)
    
    else:
        raise ValueError(f"Unknown source: {source}")
    
    # Filter error events
    error_df = filter_error_events(df)
    
    # Aggregate by service with windowing
    window_config = config['spark']['streaming']
    window_duration = f"{window_config.get('window_duration_seconds', 60)} seconds"
    slide_duration = f"{window_config.get('slide_duration_seconds', 10)} seconds"
    
    aggregated = aggregate_by_service_windowed(error_df, window_duration, slide_duration)
    
    # Detect anomalies
    anomaly_config = config.get('processing', {}).get('anomaly_detection', {})
    response_threshold = anomaly_config.get('response_time_threshold_ms', 5000)
    error_threshold = anomaly_config.get('error_rate_threshold', 0.1)
    
    anomalies = detect_real_time_anomalies(aggregated, response_threshold, error_threshold)
    
    # Format for output
    output_df = format_for_console_output(anomalies)
    
    # Set checkpoint location
    if not checkpoint_dir:
        checkpoint_dir = config['spark']['streaming'].get('checkpoint_location', 'checkpoint/logs')
    
    print(f"Checkpoint directory: {checkpoint_dir}")
    print("\nStarting streaming query...")
    print("-" * 60)
    
    # Write to console for monitoring
    query = (output_df
             .writeStream
             .format("console")
             .option("checkpointLocation", checkpoint_dir)
             .option("maxFilesPerTrigger", 1)
             .outputMode(output_mode)
             .option("truncate", False)
             .trigger(processingTime=f"{window_config.get('trigger_interval_seconds', 5)} seconds")
             .start())
    
    # Keep the query running
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nShutting down streaming pipeline...")
        query.stop()
        print("Pipeline stopped.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run real-time log monitoring streaming pipeline")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--source", choices=["file", "kafka"], default="file", help="Data source type")
    parser.add_argument("--log-dir", help="Directory with log files (for file source)")
    parser.add_argument("--output-mode", choices=["append", "update", "complete"], default="append",
                        help="Spark streaming output mode")
    parser.add_argument("--checkpoint-dir", help="Checkpoint directory")
    
    args = parser.parse_args()
    
    try:
        run_streaming_pipeline(
            config_path=args.config,
            source=args.source,
            log_dir=args.log_dir,
            output_mode=args.output_mode,
            checkpoint_dir=args.checkpoint_dir
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
