"""
Spark Streaming Job for Real-Time Log Processing

This module implements the core streaming pipeline for:
- Reading logs from file sources or Kafka
- Filtering and detecting errors
- Aggregating statistics by service, host, and time window
- Detecting anomalies and generating alerts
- Writing results to storage
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
    when, split, to_timestamp, concat_ws, lit, round as spark_round,
    collect_list, struct, first, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

from scripts.log_generator import load_logs_from_file
from spark.utils import load_json_config, preprocess_log_record, is_error_event


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


def read_logs_from_file(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read logs from a JSON Lines file and create a DataFrame.
    
    Args:
        spark: SparkSession object
        file_path: Path to the log file
        
    Returns:
        DataFrame with parsed logs
    """
    schema = get_log_schema()
    
    df = spark.read.schema(schema).json(file_path)
    
    # Convert timestamp string to timestamp type
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return df


def preprocess_logs(df: DataFrame) -> DataFrame:
    """
    Preprocess log records.
    
    Applies transformations for:
    - Timestamp normalization
    - Log level normalization
    - Response time validation
    - Status code validation
    
    Args:
        df: Input DataFrame
        
    Returns:
        Preprocessed DataFrame
    """
    df = (df
          .withColumn("log_level", col("log_level").cast(StringType()))
          .withColumn("response_time_ms", 
                      when(col("response_time_ms") < 0, 0)
                      .otherwise(col("response_time_ms")))
          .withColumn("status_code",
                      when(col("status_code") < 0, 0)
                      .otherwise(col("status_code"))))
    
    return df


def filter_error_events(df: DataFrame) -> DataFrame:
    """
    Filter DataFrame to include only error and warning events.
    
    Marks events as errors if:
    - Log level is ERROR, CRITICAL, or FATAL
    - Status code is 4xx or 5xx
    - Message contains error keywords
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with only error/warning events
    """
    error_keywords = ["error", "failure", "failed", "exception", "critical"]
    
    # Create error detection conditions
    error_condition = (
        (col("log_level").isin(["ERROR", "CRITICAL", "FATAL"])) |
        (col("status_code") >= 400) |
        col("message").rlike(f"(?i){'|'.join(error_keywords)}")
    )
    
    df = df.filter(error_condition)
    
    return df


def aggregate_by_service(df: DataFrame, window_duration: str = "60 seconds") -> DataFrame:
    """
    Aggregate metrics by service and time window.
    
    Computes:
    - Event count
    - Error rate
    - Average and max response times
    - Top status codes
    
    Args:
        df: Input DataFrame
        window_duration: Size of the time window (e.g., "60 seconds")
        
    Returns:
        Aggregated DataFrame
    """
    df = (df
          .groupBy(window(col("timestamp"), window_duration), col("service"))
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
          .withColumn("processing_timestamp", lit(datetime.utcnow()))
          )
    
    return df


def aggregate_by_host(df: DataFrame, window_duration: str = "60 seconds") -> DataFrame:
    """
    Aggregate metrics by host and time window.
    
    Args:
        df: Input DataFrame with error events
        window_duration: Size of the time window
        
    Returns:
        Aggregated DataFrame
    """
    df = (df
          .groupBy(window(col("timestamp"), window_duration), col("host"))
          .agg(
              count(lit(1)).alias("event_count"),
              avg(col("response_time_ms")).alias("avg_response_time_ms"),
              spark_max(col("response_time_ms")).alias("max_response_time_ms"),
              count(when(col("log_level").isin(["ERROR", "CRITICAL"]), 1)).alias("critical_events")
          )
          .withColumn("processing_timestamp", lit(datetime.utcnow()))
          )
    
    return df


def aggregate_by_status_code(df: DataFrame, window_duration: str = "60 seconds") -> DataFrame:
    """
    Aggregate metrics by status code and time window.
    
    Args:
        df: Input DataFrame
        window_duration: Size of the time window
        
    Returns:
        Aggregated DataFrame
    """
    df = (df
          .groupBy(window(col("timestamp"), window_duration), col("status_code"))
          .agg(
              count(lit(1)).alias("occurrence_count"),
              collect_list(col("service")).alias("affected_services")
          )
          .withColumn("service_count", 
                      col("affected_services").cast("array<string>").__len__())
          .withColumn("processing_timestamp", lit(datetime.utcnow()))
          )
    
    return df


def detect_anomalies(df: DataFrame, 
                    response_time_threshold: float = 5000,
                    error_rate_threshold: float = 0.1) -> DataFrame:
    """
    Detect anomalies in aggregated data.
    
    Flags records as anomalies if:
    - Average response time exceeds threshold
    - Error rate exceeds threshold
    - Status code is 5xx
    
    Args:
        df: DataFrame with aggregated metrics (from aggregate_by_service)
        response_time_threshold: Max acceptable avg response time (ms)
        error_rate_threshold: Max acceptable error rate (0.0-1.0)
        
    Returns:
        DataFrame with anomaly flags
    """
    df = (df
          .withColumn("is_high_latency",
                      col("avg_response_time_ms") > response_time_threshold)
          .withColumn("is_high_error_rate",
                      col("error_rate") > error_rate_threshold)
          .withColumn("is_anomaly",
                      col("is_high_latency") | col("is_high_error_rate"))
          .withColumn("anomaly_type",
                      when(col("is_high_latency") & col("is_high_error_rate"), "LATENCY_AND_ERRORS")
                      .when(col("is_high_latency"), "HIGH_LATENCY")
                      .when(col("is_high_error_rate"), "HIGH_ERROR_RATE")
                      .otherwise("NORMAL"))
          )
    
    return df


def extract_top_failing_services(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Extract the top N services by error count.
    
    Args:
        df: DataFrame with error events
        top_n: Number of top services to extract
        
    Returns:
        DataFrame with top services
    """
    df = (df
          .groupBy(col("service"))
          .agg(
              count(lit(1)).alias("error_count"),
              avg(col("response_time_ms")).alias("avg_response_time_ms")
          )
          .orderBy(col("error_count").desc())
          .limit(top_n)
          )
    
    return df


def generate_alerts(df: DataFrame, alert_threshold_error_rate: float = 0.15) -> DataFrame:
    """
    Generate alerts for critical conditions.
    
    Args:
        df: DataFrame with anomalies (from detect_anomalies)
        alert_threshold_error_rate: Error rate threshold for alerts
        
    Returns:
        DataFrame with alert records
    """
    alerts = (df
              .filter(col("is_anomaly"))
              .select(
                  col("window.start").alias("window_start"),
                  col("window.end").alias("window_end"),
                  col("service"),
                  col("event_count"),
                  col("error_rate"),
                  col("avg_response_time_ms"),
                  col("anomaly_type").alias("alert_type"),
                  lit("AUTOMATED_ALERT").alias("source"),
                  lit(datetime.utcnow()).alias("alert_generated_time")
              )
              )
    
    return alerts


def run_batch_processing(config_path: str, log_file_path: str, output_dir: str):
    """
    Run batch processing on log files.
    
    Useful for analyzing historical logs or initial data exploration.
    
    Args:
        config_path: Path to Spark configuration
        log_file_path: Path to log file to process
        output_dir: Directory to write outputs
    """
    # Load configuration
    config = load_json_config(config_path)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("RealTimeLogMonitoring-Batch") \
        .master(config['spark'].get('master', 'local[*]')) \
        .config("spark.sql.shuffle.partitions", config['spark'].get('shuffle_partitions', 4)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel(config['spark'].get('log_level', 'WARN'))
    
    print(f"Reading logs from {log_file_path}...")
    
    # Read and preprocess logs
    df = read_logs_from_file(spark, log_file_path)
    df = preprocess_logs(df)
    df.cache()
    
    total_records = df.count()
    print(f"Total records: {total_records}")
    
    # Error analysis
    print("\n=== Error Analysis ===")
    error_df = filter_error_events(df)
    error_count = error_df.count()
    error_rate = error_count / total_records if total_records > 0 else 0
    print(f"Error events: {error_count} ({error_rate:.2%})")
    
    # Top failing services
    print("\n=== Top Failing Services ===")
    top_services = extract_top_failing_services(error_df, top_n=5)
    top_services.show(truncate=False)
    
    # Aggregation by service
    print("\n=== Service Aggregation (60s windows) ===")
    service_agg = aggregate_by_service(error_df, "60 seconds")
    service_agg = detect_anomalies(service_agg)
    anomalies = service_agg.filter(col("is_anomaly"))
    
    if anomalies.count() > 0:
        print(f"Anomalies detected: {anomalies.count()}")
        anomalies.show(truncate=False)
    else:
        print("No anomalies detected in this batch")
    
    # Save results
    print(f"\nWriting results to {output_dir}...")
    # Convert window struct to string columns and export to CSV using Pandas
    service_agg_for_export = service_agg.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("service"),
        col("event_count"),
        col("avg_response_time_ms"),
        col("max_response_time_ms"),
        col("min_response_time_ms"),
        col("server_error_count"),
        col("client_error_count"),
        col("error_rate"),
        col("processing_timestamp"),
        col("is_high_latency"),
        col("is_high_error_rate"),
        col("is_anomaly"),
        col("anomaly_type")
    )
    
    # Convert to Pandas and write CSV to avoid Hadoop issues
    import os
    os.makedirs(f"{output_dir}/service_metrics", exist_ok=True)
    pandas_df = service_agg_for_export.toPandas()
    pandas_df.to_csv(f"{output_dir}/service_metrics/results.csv", index=False)
    print(f"Results saved to {output_dir}/service_metrics/results.csv")
    
    spark.stop()
    print("Batch processing completed!")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python batch_analysis.py <config_path> <log_file_path> <output_dir>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    log_file_path = sys.argv[2]
    output_dir = sys.argv[3]
    
    run_batch_processing(config_path, log_file_path, output_dir)
