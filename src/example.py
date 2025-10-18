from src.spark.spark_connector import SparkStreamingConnector
from src.flink.flink_connector import FlinkStreamingConnector
from src.common.config import StreamConfig, KafkaConfig, AWSConfig
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from typing import Dict
import time
import json

def create_example_config() -> StreamConfig:
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        topic="test-topic",
        group_id="test-group"
    )
    
    aws_config = AWSConfig(
        region="us-west-2",
        # Add your AWS credentials here if needed
    )
    
    return StreamConfig(
        kafka=kafka_config,
        aws=aws_config,
        checkpoint_location="data/spark/checkpoint"
    )

def run_spark_example():
    """
    Example of using Spark Streaming
    """
    config = create_example_config()
    connector = SparkStreamingConnector(config)
    
    # Define schema matching the Kafka messages
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read from Kafka
    print("Reading stream from Kafka...")
    stream_df = connector.read_stream(schema)
    
    # Process the stream (example: filter values > 30)
    print("Processing stream...")
    processed_df = stream_df.filter(stream_df.value > 30)
    
    # Write to console for testing
    print("Writing stream to console...")
    # query = connector.write_stream_console(processed_df)
    query = connector.write_stream(processed_df, "data/spark/csv")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Spark streaming...")
        query.stop()

def run_flink_example():
    """
    Example of using Flink Streaming
    """
    config = create_example_config()
    connector = FlinkStreamingConnector(config)
    
    # Create source and sink
    source = connector.create_kafka_source()
    sink = connector.create_kafka_sink()
    
    # Define processing function
    def process_stream(stream):
        return stream \
            .map(lambda x: json.loads(x)) \
            .filter(lambda x: x['value'] > 30) \
            .map(lambda x: json.dumps(x))
    
    # Process the stream
    try:
        connector.process_stream(source, process_stream, sink)
    except KeyboardInterrupt:
        print("Stopping Flink streaming...")

if __name__ == "__main__":
    # Run Spark example
    print("Running Spark Streaming example...")
    run_spark_example()
    
    # Run Flink example
    print("\nRunning Flink Streaming example...")
    run_flink_example()
