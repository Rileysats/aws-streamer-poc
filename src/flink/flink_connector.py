from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from typing import Callable, Optional
from ..common.config import StreamConfig

class FlinkStreamingConnector:
    def __init__(self, config: StreamConfig):
        self.config = config
        self.env = self._create_execution_environment()

    def _create_execution_environment(self) -> StreamExecutionEnvironment:
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Add required JAR files to the environment
        env.add_jars_to_classpath(
            "file:///path/to/flink-connector-kafka.jar",
            "file:///path/to/flink-sql-connector-kafka.jar"
        )
        
        return env

    def create_kafka_source(self, deserializer: Optional[Callable] = None):
        """
        Create a Kafka source for Flink streaming
        """
        properties = {
            'bootstrap.servers': self.config.kafka.bootstrap_servers,
            'group.id': self.config.kafka.group_id or 'flink-streaming-group'
        }

        if deserializer:
            return FlinkKafkaConsumer(
                topics=self.config.kafka.topic,
                deserialization_schema=deserializer(),
                properties=properties
            )
        else:
            return FlinkKafkaConsumer(
                topics=self.config.kafka.topic,
                deserialization_schema=SimpleStringSchema(),
                properties=properties
            )

    def create_kafka_sink(self, serializer: Optional[Callable] = None):
        """
        Create a Kafka sink for Flink streaming
        """
        properties = {
            'bootstrap.servers': self.config.kafka.bootstrap_servers
        }

        if serializer:
            return FlinkKafkaProducer(
                topic=self.config.kafka.topic,
                serialization_schema=serializer(),
                producer_config=properties
            )
        else:
            return FlinkKafkaProducer(
                topic=self.config.kafka.topic,
                serialization_schema=SimpleStringSchema(),
                producer_config=properties
            )

    def process_stream(self, source_function, processing_function: Callable, sink_function):
        """
        Process a streaming pipeline
        """
        stream = self.env.add_source(source_function)
        processed_stream = processing_function(stream)
        processed_stream.add_sink(sink_function)
        
        return self.env.execute("Flink Streaming Job")
