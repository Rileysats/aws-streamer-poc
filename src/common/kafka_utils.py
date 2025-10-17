from confluent_kafka import Producer, Consumer
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers import MessageSerializer
import json
import time
from typing import Dict, Any

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        self.schema_registry_client = SchemaRegistryClient(url=schema_registry_url)
        self.serializer = MessageSerializer(self.schema_registry_client)

    def produce(self, topic: str, value: Dict[str, Any], schema_subject: str):
        """
        Produce a message to Kafka with schema validation
        """
        try:
            # Get the latest schema
            schema = self.schema_registry_client.get_latest_version(schema_subject).schema
            
            # Serialize the message
            serialized_value = self.serializer.encode_record_with_schema(
                topic, schema, value
            )
            
            # Produce to Kafka
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                callback=self._delivery_report
            )
            self.producer.flush()
            
        except Exception as e:
            print(f"Error producing message: {str(e)}")
            raise

    @staticmethod
    def _delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

class KafkaConsumer:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str, group_id: str):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.schema_registry_client = SchemaRegistryClient(url=schema_registry_url)
        self.serializer = MessageSerializer(self.schema_registry_client)

    def consume(self, topics: list, timeout: float = 1.0):
        """
        Consume messages from Kafka with schema validation
        """
        try:
            self.consumer.subscribe(topics)
            
            while True:
                msg = self.consumer.poll(timeout)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Deserialize the message
                decoded_message = self.serializer.decode_message(msg.value())
                print(f"Received message: {decoded_message}")
                yield decoded_message
                
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()

# Example usage
if __name__ == "__main__":
    # Configuration
    BOOTSTRAP_SERVERS = "localhost:9092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    TOPIC = "test-topic"
    GROUP_ID = "test-group"
    
    # Example schema
    SCHEMA = {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "value", "type": "double"}
        ]
    }
    
    # Register schema
    schema_registry = SchemaRegistryClient(url=SCHEMA_REGISTRY_URL)
    schema_registry.register("TestRecord", SCHEMA)
    
    # Producer example
    producer = KafkaProducer(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL)
    producer.produce(
        TOPIC,
        {
            "id": "test1",
            "timestamp": int(time.time()),
            "value": 42.0
        },
        "TestRecord"
    )
    
    # Consumer example
    consumer = KafkaConsumer(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, GROUP_ID)
    for message in consumer.consume([TOPIC]):
        print(f"Processed message: {message}")
