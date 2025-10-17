# AWS Streaming POC

This repository contains a Proof of Concept (POC) implementation for testing various streaming solutions with AWS EMR and AWS Glue, including:
- Apache Spark Streaming
- Apache Flink Streaming
- Local Schema Registry
- Kafka Producers and Consumers

## Prerequisites

- Python 3.8+
- Apache Kafka
- AWS Account (for EMR and Glue integration)
- Java 8+ (for running Flink locally)

## Installation

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Download the required Flink connector JARs:
- flink-connector-kafka
- flink-sql-connector-kafka
Place them in an accessible location and update the path in `src/flink/flink_connector.py`

## Components

### 1. Schema Registry
A simple schema registry implementation using FastAPI. Start it with:
```bash
python -m src.schema_registry.registry
```
The registry will be available at http://localhost:8081

### 2. Spark Streaming
The Spark streaming connector (`src/spark/spark_connector.py`) provides:
- Kafka source and sink integration
- AWS S3 integration
- Schema validation
- Configurable processing options

### 3. Flink Streaming
The Flink streaming connector (`src/flink/flink_connector.py`) provides:
- Kafka source and sink integration
- Customizable serialization/deserialization
- Processing function support

### 4. Kafka Utilities
Common Kafka utilities (`src/common/kafka_utils.py`) include:
- Schema-validated producer
- Schema-validated consumer
- Example implementations

## Usage

1. Start the local schema registry:
```bash
python -m src.schema_registry.registry
```

2. Run the example implementation:
```bash
python -m src.example
```

This will demonstrate both Spark and Flink streaming capabilities.

## Configuration

Update the configuration in `src/example.py` to match your environment:
- Kafka bootstrap servers
- Schema registry URL
- AWS credentials (if using AWS services)
- Topics and consumer groups

## AWS Integration

### EMR
To run on EMR:
1. Package the application
2. Upload to S3
3. Create an EMR cluster with Spark/Flink
4. Submit the job using EMR step

### Glue
To run as a Glue job:
1. Package the application
2. Upload to S3
3. Create a Glue job pointing to the application
4. Configure job parameters

## Testing

The repository includes example implementations that demonstrate:
- Schema validation
- Stream processing
- Error handling
- AWS integration

## Project Structure
```
aws-streamer-poc/
├── requirements.txt
├── README.md
└── src/
    ├── common/
    │   ├── config.py
    │   └── kafka_utils.py
    ├── spark/
    │   └── spark_connector.py
    ├── flink/
    │   └── flink_connector.py
    ├── schema_registry/
    │   └── registry.py
    └── example.py
```