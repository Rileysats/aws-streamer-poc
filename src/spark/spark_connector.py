from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from typing import Optional
from ..common.config import StreamConfig

class SparkStreamingConnector:
    def __init__(self, config: StreamConfig):
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName("AWS-Spark-Streaming") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

        # Set AWS credentials if provided
        if self.config.aws.access_key_id and self.config.aws.secret_access_key:
            spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.config.aws.access_key_id)
            spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.config.aws.secret_access_key)
            if self.config.aws.session_token:
                spark._jsc.hadoopConfiguration().set("fs.s3a.session.token", self.config.aws.session_token)

        return spark

    def read_stream(self, schema: Optional[StructType] = None):
        """
        Create a streaming DataFrame from Kafka source
        """
        stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.config.kafka.topic) \
            .option("startingOffsets", "latest") \
            .load()

        if schema:
            return stream.selectExpr("CAST(value AS STRING) as json") \
                .select("from_json(json, schema) as data") \
                .select("data.*")
        return stream

    def write_stream(self, df, output_path: str) -> StreamingQuery:
        """
        Write streaming DataFrame to S3
        """
        return df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", self.config.checkpoint_location) \
            .outputMode("append") \
            .trigger(processingTime=self.config.processing_time) \
            .start()

    def write_stream_console(self, df) -> StreamingQuery:
        """
        Write streaming DataFrame to console (for testing)
        """
        return df.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .outputMode("append") \
            .trigger(processingTime=self.config.processing_time) \
            .start()
