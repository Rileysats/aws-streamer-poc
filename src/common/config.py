from dataclasses import dataclass
from typing import Optional

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    group_id: Optional[str] = None

@dataclass
class AWSConfig:
    region: str
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None

@dataclass
class StreamConfig:
    kafka: KafkaConfig
    aws: AWSConfig
    checkpoint_location: str
    processing_time: str = "5 seconds"
