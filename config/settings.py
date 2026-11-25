"""Centralized application settings loaded from environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

# Resolve project root directory and load .env if present
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    events_topic: str = os.getenv("KAFKA_EVENTS_TOPIC", "ecommerce-events")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "ecommerce-events-dlq")
    partitions: int = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "ecommerce-debug-group")


@dataclass(frozen=True)
class GeneratorSettings:
    interval_sec: float = float(os.getenv("EVENT_GENERATION_INTERVAL_SEC", "0.5"))
    invalid_event_rate: float = float(os.getenv("INVALID_EVENT_RATE", "0.05"))


@dataclass(frozen=True)
class SparkSettings:
    app_name: str = os.getenv("SPARK_APP_NAME", "EcommerceRealtimePipeline")
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    checkpoint_dir: str = str(
        (BASE_DIR / os.getenv("SPARK_CHECKPOINT_DIR", "checkpoints/ecommerce_stream")).resolve()
    )
    watermark_delay: str = os.getenv("SPARK_WATERMARK_DELAY", "2 minutes")
    window_duration: str = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))


@dataclass(frozen=True)
class PostgresSettings:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "ecommerce_analytics")
    user: str = os.getenv("POSTGRES_USER", "pipeline_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    generator: GeneratorSettings = field(default_factory=GeneratorSettings)
    spark: SparkSettings = field(default_factory=SparkSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
