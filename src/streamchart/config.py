from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVICE_", extra="ignore")

    # Platform / shared variables use explicit aliases (no SERVICE_ prefix).
    database_url: str = Field(
        "postgresql+psycopg://app:app@localhost:5432/app",
        validation_alias="DATABASE_URL",
    )
    api_listen_address: str = Field("0.0.0.0", validation_alias="API_LISTEN_ADDRESS")  # nosec B104
    api_port: int = Field(8000, validation_alias="API_PORT")

    log_level: str = "INFO"
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # Yahoo Finance source.
    yf_base_url: str = "https://query1.finance.yahoo.com/v8/finance/chart"
    yf_user_agent: str = "quant-streamingchart/0.1 (+contact: /u/homelabids)"
    yf_timeout_seconds: float = 10.0

    # Fetch / replay defaults.
    default_ticker: str = "MSFT"
    base_interval: str = "1m"
    source_range: str = "1d"
    target_interval: str = "1m"

    # Replay cadence.
    replay_interval_seconds: float = 1.0
    replay_worker_poll_seconds: float = 600.0

    # Scheduled watchlist ingest.
    scheduler_enabled: bool = False
    scheduler_trigger_time: str = "11:00"
    scheduler_timezone: str = "America/New_York"
    scheduler_check_interval_seconds: float = 60.0
    scheduler_http_timeout_seconds: float = 30.0
    stickynote_base_url: str = "http://stickynote:8080"
    internal_api_base_url: str = "http://localhost:8000"

    # Kafka.
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "paper-ticks"
    kafka_client_id: str = "streamchart-producer"
    kafka_acks: str = "all"


settings = Settings()
