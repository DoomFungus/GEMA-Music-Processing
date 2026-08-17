from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://user:pass@localhost:5432/db"
    api_base_url: str = "https://api.example.com"
    api_key: str = "changeme"
    api_timeout_seconds: int = 10
    log_level: str = "INFO"
    use_mock_api: bool = Field(default=True, validation_alias="API_MOCK")
    api_batch_size: int = 50


settings = Settings()
