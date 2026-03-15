from pydantic_settings import BaseSettings, SettingsConfigDict
import secrets

class Settings(BaseSettings):
    PROJECT_NAME: str = "Auth Service"
    API_V1_STR: str = "/api/v1"
    
    # 32 byte secret key for JWT
    SECRET_KEY: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days
    
    # PostgreSQL connection (replace with valid defaults or read from .env)
    DATABASE_URL: str = "postgresql://postgres:1@localhost:5432/postgres"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
