from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    
    PROJECT_NAME: str = 'SETA API Gateway'
    
    AUTH_SERVICE_URL: str = "http://localhost:8001"
    USER_SERVICE_URL: str = "http://localhost:8002"
    
    PROXY_TIMEOUT: float = 30.0
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra='ignore')
    
    
settings = Settings()

