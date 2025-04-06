from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SSH_HOST: str
    SSH_USER:str
    REMOTE_DB_HOST: str
    REMOTE_DB_PORT: int
    LOCAL_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

config = Settings()