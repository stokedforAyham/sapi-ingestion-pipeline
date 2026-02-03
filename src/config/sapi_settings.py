from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SapiHttpSettings(BaseSettings):
    """
    Loads RapidAPI credentials for SAPI from .env.
    """

    rapidapi_key: str = Field(alias="RAPIDAPI_KEY")
    rapidapi_host: str = Field(alias="RAPIDAPI_HOST")
    sapi_base_url: str = Field(alias="SAPI_BASE_URL")

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )
