from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class PostgresSettings(BaseSettings):

    db_user: str = Field(alias="DB_USER")
    db_password: str = Field(alias="DB_PASSWORD")
    db_host: str = Field(alias="DB_HOST")
    db_port: int = Field(alias="DB_PORT")

    db_name: str

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        populate_by_name=True,  # Allows us to still use db_user=xxx in Python code
    )

    @property
    def url(self) -> str:

        url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

        return url


class SapiSettings(PostgresSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="SAPI_")


class CoreSettings(PostgresSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="CORE_")
