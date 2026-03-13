from dataclasses import dataclass
import os

# from dotenv import dotenv_values
# config = dotenv_values(".env")

@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5433"))
    # db_name: str = os.getenv("POSTGRES_DB", config["POSTGRES_DB"])
    # db_user: str = os.getenv("POSTGRES_USER", config["POSTGRES_USER"])
    # db_password: str = os.getenv("POSTGRES_PASSWORD", config["POSTGRES_PASSWORD"])
    # spac_api_token: str = os.getenv("SPAC_API_TOKEN", config["SPAC_A_DB"])
    db_name: str = os.getenv("POSTGRES_DB")
    db_user: str = os.getenv("POSTGRES_USER")
    db_password: str = os.getenv("POSTGRES_PASSWORD")
    spac_api_token: str = os.getenv("SPAC_API_TOKEN")

    dmi_table_name: str = "dmi"
    spac_bme_table_name: str = "bme280"
    spac_ds_table_name: str = "ds18b20"

    dmi_interval_minutes: int = 10
    dmi_chunk_hours: int = 24
    dmi_max_api_limit: int = 1000

    spac_max_api_limit: int = 5000

    request_timeout_seconds: int = 30


settings = Settings()