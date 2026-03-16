from dataclasses import dataclass
import os
from pathlib import Path

# print(PROJECT_ROOT_DIR)
# from dotenv import dotenv_values
# config = dotenv_values(".env")

@dataclass(frozen=True)
class Settings:
    project_root_dir = Path(__file__).parent.resolve()
    running_locally: bool = os.getenv("RUNNING_LOCALLY") == "True"
    if running_locally:
        print("Running locally")
        from dotenv import dotenv_values
        config = dotenv_values(project_root_dir / "../.env")
        # print(config)
        db_host = "localhost"
        db_port = 5433
        db_name = config["POSTGRES_DB"]
        db_user = config["POSTGRES_USER"]
        db_password = config["POSTGRES_PASSWORD"]
        spac_api_token = config["SPAC_API_TOKEN"]
    else:
        db_host: str = os.getenv("DB_HOST")
        db_port: int = int(os.getenv("DB_PORT"))
        db_name: str = os.getenv("POSTGRES_DB")
        db_user: str = os.getenv("POSTGRES_USER")
        db_password: str = os.getenv("POSTGRES_PASSWORD")
        spac_api_token: str = os.getenv("SPAC_API_TOKEN")

    # print(db_name, db_user, db_password, spac_api_token)

    dmi_table_name: str = "dmi"
    spac_bme_table_name: str = "bme280"
    spac_ds_table_name: str = "ds18b20"

    dmi_interval_minutes: int = 10
    dmi_chunk_hours: int = 24
    dmi_max_api_limit: int = 1000

    spac_max_api_limit: int = 5000

    request_timeout_seconds: int = 30


settings = Settings()