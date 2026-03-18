from dataclasses import dataclass
import os
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    project_root_dir = Path(__file__).parent.resolve()
    running_locally: bool = os.getenv("RUNNING_LOCALLY") == "True"

    db_host: str | None = None
    db_port: int | None = None
    db_name: str | None = None
    db_user: str | None = None
    db_password: str | None = None
    spac_api_token: str | None = None

    dmi_interval_minutes: int = 10
    dmi_chunk_hours: int = 24
    dmi_max_api_limit: int = 1000

    spac_max_api_limit: int = 5000

    request_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        if self.running_locally:
            from dotenv import dotenv_values

            config = dotenv_values(self.project_root_dir / "../.env")
            object.__setattr__(self, "db_host", "localhost")
            object.__setattr__(self, "db_port", 5433)
            object.__setattr__(self, "db_name", config["POSTGRES_DB"])
            object.__setattr__(self, "db_user", config["POSTGRES_USER"])
            object.__setattr__(self, "db_password", config["POSTGRES_PASSWORD"])
            object.__setattr__(self, "spac_api_token", config["SPAC_API_TOKEN"])
        else:
            object.__setattr__(self, "db_host", os.getenv("DB_HOST"))
            object.__setattr__(self, "db_port", int(os.getenv("DB_PORT")))
            object.__setattr__(self, "db_name", os.getenv("POSTGRES_DB"))
            object.__setattr__(self, "db_user", os.getenv("POSTGRES_USER"))
            object.__setattr__(self, "db_password", os.getenv("POSTGRES_PASSWORD"))
            object.__setattr__(self, "spac_api_token", os.getenv("SPAC_API_TOKEN"))



settings = Settings()