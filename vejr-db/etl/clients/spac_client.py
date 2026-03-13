import logging
from datetime import datetime

from etl.config import settings
from etl.utils.http import get_json

logger = logging.getLogger(__name__)


class SpacClient:
    base_url = "https://climate.spac.dk/api/records"
    source_ids = ["BME280", "DS18B20"]
    table_names = {src_id: src_id.lower() + "_readings" for src_id in source_ids}

    def _headers(self) -> dict:
        if not settings.spac_api_token:
            raise ValueError("SPAC_API_TOKEN is not set")
        return {"Authorization": f"Bearer {settings.spac_api_token}"}

    def fetch_records(self, source_id: str, start: datetime | None = None, limit: int | None = None) -> list[dict]:
        if source_id not in self.source_ids:
            raise ValueError(f"Unsupported source_id: {source_id}")

        params = {"limit": limit or settings.spac_max_api_limit}
        if start is not None:
            params["from"] = start.isoformat()

        data = get_json(self.base_url, headers=self._headers(), params=params)
        return data.get("records", [])

    @staticmethod
    def transform_bme280_records(raw_records: list[dict]) -> list[tuple]:
        rows = []
        for record in raw_records:
            reading = record.get("reading", {})
            if "BME280" not in reading:
                continue

            bme = reading["BME280"]
            rows.append(
                (
                    record["id"],
                    record["timestamp"],
                    "BME280",
                    bme.get("temperature"),
                    bme.get("humidity"),
                    bme.get("pressure", 0) / 100 if bme.get("pressure") is not None else None,
                )
            )
        return rows

    @staticmethod
    def transform_ds18b20_records(raw_records: list[dict]) -> list[tuple]:
        rows = []
        for record in raw_records:
            reading = record.get("reading", {})
            if "DS18B20" not in reading:
                continue

            ds = reading["DS18B20"]
            raw_value = ds.get("raw_reading")
            rows.append(
                (
                    record["id"],
                    record["timestamp"],
                    "DS18B20",
                    raw_value / 1000 if raw_value is not None else None,
                )
            )
        return rows

    def transform_records(self,raw_records: list[dict], source_id:str) -> list[tuple]:
        if source_id == "BME280":
            return self.transform_bme280_records(raw_records)
        elif source_id == "DS18B20":
            return self.transform_ds18b20_records(raw_records)
        else:
            raise ValueError(f"Unsupported source_id: {source_id}")

