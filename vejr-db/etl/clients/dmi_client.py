import logging
from datetime import datetime, timezone

from etl.config import settings
from etl.utils.http import get_json
from etl.utils.time_utils import utc_str

logger = logging.getLogger(__name__)


class DMIClient:
    station_ids = ["06181", "06120", "06072"]
    station_names = ["Jaegersborg", "Odense Lufthavn", "Oedum"]
    stations = list(zip(station_names, station_ids))

    parameters = ["temp_dry", "temp_dew", "humidity", "pressure"]
    base_cols = ["timestamp", "source_id"]
    col_names = base_cols + parameters

    def fetch_parameter_observations(self, station_id: str, parameter: str, time_str: str, limit: int | None = None) -> list[dict]:
        limit = limit or settings.dmi_max_api_limit
        url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
        params = {
            "stationId": station_id,
            "parameterId": parameter,
            "limit": limit,
            "datetime": time_str,
        }
        data = get_json(url, params=params)
        features = data.get("features", [])
        logger.info("DMI %s %s -> %s records", station_id, parameter, len(features))
        return features

    def fetch_range(self, start: datetime, end: datetime, limit: int | None = None) -> list[tuple]:
        time_str = f"{utc_str(start)}/{utc_str(end)}"
        return self.build_dense_rows(time_str, limit=limit)

    def build_dense_rows(self, time_str: str, limit: int | None = None, require_complete: bool = True) -> list[tuple]:
        rows_by_key: dict[tuple[str, str], dict] = {}

        for station_id in self.station_ids:
            for parameter in self.parameters:
                features = self.fetch_parameter_observations(station_id, parameter, time_str, limit=limit)

                for feature in features:
                    props = feature.get("properties", {})
                    observed = props.get("observed")
                    value = props.get("value")

                    if observed is None:
                        continue

                    key = (observed, station_id)
                    row_data = rows_by_key.setdefault(
                        key,
                        {col: None for col in self.col_names} | {
                            "timestamp": observed,
                            "source_id": station_id,
                        },
                    )
                    row_data[parameter] = value

        rows: list[tuple] = []
        for row_data in rows_by_key.values():
            if require_complete and any(row_data[param] is None for param in self.parameters):
                logger.warning("Skipping incomplete DMI row: %s", row_data)
                continue

            rows.append(tuple(row_data[col] for col in self.col_names))

        rows.sort(key=lambda row: (row[0], row[1]))
        return rows