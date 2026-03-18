from __future__ import annotations

from etl.db.repositories.base import BaseRepository
from etl.db.schema import (
    SPAC_COLUMNS,
    SPAC_RESAMPLE_COLUMNS,
    build_create_table_sql,
    build_unique_index_sql,
)
from etl.db.constants import SPAC_RESAMPLE_NAMES,SPAC_TABLE_NAMES,SPAC_SOURCE_IDS
from etl.utils.time_utils import floor_to_interval, utc_str
import pandas as pd

class SpacRepository(BaseRepository):
    def create_tables(self) -> None:
        for sid in SPAC_SOURCE_IDS:
            table_name = SPAC_TABLE_NAMES[sid]
            resample_table_name = SPAC_RESAMPLE_NAMES[sid]

            self.conn.execute(build_create_table_sql(table_name, SPAC_COLUMNS[sid]))
            self.conn.execute(build_create_table_sql(resample_table_name, SPAC_RESAMPLE_COLUMNS[sid]))
            self.conn.execute(build_unique_index_sql(resample_table_name, on=["timestamp"]))

    def insert_spac_rows(self, source_id: str, rows: list[tuple]) -> None:
        if not rows:
            return

        table_name = SPAC_TABLE_NAMES[source_id]
        self.insert_rows(table_name, SPAC_COLUMNS[source_id], rows)

    def resample(self, source_id: str) -> None:
        table_name = SPAC_TABLE_NAMES[source_id]
        resample_table_name = SPAC_RESAMPLE_NAMES[source_id]

        latest = self.get_latest_timestamp(table_name)
        if latest is None:
            return

        max_time = floor_to_interval(latest, 10)
        col_names_str = ", ".join(SPAC_RESAMPLE_COLUMNS[source_id])

        query = (
            f"SELECT {col_names_str} FROM {table_name} "
            f"WHERE timestamp < '{utc_str(max_time)}' "
            f"ORDER BY timestamp ASC"
        )
        df = self.query_fetch_df(query).resample(on="timestamp", rule="10min").mean().reset_index()
        df["timestamp"] = df["timestamp"].apply(lambda x: x.isoformat())

        values = [row for row in df.itertuples(name=None, index=False) if not any(pd.isna(e) for e in row)]
        self.insert_rows(resample_table_name, list(df.columns), values)