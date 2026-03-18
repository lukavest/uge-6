from __future__ import annotations

from etl.db.constants import DMI_TABLE_NAME
from etl.db.repositories.base import BaseRepository
from etl.db.schema import DMI_COLUMNS, build_create_table_sql, build_unique_index_sql


class DMIRepository(BaseRepository):
    @property
    def table_name(self) -> str:
        return DMI_TABLE_NAME

    def create_table(self) -> None:
        self.conn.execute(build_create_table_sql(self.table_name, DMI_COLUMNS))
        self.conn.execute(build_unique_index_sql(self.table_name,["timestamp", "source_id"]))

    def insert_dmi_rows(self, rows: list[tuple]) -> None:
        self.insert_rows(
            self.table_name,
            DMI_COLUMNS,
            rows,
            conflict_clause="ON CONFLICT (timestamp, source_id) DO NOTHING",
        )
