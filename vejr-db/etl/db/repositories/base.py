from __future__ import annotations

import pandas as pd

from etl.db.connection import Connection


class BaseRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def get_latest_timestamp(self, table_name:str):
        row = self.conn.fetch_one(f"SELECT MAX(timestamp) FROM {table_name}")
        return None if row is None else row[0]

    def insert_rows(self,table_name: str,col_names: list[str],rows: list[tuple],
                    conflict_clause: str = "ON CONFLICT DO NOTHING",) -> None:
        if not rows:
            return

        cols = ", ".join(col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) {conflict_clause}"
        self.conn.executemany(query, rows)


    def query_fetch_df(self, query_str: str) -> pd.DataFrame:
        """Run a query and return as a pandas DataFrame."""
        rows = self.conn.fetch_all(query_str)
        return pd.DataFrame(rows, columns=[desc.name for desc in self.conn.cur.description])