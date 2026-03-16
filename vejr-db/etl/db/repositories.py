from etl.config import settings
import pandas as pd
from etl.utils.time_utils import utc_str, floor_to_interval
from datetime import timedelta

from etl.db.schema import (
    DMI_COLUMNS,
    SPAC_TABLE_NAMES,
    SPAC_SOURCE_IDS,
    SPAC_COLUMNS,
    build_create_table_sql,
    build_dmi_unique_index_sql,
)


def get_latest_timestamp(conn, table_name: str):
    row = conn.fetch_one(f"SELECT MAX(timestamp) FROM {table_name}")
    return None if row is None else row[0]


def create_spac_tables(conn) -> None:
    for sid in SPAC_SOURCE_IDS:
        table_name = SPAC_TABLE_NAMES[sid]
        conn.execute(build_create_table_sql(table_name, SPAC_COLUMNS[sid]))
        conn.execute(build_create_table_sql(table_name+'_res', SPAC_COLUMNS[sid])) # resample tables


def insert_spac_rows(conn, table_name: str, col_names: list[str], rows: list[tuple]) -> None:
    if not rows:
        return

    # col_names = [name for name, _ in columns]
    cols = ", ".join(col_names)
    placeholders = ", ".join(["%s"] * len(col_names))
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    conn.executemany(query, rows)


def query_fetch_df(conn, query_str: str):
    """Run a query and return as a pandas DataFrame."""
    return pd.DataFrame(conn.fetch_all(query_str), columns=[desc.name for desc in conn.cur.description])

def resample_spac_data(conn, table_name: str) -> None:
    """Resample SPAC data."""
    latest = get_latest_timestamp(conn, table_name)
    max_time = floor_to_interval(latest, 10)

    q = f"SELECT * FROM {table_name} WHERE timestamp < {utc_str(max_time)} ORDER BY timestamp ASC"
    df = query_fetch_df(conn, q).resample(on='timestamp',rule='10min').mean().reset_index()
    # df.to_sql(table_name+'_res', conn, if_exists='replace', index=False)

    insert_spac_rows(conn, table_name + '_res', df.columns, df.to_records(index=False))



def create_dmi_table(conn) -> None:
    conn.execute(build_create_table_sql(settings.dmi_table_name, DMI_COLUMNS))
    conn.execute(build_dmi_unique_index_sql())


def insert_dmi_rows(conn, rows: list[tuple]) -> None:
    if not rows:
        return

    col_names = [name for name, _ in DMI_COLUMNS]
    cols = ", ".join(col_names)
    placeholders = ", ".join(["%s"] * len(col_names))
    query = (
        f"INSERT INTO {settings.dmi_table_name} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT (timestamp, source_id) DO NOTHING"
    )
    conn.executemany(query, rows)