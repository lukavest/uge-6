from etl.config import settings
from .schema import (
    DMI_COLUMNS,
    SPAC_TABLE_NAMES,
    SPAC_SOURCE_IDS,
    SPAC_COLUMNS,
    build_create_table_sql,
    build_dmi_unique_index_sql,
)


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


def create_spac_tables(conn) -> None:
    for sid in SPAC_SOURCE_IDS:
        conn.execute(build_create_table_sql(SPAC_TABLE_NAMES[sid], SPAC_COLUMNS[sid]))


def insert_spac_rows(conn, table_name: str, columns: list[tuple[str, str]], rows: list[tuple]) -> None:
    if not rows:
        return

    col_names = [name for name, _ in columns]
    cols = ", ".join(col_names)
    placeholders = ", ".join(["%s"] * len(col_names))
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    conn.executemany(query, rows)


def get_latest_timestamp(conn, table_name: str):
    row = conn.fetch_one(f"SELECT MAX(timestamp) FROM {table_name}")
    return None if row is None else row[0]
