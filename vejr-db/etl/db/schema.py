from etl.config import settings


DMI_COLUMNS = [
    ("timestamp", "TIMESTAMPTZ"),
    ("source_id", "CHAR(5)"),
    ("temperature", "FLOAT"),
    ("temp_dew", "FLOAT"),
    ("humidity", "FLOAT"),
    ("pressure", "FLOAT"),
]

SPAC_SHARED_COLUMNS = [
    ("id", "CHAR(36) PRIMARY KEY"),
    ("timestamp", "TIMESTAMPTZ"),
    ("source_id", "VARCHAR(16)"),
    ("temperature", "FLOAT"),
]

SPAC_BME_EXTRA_COLUMNS = [
    ("humidity", "FLOAT"),
    ("pressure", "FLOAT"),
]

SPAC_DS_COLUMNS = SPAC_SHARED_COLUMNS
SPAC_BME_COLUMNS = SPAC_SHARED_COLUMNS + SPAC_BME_EXTRA_COLUMNS
SPAC_COLUMNS = {"BME280":SPAC_BME_COLUMNS,"DS18B20":SPAC_DS_COLUMNS}

SPAC_SOURCE_IDS = ["BME280", "DS18B20"]
SPAC_TABLE_NAMES = {"BME280": settings.spac_bme_table_name,"DS18B20": settings.spac_ds_table_name}



def build_create_table_sql(table_name: str, columns: list[tuple[str, str]]) -> str:
    cols_str = ", ".join(f"{name} {col_type}" for name, col_type in columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"


def build_dmi_unique_index_sql() -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {settings.dmi_table_name}_idx "
        f"ON {settings.dmi_table_name} (timestamp, source_id)"
    )