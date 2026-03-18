from etl.db.constants import SPAC_SOURCE_IDS

TYPE_DIC = {"id": "CHAR(36) PRIMARY KEY", "timestamp": "TIMESTAMPTZ",
            "source_id": "VARCHAR(16)", "temperature": "FLOAT", "humidity": "FLOAT", "pressure": "FLOAT",
            "temp_dew": "FLOAT"}
SPAC_SHARED_COLUMNS = ["id","timestamp","source_id","temperature"]
SPAC_BME_EXTRA_COLUMNS = ["humidity", "pressure"]

SPAC_DS_COLUMNS = SPAC_SHARED_COLUMNS
SPAC_BME_COLUMNS = SPAC_SHARED_COLUMNS + SPAC_BME_EXTRA_COLUMNS
SPAC_COLUMNS = {
    SPAC_SOURCE_IDS[0]: SPAC_BME_COLUMNS,
    SPAC_SOURCE_IDS[1]: SPAC_DS_COLUMNS,
}

SPAC_RESAMPLE_BASE_COLUMNS = ["timestamp","temperature"]
SPAC_RESAMPLE_COLUMNS = {
    SPAC_SOURCE_IDS[0]: SPAC_RESAMPLE_BASE_COLUMNS + SPAC_BME_EXTRA_COLUMNS,
    SPAC_SOURCE_IDS[1]: SPAC_RESAMPLE_BASE_COLUMNS,
}

DMI_PARAMETERS = ["temp_dry", "temp_dew", "humidity", "pressure"]
DMI_COLUMNS = ["timestamp", "source_id", "temperature"] + DMI_PARAMETERS[1:]

def build_create_table_sql(table_name: str, columns: list[str]) -> str:
    cols_str = ", ".join(f"{name} {TYPE_DIC[name]}" for name in columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"


def build_unique_index_sql(table_name:str,on:list[str]) -> str:
    on_str = ", ".join(on)
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_idx "
        f"ON {table_name} ({on_str})"
    )