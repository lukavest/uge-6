from __future__ import annotations

SPAC_SOURCE_IDS = ["BME280", "DS18B20"]

SPAC_TABLE_NAMES = {
    SPAC_SOURCE_IDS[0]: "bme280",
    SPAC_SOURCE_IDS[1]: "ds18b20"
}
SPAC_RESAMPLE_NAMES = {k:v+'_res' for k,v in SPAC_TABLE_NAMES.items()}

SPAC_BASE_URL = "https://climate.spac.dk/api/records"

DMI_TABLE_NAME = "dmi"
DMI_SOURCE_IDS = ["06181", "06120", "06072"]
DMI_STATION_NAMES = {"06181": "Jægersborg", "06120": "Odense Lufthavn", "06072": "Ødum"}
