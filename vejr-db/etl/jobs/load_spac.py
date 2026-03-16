import logging

from etl.clients.spac_client import SpacClient
from etl.config import settings
from etl.db.connection import Connection
from etl.db.repositories import create_spac_tables, get_latest_timestamp, insert_spac_rows, resample_spac_data
from etl.db.schema import SPAC_COLUMNS, SPAC_TABLE_NAMES, SPAC_SOURCE_IDS

logger = logging.getLogger(__name__)


def run() -> None:
    client = SpacClient()
    print("spac run") # TODO make this run , test resampling
    with Connection() as conn:
        create_spac_tables(conn)

        for sid in SPAC_SOURCE_IDS:
            table_name = SPAC_TABLE_NAMES[sid]
            #cols = SPAC_COLUMNS[sid]
            col_names = [t[0] for t in SPAC_COLUMNS[sid]]

            latest = get_latest_timestamp(conn, table_name)
            raw = client.fetch_records(sid,start=latest)
            rows = client.transform_records(raw,sid)
            insert_spac_rows(conn, table_name, col_names, rows)
            logger.info("Inserted %s SPAC %s rows ", len(rows), sid)
            print("resampling")
            resample_spac_data(conn, table_name)

        # latest_bme = get_latest_spac_timestamp(conn, settings.spac_bme_table_name)
        # raw_bme = client.fetch_records("BME280", start=latest_bme)
        # bme_rows = client.transform_bme280_records(raw_bme)
        # insert_spac_rows(conn, settings.spac_bme_table_name, SPAC_BME_COLUMNS, bme_rows)
        # logger.info("Inserted %s SPAC BME280 rows", len(bme_rows))
        #
        # latest_ds = get_latest_spac_timestamp(conn, settings.spac_ds_table_name)
        # raw_ds = client.fetch_records("DS18B20", start=latest_ds)
        # ds_rows = client.transform_ds18b20_records(raw_ds)
        # insert_spac_rows(conn, settings.spac_ds_table_name, SPAC_DS_COLUMNS, ds_rows)
        # logger.info("Inserted %s SPAC DS18B20 rows", len(ds_rows))