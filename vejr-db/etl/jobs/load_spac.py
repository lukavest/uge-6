import logging

from etl.clients.spac_client import SpacClient
from etl.db.connection import Connection
from etl.db.repositories.spac import SpacRepository
from etl.db.constants import SPAC_TABLE_NAMES, SPAC_SOURCE_IDS

logger = logging.getLogger(__name__)


def run() -> None:
    client = SpacClient()
    with Connection() as conn:
        repo = SpacRepository(conn)
        repo.create_tables()
        for sid in SPAC_SOURCE_IDS:
            table_name = SPAC_TABLE_NAMES[sid]
            latest = repo.get_latest_timestamp(table_name)
            raw = client.fetch_records(sid,start=latest)
            rows = client.transform_records(raw,sid)
            repo.insert_spac_rows(sid, rows)
            logger.info("Inserted %s SPAC %s rows ", len(rows), sid)
            repo.resample(sid)
            logger.info("Resampled SPAC %s", sid)

        conn.commit()

def main():
    run()
    print("All jobs completed successfully.")

if __name__ == "__main__":
    main()