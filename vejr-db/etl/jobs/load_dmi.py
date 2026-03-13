import logging
from datetime import datetime, timedelta, timezone

from etl.clients.dmi_client import DMIClient
from etl.config import settings
from etl.db.connection import Connection
from etl.db.repositories import create_dmi_table, get_latest_timestamp, insert_dmi_rows
from etl.utils.time_utils import floor_to_interval, iter_time_chunks

logger = logging.getLogger(__name__)

MIN_DATE = datetime(2026, 3, 9, tzinfo=timezone.utc)


def run() -> None:
    client = DMIClient()

    with Connection() as conn:
        create_dmi_table(conn)

        latest = get_latest_timestamp(conn,settings.dmi_table_name)
        if latest is None:
            start = MIN_DATE
        else:
            start = latest.replace(microsecond=0, second=0) + timedelta(minutes=settings.dmi_interval_minutes)

        end = floor_to_interval(datetime.now(timezone.utc), settings.dmi_interval_minutes)

        if start >= end:
            logger.info("No new DMI range to load")
            return

        for chunk_start, chunk_end in iter_time_chunks(start, end, chunk_hours=settings.dmi_chunk_hours):
            logger.info("Loading DMI chunk %s -> %s", chunk_start, chunk_end)
            rows = client.fetch_range(chunk_start, chunk_end)
            insert_dmi_rows(conn, rows)
            logger.info("Inserted %s DMI rows", len(rows))