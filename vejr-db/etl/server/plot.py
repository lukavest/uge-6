import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
import io

from etl.db.connection import Connection
# from etl.db.repositories.base import get_latest_timestamp
from etl.utils.time_utils import utc_str
from etl.config import settings
from etl.db.constants import DMI_TABLE_NAME,SPAC_TABLE_NAMES

def get_latest_timestamp(conn,table_name: str):
    row = conn.fetch_one(f"SELECT MAX(timestamp) FROM {table_name}")
    return None if row is None else row[0]

labels = ["DMI Jægersborg", "BME280", "DS18B20"]
source_ids = ["06181", "BME280", "DS18B20"]
tables = [DMI_TABLE_NAME, SPAC_TABLE_NAMES[source_ids[1]], SPAC_TABLE_NAMES[source_ids[2]]]

def build_plot() -> bytes:
    # x = [1, 2, 3, 4]
    # y = [10, 15, 8, 20]

    fig, ax = plt.subplots()

    with Connection() as conn:
        max_time = min([get_latest_timestamp(conn, table) for table in tables])
        min_time = max_time - timedelta(days=1)

        t1_str = utc_str(max_time)
        t0_str = utc_str(min_time)

        queries = [
            f"""SELECT timestamp,temperature FROM {table} 
                WHERE source_id = '{sid}'
                AND timestamp BETWEEN '{t0_str}' AND '{t1_str}'
                ORDER BY timestamp DESC"""
            for table, sid in zip(tables, source_ids)]

        for q,label in zip(queries, labels):

            rows = conn.fetch_all(q)

            xs = [row[0] for row in rows]
            ys = [row[1] for row in rows]

            ax.plot(xs, ys, label=label)
    # ax.plot(x, y)
    ax.legend()
    ax.set_title("Example Plot")
    ax.set_xlabel("timestamp")
    ax.set_ylabel("temperature")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()
