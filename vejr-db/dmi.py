import requests
from connection import Connection
from datetime import datetime, timezone

table_name = "dmi_readings"
group_table_name = "dmi_dense"

station_ids = ["06181", "06120", "06072"]
station_names = ["Jaegersborg", "Odense Lufthavn", "Oedum"]
stations = list(zip(station_names, station_ids))

parameters = ["temp_dry", "temp_dew", "humidity", "pressure"]
base_cols = ["id", "timestamp", "source_id"]
col_types = ["CHAR(36) PRIMARY KEY", "TIMESTAMPTZ", "SMALLINT"] + ["FLOAT"] * len(parameters)
col_names = base_cols + parameters
cols = list(zip(col_names, col_types))

min_date = datetime(2026, 3, 9, tzinfo=timezone.utc)
interval_mins = 10

def utc_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class DMIClient:
    def __init__(self, conn: Connection):
        self.conn = conn

    def create_table(self, tbl_name, columns, drop_first=False):
        if drop_first:
            self.conn.execute(f"DROP TABLE IF EXISTS {tbl_name}")
        cols_str = ", ".join([f"{name} {tp}" for name, tp in columns])
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {tbl_name} ({cols_str})")

    def create_main_table(self):
        self.create_table(table_name, cols)

    def create_group_table(self):
        self.create_table(group_table_name, cols[1:])
        self.conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {group_table_name}_idx "
            f"ON {group_table_name} (timestamp, source_id)"
        )

    def insert_param(self, data, param, ignore_conflict=True):
        col_names_str = ",".join(base_cols + [param])
        vals_str = ",".join(["%s"] * (len(base_cols) + 1))
        q = f"INSERT INTO {table_name} ({col_names_str}) VALUES ({vals_str})"
        if ignore_conflict:
            q += " ON CONFLICT DO NOTHING"
        self.conn.cur.executemany(q, data)

    def get_observations(self, time_str: str, lim=10):
        print(f"Getting records for {time_str}")
        for sid in station_ids:
            print(f" station id = {sid}")
            for param in parameters:
                print(f"   {param}\t", end="")
                url = (
                    "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
                    f"?stationId={sid}&parameterId={param}&limit={lim}&datetime={time_str}"
                )
                response = requests.get(url).json()
                data = [
                    (
                        r["id"],
                        r["properties"]["observed"],
                        sid,
                        r["properties"]["value"]
                    )
                    for r in response["features"]
                ]
                print(f"{len(data)} records")
                if len(data) == lim:
                    print("Warning: max limit reached")
                self.insert_param(data, param)

    def get_all_data(self):
        t1 = datetime.now(timezone.utc)
        t1 = t1.replace(microsecond=0, second=0, minute=t1.minute // interval_mins * interval_mins)
        time_str = f"{utc_str(min_date)}/{utc_str(t1)}"
        self.get_observations(time_str, lim=1000)

    def get_new_data(self):
        mx = self.conn.query_fetch(f"SELECT MAX(timestamp) FROM dmi_readings")[0][0]

        t = mx.replace(microsecond=0, second=0, minute=mx.minute + interval_mins)
        time_str = f"{utc_str(t)}/.."
        self.get_observations(time_str, lim=1000)

    def densify(self):
        param_cols = ",".join(parameters)
        agg_cols = ",".join([f"MAX({p}) AS {p}" for p in parameters])
        update_cols = ",".join([f"{p} = EXCLUDED.{p}" for p in parameters])

        self.conn.execute(f"""
            INSERT INTO dmi_dense (timestamp,source_id,{param_cols})
            SELECT timestamp,source_id,{agg_cols}
            FROM dmi_readings
            GROUP BY timestamp, source_id
            ON CONFLICT (timestamp, source_id) DO UPDATE SET {update_cols}
        """)

        # remove rows with null values
        null_str = " OR ".join([f"{p} IS NULL" for p in parameters])
        self.conn.execute(f"DELETE FROM dmi_dense WHERE {null_str}")

def main():
    conn = Connection()
    dmi = DMIClient(conn)
    dmi.create_main_table()
    dmi.create_group_table()
    # dmi.get_all_data()
    dmi.get_new_data()
    dmi.densify()
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
