import requests
from connection import Connection
from datetime import datetime, timezone, timedelta
conn = Connection()

table_name = 'dmi_readings'
group_table_name = 'dmi_dense'

station_ids     = ['06181',         '06120',            '06072']
station_names   = ['Jaegersborg',   'Odense Lufthavn',  'Oedum']
stations = list(zip(station_names,station_ids))

parameters  = ['temp_dry','temp_dew','humidity','pressure']
base_cols   = ['id',                    'timestamp',    'source_id']
col_types   = ['CHAR(36) PRIMARY KEY',  'TIMESTAMPTZ',  'SMALLINT'] + ['FLOAT']*len(parameters)
col_names   = base_cols + parameters
cols = list(zip(col_names,col_types))

min_date = datetime(2026,3,9,tzinfo=timezone.utc)

#def create_table(tbl_name=table_name,columns=cols,drop_first=False):
def create_table(tbl_name,columns,drop_first=False):
    if drop_first:
        conn.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    cols_str = ', '.join([f"{name} {tp}" for name,tp in columns])
    conn.execute(f"CREATE TABLE IF NOT EXISTS {tbl_name} ({cols_str})")

def create_main_table():
    create_table(table_name,cols)

def create_group_table():
    create_table(group_table_name,cols[1:])
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {group_table_name}_idx ON {group_table_name} (timestamp, source_id)")

def insert_param(data, param, ignore_conflict=True):
    col_names_str = ','.join(base_cols + [param])
    vals_str = ','.join(['%s']*(len(base_cols)+1))
    q = f"INSERT INTO {table_name} ({col_names_str}) VALUES ({vals_str})"
    if ignore_conflict:
        q += " ON CONFLICT DO NOTHING"
    conn.cur.executemany(q, data)


def get_observations(time_str:str,lim=10):
    print(f"Getting records for {time_str}")
    for sid in station_ids:
        print(f" station id = {sid}")
        for param in parameters:
            print(f"   parameter = {param}\t",end="")
            url = f"https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId={sid}&parameterId={param}&limit={lim}&datetime={time_str}"
            response = requests.get(url).json()
            data = [
                (
                    r['id'],
                    r['properties']['observed'],
                    sid,
                    r['properties']['value']
                )
                for r in response['features']
            ]
            print(f"{len(data)} records")
            if len(data) == lim: print("Warning: max limit reached")
            insert_param(data, param)

def utc_str(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def get_all_data():
    t1 = datetime.now(timezone.utc)
    t1 = t1.replace(microsecond=0, second=0, minute=t1.minute//10 * 10)
    time_str = f"{utc_str(min_date)}/{utc_str(t1)}"
    get_observations(time_str, lim=1000)

def densify():
    param_cols  = ','.join(parameters)
    agg_cols    = ','.join([f"MAX({p}) AS {p}" for p in parameters])
    update_cols = ','.join([f"{p} = EXCLUDED.{p}" for p in parameters])

    conn.execute(f"""
        INSERT INTO dmi_dense (timestamp,source_id,{param_cols})
        SELECT timestamp,source_id,{agg_cols}
        FROM dmi_readings
        GROUP BY timestamp, source_id
        ON CONFLICT (timestamp, source_id) DO UPDATE SET {update_cols}
    """)


def main():
    create_main_table()
    create_group_table()
    get_all_data()
    densify()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()