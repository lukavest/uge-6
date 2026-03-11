import requests
from connection import Connection

conn = Connection()

table_name = 'dmi_readings'

station_ids     = ['06181',         '06120',            '06072']
station_names   = ['Jaegersborg',   'Odense Lufthavn',  'Oedum']
stations = list(zip(station_names,station_ids))

parameters  = ['temp_dry','temp_dew','humidity','pressure']
base_cols   = ['id',                    'timestamp',    'source_id']
col_types   = ['CHAR(36) PRIMARY KEY',  'TIMESTAMPTZ',  'SMALLINT'] + ['FLOAT']*len(parameters)
col_names   = base_cols + parameters
cols = list(zip(col_names,col_types))


def create_table(tbl_name=table_name,columns=cols,drop_first=False):
    if drop_first:
        conn.execute(f"DROP TABLE IF EXISTS {tbl_name}")
    cols_str = ', '.join([f"{name} {tp}" for name,tp in columns])
    conn.execute(f"CREATE TABLE IF NOT EXISTS {tbl_name} ({cols_str})")

def insert_data(data,param,ignore_conflict=True):
    col_names_str = ','.join(base_cols + [param])
    vals_str = ','.join(['%s']*(len(base_cols)+1))
    q = f"INSERT INTO {table_name} ({col_names_str}) VALUES ({vals_str})"
    if ignore_conflict:
        q += " ON CONFLICT DO NOTHING"
    conn.cur.executemany(q, data)

def get_observations(ids=station_ids,params=parameters,lim=10,ignore_conflict=True):
    assert all(x in station_ids for x in ids), 'ids must be a subset of station_ids'
    assert all(x in parameters for x in params), 'params must be a subset of parameters'

    for sid in ids:
        for param in params:
            url = f"https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId={sid}&parameterId={param}&limit={lim}"
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
            insert_data(data,param,ignore_conflict=ignore_conflict)

def group_table(commit_first=True):
    # WIP
    group_cols = cols[1:]
    print(group_cols)

    create_table("dmi_dense",group_cols)
    # if commit_first:
    #     conn.commit()


    #create_table(columns=cols,drop_first=True)
    #conn.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_idx ON {table_name} (id,timestamp)")

def main():
    #create_table()
    #get_observations()
    #conn.commit()
    group_table(False)
    conn.close()

if __name__ == "__main__":
    main()