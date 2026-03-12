from connection import Connection
import requests
from datetime import datetime, timedelta

max_limit = 5000
url = "https://climate.spac.dk/api/records"
token = "fwXXhuCtiikxi5npDB_PDlGGhQ2PX4LI2F9kHfLBtmc"
headers = {"Authorization": f"Bearer {token}"}  # ,"Content-Type": "application/json"} # <- for PUT requests

source_ids = ["BME280", "DS18B20"]
table_names = {src_id:src_id.lower()+"_readings" for src_id in source_ids}
shared_cols = [("id", "CHAR(36) PRIMARY KEY"), ("timestamp", "TIMESTAMPTZ"), ("source_id", "VARCHAR(16)"), ("temperature", "FLOAT") ]
extra_cols = [("humidity", "FLOAT"), ("pressure", "FLOAT")]

conn = Connection()

def create_table(src_id,drop_first=False):

    if src_id in source_ids:
        table_name = table_names[src_id]
        if drop_first:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        if src_id == source_ids[0]:
            cols = shared_cols + extra_cols
        elif src_id == source_ids[1]:
            cols = shared_cols
        else:
            print("Unhandled source id: ", src_id)
            return
        cols_str = ','.join(f"{col} {col_type}" for col, col_type in cols)
        q = f"""CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"""

        conn.execute(q)
    else:
        raise ValueError("Invalid source id")

def get_records(src_id,frm:datetime=None,limit=100):
    if src_id not in source_ids:
        raise ValueError("Invalid source id")

    params = {"limit":limit}
    if frm is not None:
        params["from"] = frm.isoformat()

    response = requests.get(url, headers=headers, params=params).json()
    records = response["records"]

    if src_id == source_ids[0]:
        records_lst = [
            (
                r["id"],
                r["timestamp"],
                src_id,
                r["reading"]["BME280"]["temperature"],
                r["reading"]["BME280"]["humidity"],
                r["reading"]["BME280"]["pressure"]
            )
            for r in records
            if "BME280" in r.get("reading", {})
        ]

    elif src_id == source_ids[1]:
        records_lst = [
            (
                r["id"],
                r["timestamp"],
                src_id,
                r["reading"]["DS18B20"]["raw_reading"] / 1000
            )
        for r in records
        if "DS18B20" in r.get("reading", {})
    ]
    else:
        raise ValueError("Unhandled source id: ", src_id)

    return records_lst

def insert_records(src_id,records_lst,ignore_conflict=True,commit=False):
    if src_id not in source_ids:
        raise ValueError("Invalid source id")
    elif len(records_lst) == 0:
        print("No records to insert")
        return
    if src_id == source_ids[0]:
        cols = shared_cols + extra_cols
    elif src_id == source_ids[1]:
        cols = shared_cols
    else:
        raise ValueError("Unhandled source id: ", src_id)
    col_names_str = ','.join([t[0] for t in cols])
    vals_str = ','.join(['%s']*len(cols))
    table_name = table_names[src_id]
    q = f"INSERT INTO {table_name} ({col_names_str}) VALUES ({vals_str})"
    if ignore_conflict:
        q += " ON CONFLICT DO NOTHING"
    conn.cur.executemany(q, records_lst)
    if commit:
        conn.commit()

def update_table(src_id):
    table_name = table_names[src_id]
    print(f"Updating {src_id} records...")
    n_records = max_limit
    while n_records == max_limit:
        try:
            mx = conn.query_fetch(f"SELECT MAX(timestamp) FROM {table_name}")[0][0]
            mx = mx.replace(microsecond=mx.microsecond+1)
        except:
            mx = None
            print(f"Warning: no records found in {table_name} table.")
        records = get_records(src_id,frm=mx, limit=max_limit)
        n_records = len(records)
        print(n_records, "records found beyond max timestamp ", mx)
        if n_records > 0:
            insert_records(src_id,records,commit=True)

def create_all(drop_first=False):
    for source_id in source_ids:
        create_table(source_id,drop_first)

def update_all():
    for source_id in source_ids:
        update_table(source_id)

def main():
    for source_id in source_ids:
        create_table(source_id)
        update_table(source_id)
    conn.commit()
    conn.close()
if __name__ == "__main__":
    main()