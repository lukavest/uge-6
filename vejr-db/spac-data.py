from connection import Connection
import requests

url = "https://climate.spac.dk/api/records"
token = "fwXXhuCtiikxi5npDB_PDlGGhQ2PX4LI2F9kHfLBtmc"
auth_header = {"Authorization": f"Bearer {token}"}
response = requests.get(url, headers={'Authorization': f'Bearer {token}'}).json()
records = response["records"]
conn = Connection()

def get_ds18b20():
    print("Getting DS18B20 readings...")

    ds_records = [
        (
            r["id"],
            r["reading"]["DS18B20"]["raw_reading"] / 1000,
            r["timestamp"],
            "DS18B20")
        for r in records
        if "DS18B20" in r.get("reading", {})
    ]

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ds18b20_readings (
            id CHAR(36) PRIMARY KEY,
            temperature FLOAT,
            timestamp TIMESTAMP,
            source_id VARCHAR(16)
        )
    """)

    conn.cur.executemany("""
        INSERT INTO ds18b20_readings (id, temperature, timestamp, source_id)
        VALUES (%s, %s, %s, %s)
    """, ds_records)

def get_bme280():
    print("Getting BME280 readings...")

    response = requests.get(url, headers={'Authorization': f'Bearer {token}'}).json()
    records = response["records"]

    # --- Filter and transform BME280 records ---
    bme_records = [
        (
            r["id"],
            r["reading"]["BME280"]["humidity"],
            r["reading"]["BME280"]["pressure"],
            r["reading"]["BME280"]["temperature"],
            r["timestamp"],
            "BME280"
        )
        for r in records
        if "BME280" in r.get("reading", {})
    ]

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bme280_readings (
            id CHAR(36) PRIMARY KEY,
            humidity FLOAT,
            pressure FLOAT,
            temperature FLOAT,
            timestamp TIMESTAMPTZ,
            source_id VARCHAR(16)
        )
    """)
    conn.cur.executemany("""
        INSERT INTO bme280_readings (id, humidity, pressure, temperature, timestamp, source_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, bme_records)


def main():
    get_ds18b20()
    get_bme280()
    conn.close(commit=True)

if __name__ == "__main__":
    main()