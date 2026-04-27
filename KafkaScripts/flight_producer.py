"""
flight_producer.py
BTS CSV to Kafka (flight-events topic)
Streams all BTS flight records. Carrier/airport filtering happens in Silver DLT.
Place your BTS CSV files in /data/bts/ before running.
"""

import pandas as pd
from kafka import KafkaProducer
import json, time, glob, os

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-vm-ip:9092")
BTS_DATA_DIR = os.getenv("BTS_DATA_DIR", "/data/bts/*.csv")
SLEEP_SECONDS = 0.05          # 20 records/second — simulates real-time stream

# BTS column → Kafka payload key mapping
BTS_COLUMNS = {
    "FlightDate":          "flight_date",
    "Reporting_Airline":   "carrier",
    "Tail_Number":         "tail_number",
    "Origin":              "origin",
    "Dest":                "dest",
    "DepDelay":            "dep_delay",
    "ArrDelay":            "arr_delay",
    "Cancelled":           "cancelled",
    "CancellationCode":    "cancellation_code",
    "CarrierDelay":        "carrier_delay",
    "WeatherDelay":        "weather_delay",
    "NASDelay":            "nas_delay",
    "LateAircraftDelay":   "late_aircraft_delay",
    "CRSDepTime":          "scheduled_dep_time",
    "CRSArrTime":          "scheduled_arr_time",
}

# ── Kafka producer setup ───────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",               # Wait for all replicas — ensures no data loss
    retries=3,
)

# ── Helper: build record from a BTS row ───────────────────────────────────────
def build_record(row: pd.Series) -> dict:
    record = {}
    for bts_col, kafka_key in BTS_COLUMNS.items():
        val = row.get(bts_col)
        # Numeric fields — keep as float/int or None
        if kafka_key in ("dep_delay", "arr_delay", "carrier_delay",
                         "weather_delay", "nas_delay", "late_aircraft_delay"):
            record[kafka_key] = float(val) if pd.notna(val) else None
        elif kafka_key == "cancelled":
            record[kafka_key] = int(val) if pd.notna(val) else 0
        else:
            record[kafka_key] = str(val).strip() if pd.notna(val) else ""

    record["event_timestamp"] = pd.Timestamp.now().isoformat()
    return record

# ── Main: stream each CSV file ────────────────────────────────────────────────
csv_files = sorted(glob.glob(BTS_DATA_DIR))
if not csv_files:
    print(f"[ERROR] No CSV files found at: {BTS_DATA_DIR}")
    exit(1)

print(f"[INFO] Found {len(csv_files)} CSV file(s). Streaming all BTS records to Kafka...")

total_sent = 0

for csv_file in csv_files:
    print(f"\n[FILE] Processing: {csv_file}")

    df = pd.read_csv(csv_file, low_memory=False, usecols=list(BTS_COLUMNS.keys()))
    print(f"[INFO] {len(df)} rows loaded")

    for _, row in df.iterrows():
        record = build_record(row)

        # Kafka message key — used for partitioning (tail number tracks cascade)
        key = f"{record['carrier']}-{record['tail_number']}-{record['flight_date']}"

        producer.send("flight-events", key=key, value=record)
        total_sent += 1
        time.sleep(SLEEP_SECONDS)

    producer.flush()
    print(f"[DONE] File complete. Running total sent: {total_sent}")

print(f"\n[COMPLETE] Sent {total_sent} records total")
producer.close()
