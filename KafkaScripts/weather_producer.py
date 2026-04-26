"""
weather_producer.py
OpenMeteo API → Kafka (weather-events topic)
Polls weather every 5 minutes for your 5 focus airports.
Free API — no key required.
"""

import requests, json, time, os
import pandas as pd
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "kafka-vm-ip:9092")
POLL_INTERVAL   = 300     # seconds — every 5 minutes
OPENMETEO_URL   = "https://api.open-meteo.com/v1/forecast"

# ── Focus airports with coordinates (matches your planning doc + Silver layer) ─
AIRPORTS = {
    "YYC": {"lat": 51.1139,  "lon": -114.0200},   # Calgary International
    "LAX": {"lat": 33.9425,  "lon": -118.4081},   # Los Angeles International
    "ORD": {"lat": 41.9742,  "lon":  -87.9073},   # Chicago O'Hare
    "JFK": {"lat": 40.6413,  "lon":  -73.7781},   # New York JFK
    "YYZ": {"lat": 43.6777,  "lon":  -79.6248},   # Toronto Pearson
}

# ── Kafka producer setup ───────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)

# ── Helper: fetch weather for one airport ─────────────────────────────────────
def fetch_weather(airport_code: str, coords: dict) -> dict | None:
    params = {
        "latitude":  coords["lat"],
        "longitude": coords["lon"],
        "current":   "temperature_2m,precipitation,windspeed_10m,visibility,weathercode",
        "timezone":  "auto",
    }
    try:
        resp = requests.get(OPENMETEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data    = resp.json()
        current = data.get("current", {})

        return {
            "airport_code":     airport_code,
            "latitude":         coords["lat"],
            "longitude":        coords["lon"],
            "temperature_c":    current.get("temperature_2m"),
            "precipitation_mm": current.get("precipitation"),
            "wind_speed_kmh":   current.get("windspeed_10m"),
            "visibility_km":    current.get("visibility"),
            "weather_code":     current.get("weathercode"),
            "event_timestamp":  pd.Timestamp.now().isoformat(),
        }
    except requests.RequestException as e:
        print(f"[WARN] Weather fetch failed for {airport_code}: {e}")
        return None

# ── Main polling loop ─────────────────────────────────────────────────────────
print(f"[INFO] Starting weather producer for {list(AIRPORTS.keys())}")
print(f"[INFO] Polling every {POLL_INTERVAL // 60} minutes → topic: weather-events")

poll_count = 0
while True:
    poll_count += 1
    print(f"\n[POLL #{poll_count}] {pd.Timestamp.now().isoformat()}")

    for airport_code, coords in AIRPORTS.items():
        record = fetch_weather(airport_code, coords)

        if record:
            key = f"{airport_code}-{record['event_timestamp'][:10]}"   # airport-date
            producer.send("weather-events", key=key, value=record)
            print(f"  [SENT] {airport_code} — "
                  f"temp={record['temperature_c']}°C  "
                  f"precip={record['precipitation_mm']}mm  "
                  f"wind={record['wind_speed_kmh']}km/h  "
                  f"vis={record['visibility_km']}km")

    producer.flush()
    print(f"[SLEEP] Waiting {POLL_INTERVAL}s until next poll...")
    time.sleep(POLL_INTERVAL)
