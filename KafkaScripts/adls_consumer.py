
from dotenv import load_dotenv
load_dotenv()
from azure.storage.filedatalake import DataLakeServiceClient
from kafka import KafkaConsumer
import os, json
from azure.identity import DefaultAzureCredential


# ── Config ────────────────────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = "flightanalyticsadls"
ADLS_FILESYSTEM   = "flight-data"                    # updated to match ADLS container
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPICS            = ["flight-events", "weather-events"]
DEMO_MODE         = os.getenv("DEMO_MODE", "false").lower() == "true"
DEMO_LIMIT        = 100


# ── Azure AD Credential ───────────────────────────────────────────────────────
credential = DefaultAzureCredential()

# ── ADLS client ───────────────────────────────────────────────────────────────
service_client    = DataLakeServiceClient(
    account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=credential
)
filesystem_client = service_client.get_file_system_client(ADLS_FILESYSTEM)

# ── Kafka consumer ────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="adls-consumer-group"
)

print(f"Listening to topics: {TOPICS}")

from datetime import datetime, timezone


# Track message count per topic for demo mode
topic_counts = {topic: 0 for topic in TOPICS}

for message in consumer:
    topic = message.topic

    # DEMO_MODE: Only process up to DEMO_LIMIT messages per topic
    if DEMO_MODE:
        if topic_counts[topic] >= DEMO_LIMIT:
            # If all topics reached limit, break
            if all(count >= DEMO_LIMIT for count in topic_counts.values()):
                print("[INFO] DEMO_MODE: Reached limit for all topics. Exiting.")
                break
            continue
        topic_counts[topic] += 1

    adls_directory = f"landing/{topic}"

    # Ensure directory exists
    directory_client = filesystem_client.get_directory_client(adls_directory)
    try:
        directory_client.create_directory()
    except Exception:
        pass

    filename     = f"{message.timestamp}_{message.offset}.json"
    file_client  = directory_client.get_file_client(filename)
    data         = (json.dumps(message.value) + "\n").encode("utf-8")

    file_client.create_file()
    file_client.append_data(data=data, offset=0, length=len(data))
    file_client.flush_data(len(data))

    print(f"[WRITTEN] {adls_directory}/{filename}")