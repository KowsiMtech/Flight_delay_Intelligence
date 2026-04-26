import os
from databricks import sql

# Read environment variables
host = os.environ.get("DATABRICKS_HOST")
http_path = os.environ.get("DATABRICKS_HTTP_PATH")
token = os.environ.get("DATABRICKS_TOKEN")

print(f"Host: {host}")
print(f"HTTP Path: {http_path}")
print(f"Token (first 6 chars): {token[:6]}... (hidden)")

try:
    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchall()
            print("Connection successful! Result:", result)
except Exception as e:
    print("Connection failed:", e)
