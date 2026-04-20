# data_lake_pipeline.py

import boto3
import pandas as pd
import requests
import logging
import json
from datetime import datetime
from io import StringIO
from botocore.exceptions import ClientError

# ── 1. Logging ─────────────────────────────────────────────────
logging.basicConfig(
    filename="data_lake.log",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s — %(message)s", "%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(console)

# ── 2. MinIO Config ────────────────────────────────────────────
MINIO_CONFIG = {
    "endpoint_url":          "http://localhost:9000",
    "aws_access_key_id":     "minioadmin",
    "aws_secret_access_key": "minioadmin",
    "region_name":           "us-east-1",
}

BUCKET_NAME = "weather-data-lake"

# ── 3. Cities ──────────────────────────────────────────────────
CITIES = {
    "Bengaluru": {"latitude": 12.97, "longitude": 77.59},
    "Mumbai":    {"latitude": 19.07, "longitude": 72.87},
    "Delhi":     {"latitude": 28.61, "longitude": 77.20},
    "Chennai":   {"latitude": 13.08, "longitude": 80.27},
    "Hyderabad": {"latitude": 17.38, "longitude": 78.48},
}

# ── 4. Connect to MinIO ────────────────────────────────────────
def get_s3_client():
    client = boto3.client("s3", **MINIO_CONFIG)
    logging.info("✅ Connected to MinIO")
    return client

# ── 5. Create Bucket ───────────────────────────────────────────
def create_bucket(client):
    try:
        client.create_bucket(Bucket=BUCKET_NAME)
        logging.info(f"✅ Bucket created: {BUCKET_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logging.info(f"✅ Bucket already exists: {BUCKET_NAME}")
        else:
            raise

# ── 6. Fetch Weather Data ──────────────────────────────────────
def fetch_weather() -> list:
    logging.info("🌤️ Fetching weather data...")
    results = []

    for city, coords in CITIES.items():
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude":        coords["latitude"],
                "longitude":       coords["longitude"],
                "current_weather": True,
            }
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                w = response.json()["current_weather"]
                results.append({
                    "city":        city,
                    "temperature": round(w["temperature"], 1),
                    "windspeed":   round(w["windspeed"], 1),
                    "weathercode": w["weathercode"],
                    "fetched_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                logging.info(f"✅ Fetched: {city} → {w['temperature']}°C")
        except Exception as e:
            logging.error(f"❌ Error fetching {city}: {e}")

    return results

# ── 7. Upload to Data Lake ─────────────────────────────────────
def upload_to_data_lake(client, data: list):
    now = datetime.now()

    # ── Date partitioned path — like real S3 data lakes!
    partition_path = (
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"hour={now.strftime('%H')}/"
    )

    # ── Upload 1: Raw JSON ─────────────────────────────────────
    json_key = f"raw/{partition_path}weather_raw.json"
    json_data = json.dumps(data, indent=2)

    client.put_object(
        Bucket=BUCKET_NAME,
        Key=json_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
    )
    logging.info(f"✅ Uploaded raw JSON → {json_key}")

    # ── Upload 2: Processed CSV ────────────────────────────────
    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_key = f"processed/{partition_path}weather_processed.csv"
    client.put_object(
        Bucket=BUCKET_NAME,
        Key=csv_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logging.info(f"✅ Uploaded processed CSV → {csv_key}")

    # ── Upload 3: Summary JSON ─────────────────────────────────
    summary = {
        "pipeline_run_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "total_cities":    len(data),
        "avg_temperature": round(
            sum(r["temperature"] for r in data) / len(data), 1
        ),
        "hottest_city": max(data, key=lambda x: x["temperature"])["city"],
        "coldest_city": min(data, key=lambda x: x["temperature"])["city"],
    }

    summary_key = f"summary/{partition_path}weather_summary.json"
    client.put_object(
        Bucket=BUCKET_NAME,
        Key=summary_key,
        Body=json.dumps(summary, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logging.info(f"✅ Uploaded summary → {summary_key}")

    return partition_path, summary

# ── 8. List Files in Data Lake ─────────────────────────────────
def list_data_lake(client):
    logging.info("📂 Listing data lake contents...")

    response = client.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" in response:
        logging.info(f"📁 Files in {BUCKET_NAME}:")
        for obj in response["Contents"]:
            size_kb = round(obj["Size"] / 1024, 2)
            logging.info(f"   📄 {obj['Key']} ({size_kb} KB)")
    else:
        logging.info("📁 Bucket is empty")

# ── 9. Read Back from Data Lake ────────────────────────────────
def read_from_data_lake(client, partition_path: str):
    logging.info("📖 Reading back from data lake...")

    csv_key = f"processed/{partition_path}weather_processed.csv"

    response = client.get_object(Bucket=BUCKET_NAME, Key=csv_key)
    csv_content = response["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(csv_content))
    logging.info(f"✅ Read back {len(df)} rows from data lake")
    logging.info(f"\n{df.to_string(index=False)}")

    return df

# ── 10. Main Pipeline ──────────────────────────────────────────
def main():
    logging.info("═══════════ Data Lake Pipeline Started ═══════════")

    # Connect
    client = get_s3_client()

    # Create bucket
    create_bucket(client)

    # Fetch weather
    data = fetch_weather()

    # Upload to data lake
    partition_path, summary = upload_to_data_lake(client, data)

    # Print summary
    logging.info("📊 Pipeline Summary:")
    logging.info(f"   Cities processed: {summary['total_cities']}")
    logging.info(f"   Avg temperature:  {summary['avg_temperature']}°C")
    logging.info(f"   Hottest city:     {summary['hottest_city']}")
    logging.info(f"   Coldest city:     {summary['coldest_city']}")

    # List all files
    list_data_lake(client)

    # Read back and verify
    read_from_data_lake(client, partition_path)

    logging.info("═══════════ Pipeline Complete! ═══════════════════")
    logging.info(f"🌐 View your data lake at: http://localhost:9001")

if __name__ == "__main__":
    main()