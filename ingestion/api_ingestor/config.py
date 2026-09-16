"""
Configuration settings for API Ingestor
Defines endpoints, coordinates, local Bronze Data Lake paths,
and Amazon S3 cloud storage settings.
"""

import os
import json
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "datalake" / "bronze"

# AWS S3 Cloud Storage Settings
S3_BUCKET_NAME = os.getenv("S3_BUCKET", "datanexus-datalake-analytics-dev")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
USE_S3 = os.getenv("USE_S3", "true").lower() == "true"

# API Endpoints
# Using Open-Meteo (100% Free, No API key required, reliable production weather data)
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"

# Major Indian Cities for Weather Ingestion
CITIES_COORDINATES = {
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Pune": {"lat": 18.5204, "lon": 73.8567}
}

# Free Open Currency Exchange Rate API
EXCHANGE_RATE_API_URL = "https://open.er-api.com/v6/latest/USD"

def save_json_payload(data, relative_path):
    """
    Saves JSON payload locally to Bronze Data Lake AND directly uploads to Amazon S3 if enabled.
    """
    # 1. Local Write (Guaranteed fast fallback)
    local_file = BRONZE_DIR / relative_path
    local_file.parent.mkdir(parents=True, exist_ok=True)
    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # 2. Direct AWS S3 Upload (Option 2)
    if USE_S3:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=AWS_REGION)
            s3_key = f"bronze/{relative_path.as_posix()}"
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data, indent=2).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"  ☁️ Stored directly in S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"  ⚠️ S3 Direct Upload note: {e} (saved locally to Bronze lake)")

    return local_file
