"""
DataNexus AWS S3 Storage Utility
Provides direct S3 persistence for Bronze, Silver, and Gold data layers.
Handles direct uploads, partitioned prefix mappings, and verification.
"""

import os
import sys
from pathlib import Path
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

S3_BUCKET_NAME = os.getenv("S3_BUCKET", "datanexus-datalake-analytics-dev")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def get_s3_client():
    """Initializes and returns an authenticated boto3 S3 client."""
    try:
        client = boto3.client("s3", region_name=AWS_REGION)
        # Test connection with a lightweight call
        client.head_bucket(Bucket=S3_BUCKET_NAME)
        return client
    except (NoCredentialsError, ClientError) as e:
        print(f"⚠️ S3 Connection Notice: {e}")
        return None

def upload_file_to_s3(local_path, s3_key, s3_client=None):
    """Uploads a single file to the S3 Data Lake."""
    if s3_client is None:
        s3_client = get_s3_client()
    
    if s3_client is None:
        print(f"⏩ Skipping S3 upload for {s3_key} (AWS credentials not configured).")
        return False

    try:
        s3_client.upload_file(str(local_path), S3_BUCKET_NAME, s3_key)
        print(f"  ☁️ Uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        print(f"  ❌ S3 Upload failed for {s3_key}: {e}")
        return False

def sync_directory_to_s3(local_dir, s3_prefix, s3_client=None):
    """Recursively uploads all files from a local directory to an S3 prefix."""
    local_path = Path(local_dir)
    if not local_path.exists():
        return 0

    if s3_client is None:
        s3_client = get_s3_client()

    if s3_client is None:
        print(f"⏩ S3 sync skipped (No AWS credentials found). Files remain safe in local datalake.")
        return 0

    count = 0
    for file_path in local_path.rglob("*"):
        if file_path.is_file() and not file_path.name.startswith("."):
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{s3_prefix.rstrip('/')}/{relative_path}"
            if upload_file_to_s3(file_path, s3_key, s3_client):
                count += 1
    
    return count

def sync_full_datalake_to_s3():
    """Syncs the entire local Medallion Data Lake to Amazon S3."""
    print(f"\n☁️ Syncing Medallion Data Lake to S3 Bucket: {S3_BUCKET_NAME}...")
    s3_client = get_s3_client()
    if s3_client is None:
        print("💡 To enable S3 uploads, configure your AWS credentials via `aws configure` or set AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY.")
        return

    datalake_dir = PROJECT_ROOT / "datalake"
    for zone in ["bronze", "silver", "gold"]:
        zone_path = datalake_dir / zone
        if zone_path.exists():
            print(f"  📦 Uploading {zone.upper()} zone...")
            uploaded = sync_directory_to_s3(zone_path, zone, s3_client)
            print(f"  ✅ {zone.upper()} zone: {uploaded} files synced to s3://{S3_BUCKET_NAME}/{zone}/")

if __name__ == "__main__":
    sync_full_datalake_to_s3()
