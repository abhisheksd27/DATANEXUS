"""
Currency Exchange Rate Ingestor
Fetches real-time exchange rates against USD and INR and stores
raw JSON payloads into the Bronze Data Lake partitioned by date.
"""

import sys
import json
from datetime import datetime, timezone
from pathlib import Path
import requests

# Ensure local config can be resolved from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import BRONZE_DIR, EXCHANGE_RATE_API_URL


def fetch_exchange_rates():
    print("💱 Fetching Currency Exchange Rates...")
    response = requests.get(EXCHANGE_RATE_API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    rates = data.get("rates", {})
    target_currencies = ["INR", "EUR", "GBP", "CAD", "AUD", "SGD", "AED", "JPY"]
    
    extracted_rates = {
        curr: rates.get(curr) for curr in target_currencies if curr in rates
    }

    record = {
        "base_currency": data.get("base_code", "USD"),
        "time_last_update_utc": data.get("time_last_update_utc"),
        "rates": extracted_rates,
        "inr_per_usd": extracted_rates.get("INR"),
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

    # Partition by Year / Month / Day
    now = datetime.now(timezone.utc)
    partition_path = BRONZE_DIR / "api" / "exchange_rates" / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / f"rates_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    print(f"  ✅ 1 USD = ₹{record['inr_per_usd']} INR")
    print(f"💾 Raw exchange rates successfully written to Bronze Data Lake:\n   👉 {output_file}")
    return output_file


if __name__ == "__main__":
    fetch_exchange_rates()
