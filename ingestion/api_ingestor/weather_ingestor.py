"""
Weather Data Ingestor
Fetches current temperature, rain, windspeed, and weather conditions for e-commerce delivery cities
and stores the raw JSON payload into Bronze Data Lake partitioned by date.
"""

import json
from datetime import datetime, timezone
import requests
from config import BRONZE_DIR, CITIES_COORDINATES, WEATHER_API_URL


def fetch_city_weather(city_name, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "weather_code", "wind_speed_10m"],
        "timezone": "auto"
    }
    response = requests.get(WEATHER_API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    
    current = data.get("current", {})
    return {
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
        "temperature_celsius": current.get("temperature_2m"),
        "apparent_temperature_celsius": current.get("apparent_temperature"),
        "humidity_percentage": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "rain_mm": current.get("rain"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "weather_code": current.get("weather_code"),
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }


def ingest_all_cities():
    print("🌦️ Starting Weather Data Ingestion for top e-commerce hubs...")
    all_weather_data = []

    for city, coords in CITIES_COORDINATES.items():
        try:
            weather_record = fetch_city_weather(city, coords["lat"], coords["lon"])
            all_weather_data.append(weather_record)
            print(f"  ✅ {city}: {weather_record['temperature_celsius']}°C | Rain: {weather_record['rain_mm']}mm | Wind: {weather_record['wind_speed_kmh']} km/h")
        except Exception as e:
            print(f"  ❌ Failed to fetch weather for {city}: {e}")

    # Partition by Year / Month / Day
    now = datetime.now(timezone.utc)
    partition_path = BRONZE_DIR / "api" / "weather" / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / f"weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_weather_data, f, indent=2)

    print(f"💾 Raw weather data successfully written to Bronze Data Lake:\n   👉 {output_file}")
    return output_file


if __name__ == "__main__":
    ingest_all_cities()
