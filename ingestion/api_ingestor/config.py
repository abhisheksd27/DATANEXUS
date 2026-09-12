"""
Configuration settings for API Ingestor
Defines endpoints, coordinates, and local Bronze Data Lake paths.
"""

import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "datalake" / "bronze"

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
