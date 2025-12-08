import requests
import json
import logging
from datetime import datetime
import os
from logger import get_logger
from config_loader import load_config


logger = get_logger(__name__)
config = load_config()
api_cfg = config["weather_api"]
paths_cfg = config["paths"]
city_name = config["city"]
def build_weather_url() -> str:
    """
    Build the Open Meteo URL from configuration.
    """
    base_url = api_cfg["base_url"]
    lat = api_cfg["latitude"]
    lon = api_cfg["longitude"]
    hourly_params = api_cfg["hourly_parameters"]
    url = (
        f"{base_url}"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly={hourly_params}"
    )
    return url
def fetch_weather():
    url = build_weather_url()
    timeout = api_cfg.get("timeout_seconds", 10)
    logger.info("Starting API call to Open Meteo")
    logger.info("Request URL: %s", url)
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        logger.info("Weather API call successful")
        return data
    except Exception as e:
        logger.error("API Error: %s", e)
        return None
def save_to_bronze(data):
    bronze_dir = paths_cfg["bronze_dir"]
    os.makedirs(bronze_dir, exist_ok=True)
    filename = datetime.now().strftime("%Y-%m-%d.json")
    filepath = os.path.join(bronze_dir, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Saved raw weather JSON to %s", filepath)
    print(f"[OK] Raw weather saved → {filepath}")
def main():
    logger.info("Fetch weather job started for city: %s", city_name)
    print("Fetching weather data...")
    weather = fetch_weather()
    if weather:
        save_to_bronze(weather)
        logger.info("Fetch weather job finished successfully")
    else:
        logger.error("No data received from weather API")
        print("❌ No data received. Check logs for details.")
if __name__ == "__main__":
    main()