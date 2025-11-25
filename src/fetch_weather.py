import requests
import json
import logging
from datetime import datetime
import os


from logger import get_logger

logger = get_logger(__name__)  # NEW

def fetch_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=53.55&longitude=-113.46&hourly=temperature_2m"
    )
    logger.info("Starting API call to Open-Meteo")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("Weather API call successful")
        return data
    except Exception as e:
        logger.error(f"API Error: {e}")
        return None

def save_to_bronze(data):
    os.makedirs("data/bronze", exist_ok=True)
    filename = datetime.now().strftime("%Y-%m-%d.json")
    filepath = os.path.join("data/bronze", filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved raw weather JSON to {filepath}")
    print(f"[OK] Raw weather saved → {filepath}")

def main():
    logger.info("Fetch weather job started")
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