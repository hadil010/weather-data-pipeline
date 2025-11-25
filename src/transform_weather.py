import json
import os
from datetime import datetime

from logger import get_logger
logger = get_logger(__name__)

def get_latest_bronze_file(bronze_folder="data/bronze"):
    logger.info("Searching for latest bronze JSON file...")
    if not os.path.exists(bronze_folder):
        logger.error(f"Bronze folder not found: {bronze_folder}")
        raise FileNotFoundError(f"Bronze folder not found: {bronze_folder}")

    files = [f for f in os.listdir(bronze_folder) if f.endswith(".json")]
    if not files:
        logger.error("No JSON files found in bronze layer.")
        raise FileNotFoundError("No JSON files found in bronze layer.")

    files.sort()
    latest_file = files[-1]
    path = os.path.join(bronze_folder, latest_file)

    logger.info(f"Latest bronze file selected: {path}")
    return path


def load_bronze_json(filepath):
    logger.info(f"Loading bronze JSON file: {filepath}")
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
        logger.info("Bronze JSON loaded successfully.")
        return data
    except Exception as e:
        logger.error(f"Failed to load JSON: {e}")
        raise


def clean_weather(raw_json, city_name="Edmonton"):
    logger.info("Starting weather data cleaning...")
    hourly = raw_json.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])

    if not times or not temps:
        logger.error("Missing 'time' or 'temperature_2m' fields in JSON.")
        raise ValueError("Missing 'time' or 'temperature_2m'")

    if len(times) != len(temps):
        logger.error("Time/temperature length mismatch.")
        raise ValueError("Length mismatch between time and temperature lists.")

    rows = []
    for t, temp in zip(times, temps):
        rows.append({"timestamp": t, "temp": temp, "city": city_name})

    logger.info(f"Cleaned weather rows count: {len(rows)}")
    return rows


def save_to_silver(rows, silver_folder="data/silver"):
    logger.info("Saving cleaned data to silver layer...")
    os.makedirs(silver_folder, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"clean_{today_str}.csv"
    filepath = os.path.join(silver_folder, filename)

    import csv
    fieldnames = ["timestamp", "temp", "city"]

    try:
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Silver CSV saved successfully → {filepath}")
    except Exception as e:
        logger.error(f"Failed to save silver CSV: {e}")
        raise


def main():
    logger.info("----- Starting Silver Transformation Step -----")

    bronze_path = get_latest_bronze_file()
    raw_json = load_bronze_json(bronze_path)
    rows = clean_weather(raw_json)
    save_to_silver(rows)

    logger.info("----- Silver Transformation Complete -----")


if __name__ == "__main__":
    main()
