import json
import os
from datetime import datetime
from logger import get_logger
from config_loader import load_config
config = load_config()
paths_cfg = config["paths"]
city_name = config["city"]
logger = get_logger(__name__)

def get_latest_bronze_file(bronze_folder=paths_cfg["bronze_dir"]):
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


def clean_weather(raw_json, city_name="Edmonton", run_date=None):
    """
    Adds run_date to every row.
    run_date = the date your pipeline ran (NOT the weather timestamp)
    """
    logger.info("Starting weather data cleaning...")

    # If no run_date provided → use today's date
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")

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
        rows.append({
            "timestamp": t,
            "temp": temp,
            "city": city_name,
            "run_date": run_date
        })

    logger.info(f"Cleaned weather rows count: {len(rows)} with run_date={run_date}")
    return rows


def save_to_silver(rows, silver_folder=paths_cfg["silver_dir"], run_date=None):
    logger.info("Saving cleaned data to silver layer...")

    os.makedirs(silver_folder, exist_ok=True)

    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d")

    filename = f"clean_{run_date}.csv"
    filepath = os.path.join(silver_folder, filename)

    import csv
    fieldnames = ["timestamp", "temp", "city", "run_date"]

    try:
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Silver CSV saved successfully → {filepath}")
        print(f"[OK] Cleaned data saved → {filepath}")
    except Exception as e:
        logger.error(f"Failed to save silver CSV: {e}")
        raise


def main():
    logger.info("----- Starting Silver Transformation Step -----")

    run_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Pipeline run_date = {run_date}")

    bronze_path = get_latest_bronze_file()
    raw_json = load_bronze_json(bronze_path)

    rows = clean_weather(raw_json, run_date=run_date)
    save_to_silver(rows, run_date=run_date)

    logger.info("----- Silver Transformation Complete -----")


if __name__ == "__main__":
    main()
