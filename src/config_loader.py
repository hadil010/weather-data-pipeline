import json
import os
from logger import get_logger
logger = get_logger(__name__)
CONFIG_PATH = os.path.join("config", "config.json")
def load_config(config_path: str = CONFIG_PATH) -> dict:
    """
    Load configuration from a JSON file and return it as a dictionary.
    """
    if not os.path.exists(config_path):
        logger.error("Config file not found at %s", config_path)
        raise FileNotFoundError(f"Config file not found: {config_path}")
    logger.info("Loading configuration from %s", config_path)
    with open(config_path, "r") as f:
        config = json.load(f)
    logger.info("Configuration loaded successfully")
    return config