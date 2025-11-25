import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    # Make sure logs folder exists
    os.makedirs("logs", exist_ok=True)

    # Log file per day, e.g. logs/2025-01-22.log
    log_filename = datetime.now().strftime("logs/%Y-%m-%d.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if get_logger is called multiple times
    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
