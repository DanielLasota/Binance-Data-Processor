import logging
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_logger(log_file_path: str) -> logging.Logger:
    logger = logging.getLogger('DaemonManager')
    logger.setLevel(logging.DEBUG)

    now_utc = datetime.utcnow().strftime('%d-%m-%YT%H-%M-%SZ')

    file_handler = RotatingFileHandler(
        f"{log_file_path}archiver{now_utc}.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3
    )

    logging.Formatter.converter = time.gmtime

    file_formatter = logging.Formatter('%(asctime)sUTC - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)sUTC - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
