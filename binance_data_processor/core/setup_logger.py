from __future__ import annotations

import logging
import sys
import time
from concurrent_log_handler import ConcurrentRotatingFileHandler


LOGGER_NAME = "binance_data_sink"


def setup_logger(dump: bool | None = False) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    logger.handlers.clear()
    logger.propagate = False

    logging.Formatter.converter = time.gmtime

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ [%(processName)-14s @%(process)5d] %(levelname)-5s -- %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if dump:
        file_handler = ConcurrentRotatingFileHandler(
            filename="archiver.log",
            maxBytes=5*1024*1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        def log_exception(exc_type, exc_value, exc_traceback):
            logger.error("Unhandled exception:", exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = log_exception

    return logger
