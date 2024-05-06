import logging
from logging.handlers import RotatingFileHandler


def setup_logger(log_dump):
    logger = logging.getLogger('DaemonManager')
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(
        f'{log_dump}archiver.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=3
    )
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
