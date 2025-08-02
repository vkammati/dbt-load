import logging
from logging import Logger


def get_logger(logger_name: str, level: str = None) -> Logger:
    """
    Create new logger with optional log level. If logger with the same
    name already exists, return it.
    """

    logger = logging.getLogger(logger_name.lower())
    if level:
        log_level = logging.getLevelName(level)
        logger.setLevel(log_level)

    return logger


def set_handler(logger: Logger):
    """
    Set specific handler on provided logger if it does not have any handlers yet.
    """
    if not logger.hasHandlers():
        print(f"Setting logger for {logger}")

        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(stream_handler)
