import logging
from logging import Logger


class CustomFormatter(logging.Formatter):
    """
    Custom formatter class to set color coding per log level and
    the format string of the message itself.
    """

    blue = "\x1b[36m"
    yellow = "\x1b[33;1m"
    red = "\x1b[31;1m"
    reset = "\x1b[0m"

    format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    FORMATS = {
        logging.DEBUG: blue + format + reset,
        logging.INFO: format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


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

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(CustomFormatter())
        logger.addHandler(stream_handler)
