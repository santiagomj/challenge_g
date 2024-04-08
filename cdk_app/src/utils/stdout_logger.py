import os
import logging


class Singleton(type):
    """
    Singleton is a metaclass for creating singleton classes. Singleton classes are classes that can have only one
    instance at any time. It means if you create an object and try to create another one, the same instance will be returned.

    Usage:
    from logger import Singleton

    class Logger(metaclass=Singleton):
        ...
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Logger(metaclass=Singleton):
    """
    Logger is a class for application-level logging. It provides methods to write log messages to standard output.
    Log messages include timestamp, script name and line number, log level and the message itself.

    This class follows Singleton pattern to avoid duplicate log messages.

    Usage:
    from logger import Logger

    logger = Logger("MyScript").get_logger()
    logger.info("This is an info message")
    """

    def __init__(self, logger_name: str, level: str = None):
        """
        :param logger_name: The name of the logger to be created.
        :type logger_name: str
        :param level: The logging level. It can be "DEBUG", "INFO", "WARNING", "ERROR" or "CRITICAL".
                      If not provided, the logging level will be taken from LOG_LEVEL environment variable.
                      If LOG_LEVEL is also not set, it defaults to "INFO".
        :type level: str, optional
        """
        self.logger = logging.getLogger(logger_name)

        log_level = int(
            level if level is not None else os.getenv("LOG_LEVEL", logging.INFO)
        )
        self.logger.setLevel(log_level)

        # Only add handlers if the logger has no handlers yet
        if not self.logger.hasHandlers():
            ch = logging.StreamHandler()
            ch.setLevel(int(log_level))

            formatter = logging.Formatter(
                "%(asctime)s [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def get_logger(self) -> logging.Logger:
        """
        :return: Returns the logger instance.
        :rtype: logging.Logger
        """
        return self.logger
