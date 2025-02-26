import io
import logging
import sys
from logging import NullHandler


logging.getLogger(__name__).addHandler(NullHandler())


class Logger:
    def __init__(self, stream_type: str = "", log_level: str = "INFO"):
        self.logd = logging.getLogger(__name__)
        formatter = logging.Formatter(
            "%(levelname)s : %(asctime)s - %(processName)s - %(threadName)s - %(module)s.%(funcName)s => %(message)s"
        )
        stream = io.StringIO() if stream_type == "string" else sys.stdout
        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        # Set log level mapping
        lvl = {
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "DEBUG": logging.DEBUG,
        }
        self.logd.setLevel(lvl[log_level])

        self.logd.addHandler(handler)
