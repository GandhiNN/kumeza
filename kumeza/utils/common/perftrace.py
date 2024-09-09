import logging
import time
from typing import Callable


logger = logging.getLogger(__name__)


class PerfTrace:
    @staticmethod
    def timeit(f: Callable) -> Callable:
        def timed(*args, **kwargs):
            ts = time.time()
            result = f(*args, **kwargs)
            te = time.time()
            logger.info("func: %s, took %s sec", f.__name__, str(te - ts))
            return result

        return timed
