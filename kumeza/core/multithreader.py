import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import psutil


logger = logging.getLogger(__name__)


class MultithreadingManager:
    def __init__(self, chunk_size: int, est_record_size: int):
        # Get the internal properties
        self.cpu_count = psutil.cpu_count(logical=True)
        self.memory = psutil.virtual_memory().available
        # Init number of workers to be used
        self.worker_numbers = self.get_optimum_num_threads(chunk_size, est_record_size)
        logger.info(
            "Multithreader manager initiated with %d workers!", self.worker_numbers
        )

    def get_optimum_num_threads(self, chunk_size: int, est_record_size: int) -> int:
        # For I/O bound tasks, start with 2 to 4 times the number of CPU cores
        return min(4 * self.cpu_count, self.memory // (chunk_size * est_record_size))

    def execute(self, func: Callable, func_args: list, *args) -> list:
        result_sets: list = []
        with ThreadPoolExecutor(max_workers=self.worker_numbers) as executor:
            future_to_result_set = {
                executor.submit(func, arg, args[0]): arg for arg in func_args
            }
            logger.info(
                """Submitted task with the arguments = %s
                & %s to executors - waiting for threads to finish""",
                func_args,
                args[0],
            )

            for future in as_completed(future_to_result_set):
                result_set = future_to_result_set[future]
                try:
                    data = future.result()
                except Exception as e:  # pylint: disable=broad-except
                    sys.exit(f"{result_set} generated an exception: {e}")
                else:
                    result_sets.append(data)
        return result_sets
