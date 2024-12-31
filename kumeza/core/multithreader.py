import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable


logger = logging.getLogger(__name__)


class MultithreadingManager:
    def __init__(self, worker_numbers: int = 10):
        logger.info("Setting up Multithreading manager with %s workers", worker_numbers)
        self.worker_numbers = worker_numbers

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
