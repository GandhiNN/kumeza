import unittest

import pytest

from kumeza.core.multithreader import MultithreadingManager


CHUNK_SIZE = 10000
EST_RECORD_SIZE = 1000


def add_ten(n: int, *args):
    # Return the input integer + 10.
    print(*args)
    return n + 10


class MultithreadingManagerTest(unittest.TestCase):
    def setUp(self):
        self.mt_manager = MultithreadingManager(
            chunk_size=CHUNK_SIZE, est_record_size=EST_RECORD_SIZE
        )

    def test_get_optimum_num_threads(self):
        self.mt_manager.cpu_count = 8
        self.mt_manager.memory = 10000000000
        assert (
            self.mt_manager.get_optimum_num_threads(
                chunk_size=CHUNK_SIZE, est_record_size=EST_RECORD_SIZE
            )
            == 32
        )

    def test_multithreaded_execution(self):
        iterables_input = [1, 2, 3, 4, 5]
        result_sets = self.mt_manager.execute(add_ten, iterables_input, "dummy")

        # Assert that all threads have completed successfully
        # Length of result sets should be the same of
        # the length of the iterables input
        assert len(result_sets) == len(iterables_input)

        # Sum of elements of the result sets should be 65
        # if not, then there should be a failure in one of more of the executors
        total_sum = 65
        assert sum(result_sets) == total_sum

        # Test for exceptions
        # Multithreading Manager will raise SystemExit upon executors' failure
        with pytest.raises(SystemExit):
            self.mt_manager.execute(add_ten, [1, 2, 3, "faultyinput"], "dummy")
