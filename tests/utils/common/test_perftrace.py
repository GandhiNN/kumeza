import unittest

from kumeza.utils.common.perftrace import PerfTrace


class PerfTraceTest(unittest.TestCase):

    def test_timeit_decorator(self):

        @PerfTrace.timeit
        def decorated_function():
            pass

        decorated_function()
