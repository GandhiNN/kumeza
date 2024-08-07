import unittest

from kumeza.utils.stringop.sanitizers import Sanitizers


class SanitizersTest(unittest.TestCase):

    def setUp(self):
        self.sanitizers = Sanitizers()

    def test_replace_multiple_whitespace_with_one_whitespace(self):
        test_str = "this is  a   multiple     whitespace       test"
        assert (
            self.sanitizers.replace_multiple_whitespace_with_one_whitespace(test_str)
            == "this is a multiple whitespace test"
        )

    def test_remove_leading_and_trailing_whitespace(self):
        test_str = "  this sentence has a leading and trailing whitespace  "
        assert (
            self.sanitizers.remove_leading_and_trailing_whitespace(test_str)
            == "this sentence has a leading and trailing whitespace"
        )

    def test_remove_leading_and_trailing_backticks(self):
        test_str = "`this sentence has a leading and trailing backticks`"
        assert (
            self.sanitizers.remove_leading_and_trailing_backticks(test_str)
            == "this sentence has a leading and trailing backticks"
        )

    def test_replace_special_characters_with_underscore(self):
        test_str = "<this@sentence!contains_special#characters>"
        assert (
            self.sanitizers.replace_special_characters_with_underscore(test_str)
            == "_this_sentence_contains_special_characters_"
        )
