import re


class Sanitizers:

    @staticmethod
    def replace_multiple_whitespace_with_one_whitespace(s: str) -> str:
        return re.sub(r"\s+", " ", s)

    @staticmethod
    def remove_leading_and_trailing_whitespace(s: str) -> str:
        return re.sub(r"^\s+|\s+$", "", s)

    @staticmethod
    def remove_leading_and_trailing_backticks(s: str) -> str:
        return re.sub(r"^`+|`+$", "", s)

    @staticmethod
    def remove_extraneous_blank_lines(s: str) -> str:
        return re.sub(r"^$\n", "", s, flags=re.MULTILINE)

    @staticmethod
    def replace_special_characters_with_underscore(s: str) -> str:
        return re.sub(r"[ <>/!?+@()%#\"\\]", "_", s)
