import typing as t
from dataclasses import dataclass, fields


@dataclass
class BaseConfig:

    def get_length(self):
        return len(self.__dict__)

    def get_field_name(self):
        members = [field.name for field in fields(self)]
        # members = list(self.__dict__.items())
        return members
