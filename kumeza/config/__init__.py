import typing as t
from dataclasses import dataclass


@dataclass
class BaseConfig:

    def get_length(self):
        return len(self.__dict__)

    def get_members(self):
        members = list(self.__dict__.items())
        return members
