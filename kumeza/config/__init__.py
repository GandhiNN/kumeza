from dataclasses import dataclass, fields


@dataclass
class BaseConfig:
    def get_length(self):
        return len(self.__dict__)

    def get_field_name(self) -> list[str]:
        members = [field.name for field in fields(self)]
        return members
