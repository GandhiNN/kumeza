import json
import yaml
import pathlib
import typing as t


class ConfigLoader:
    @staticmethod
    def load(f_path: str) -> t.Any:
        default_ext = ".json"
        file_ext = pathlib.Path(f_path).suffix
        with open(f_path, "r") as f:
            if file_ext == default_ext:
                input_file = json.load(f)
            else:
                input_file = yaml.safe_load(f)
        return input_file

