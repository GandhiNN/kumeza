import json
import pathlib
import typing as t

import yaml


class ConfigLoader:
    @staticmethod
    def load(f_path: str) -> t.Any:
        default_ext = ".json"
        file_ext = pathlib.Path(f_path).suffix
        with open(f_path, "r", encoding='utf-8') as f:
            if file_ext == default_ext:
                input_file = json.load(f)
            else:
                input_file = yaml.safe_load(f)
        return input_file
