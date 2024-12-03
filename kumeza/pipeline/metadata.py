import json
import logging


logger = logging.getLogger(__name__)


class Writer:
    def __init__(self):
        pass

    def write(self, metadata: dict, file_format: str, output_file: str) -> None:
        if file_format == "json":
            with open(output_file, "w", encoding="utf-8") as fout:
                json_string = json.dumps(metadata, sort_keys=True, indent=4)
                fout.write(json_string)
        elif file_format == "csv":
            pass
        else:
            raise ValueError(f"Format: {format} not known")
