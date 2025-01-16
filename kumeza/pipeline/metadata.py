import csv
import json
import logging


logger = logging.getLogger(__name__)


class Writer:
    def __init__(self):
        pass

    def write_to_file(self, metadata: dict, file_format: str, output_file: str) -> None:
        if file_format == "json":
            with open(output_file, "w+", encoding="utf-8") as fout:
                json_string = json.dumps(metadata, indent=4)
                fout.write(json_string)
        elif file_format == "csv":
            with open(output_file, "w+", encoding="utf-8") as fout:
                w = csv.DictWriter(fout, fieldnames=metadata[0].keys())
                w.writeheader()
                w.writerows(metadata)
        else:
            raise ValueError(f"Format: {format} not known")
