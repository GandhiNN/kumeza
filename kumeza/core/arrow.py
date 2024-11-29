import datetime
import logging
from typing import Any, Union

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import s3fs

from kumeza.core.transformer import ArrowToHiveMapping


logger = logging.getLogger(__name__)


class ArrowIO:  # pragma: no-cover
    def __init__(self):
        pass

    def read(self, source_type: str, path: str) -> pa.Table:
        if source_type == "s3":
            logger.info("Reading from S3 bucket into Arrow table")
            fs = s3fs.S3FileSystem(anon=False, use_ssl=True)
            return pq.ParquetDataset(path, filesystem=fs)
        elif source_type == "csv":
            logger.info("Reading from CSV into Arrow table")
            return pv.read_csv(path)
        else:
            raise ValueError(f"{source_type} is not implemented yet")

    def write(
        self,
        table_obj: Union[pa.Table, list[pa.Table]],
        path: str,  # path in local or can be S3 URI
        prefix: str,
        ingestion_flag: str,
        cur_date: str,
        output_format: str = "parquet",
    ):
        if output_format == "csv":
            # Input is a single PyArrow table object
            if isinstance(table_obj, pa.Table):
                obj_name = f"{prefix}-000-{cur_date}_utc_{ingestion_flag}.csv"
                output = f"{path}/{obj_name}"
                logger.info("Writing Arrow table as %s", f"{output}")
                pv.write_csv(input, output_file=output)
            # Input is a list of PyArrow tables
            elif isinstance(table_obj, list):  # noqa
                for idx, t in enumerate(table_obj):
                    seqnum = f"{idx:03}"  # 000, 001, 002...
                    obj_name = (
                        f"{prefix}-{seqnum}-{cur_date}_utc_{ingestion_flag}.parquet"
                    )
                    output = f"{path}/{obj_name}"
                    logger.info("Writing Arrow table to %s", output)
                    pv.write_csv(t, output_file=output)
        elif output_format == "parquet":
            # input is a single pyarrow table object
            if isinstance(table_obj, pa.Table):
                templ = f"{prefix}-00{{i}}-{cur_date}_utc_{ingestion_flag}.parquet"
                obj_name = f"{prefix}-000-{cur_date}_utc_{ingestion_flag}.parquet"
                logger.info("Writing Arrow table to %s/%s", path, obj_name)
                pq.write_to_dataset(
                    table_obj,
                    root_path=path,
                    basename_template=templ,
                )
            # input is list of pyarrow tables
            elif isinstance(table_obj, list):  # noqa
                for idx, t in enumerate(table_obj):
                    seqnum = f"{idx:03}"  # 000, 001, 002...
                    obj_name = (
                        f"{prefix}-{seqnum}-{cur_date}_utc_{ingestion_flag}.parquet"
                    )
                    logger.info("Writing Arrow table to %s/%s", path, obj_name)
                    pq.write_to_dataset(
                        t,
                        root_path=path,
                        basename_template=obj_name,
                    )
            else:
                raise ValueError(f"{output_format} not implemented yet")


class ArrowUtils:  # pragma: no cover
    @classmethod
    def convert_python_list_to_arrow(
        cls, result_sets: list[dict[str, Any]]
    ) -> pa.Table:
        logger.info("Converting input to PyArrow table")
        return pa.Table.from_pylist(result_sets)

    @classmethod
    def get_schema(cls, table: pa.Table, hive: bool = False) -> list[dict]:
        schema = []
        logger.info("Getting table schema from Arrow table. Hive mapping = %s", hive)
        for field in table.schema:
            s = {
                "name": str(field.name),
                "type": (
                    ArrowToHiveMapping.transform_schema(str(field.type))
                    if hive
                    else str(field.type)
                ),
                "description": str(field.metadata),
                "nullable": str(field.nullable),
            }
            schema.append(s)
        return schema

    @classmethod
    def convert_int64_to_timestamp(cls, col: pa.Table.column) -> pa.Array:
        """Convert to datetime object with second precision (floored, not rounded)"""
        ts_list = col.to_pylist()
        ts = [
            datetime.datetime.fromtimestamp(x // 1e9).astimezone(datetime.timezone.utc)
            for x in ts_list
        ]
        return pa.array(ts)
