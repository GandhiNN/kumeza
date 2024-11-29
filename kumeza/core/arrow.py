import datetime
import logging
from typing import Any, Union

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import s3fs

from kumeza.core.transformer import ArrowToHiveMapping
from kumeza.utils.common.date_object import DateObject


logger = logging.getLogger(__name__)

dateobj = DateObject()


class ArrowConverter:
    @classmethod
    def from_python_list(cls, result_sets: list[dict[str, Any]]) -> pa.Table:
        logger.info("Converting input to PyArrow table")
        return pa.Table.from_pylist(result_sets)

    @classmethod
    def from_csv(cls, f: str) -> pa.Table:
        logger.info("Converting CSV input to PyArrow table")
        return csv.read_csv(f)


class ArrowManager:  # pragma: no cover
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
    def write_parquet(
        cls,
        table: Union[pa.Table, list[pa.Table]],
        path: str,  # path in local or can be S3 URI
        prefix: str,
        ingestion_flag: str,
    ):
        cur_date = dateobj.get_timestamp_as_str(ts_format="date_filename")
        # input is a single pyarrow table object
        if isinstance(table, pa.Table):
            templ = f"{prefix}-00{{i}}-{cur_date}_utc_{ingestion_flag}.parquet"
            obj_name = f"{prefix}-000-{cur_date}_utc_{ingestion_flag}.parquet"
            logger.info("Writing Arrow table to %s/%s", path, obj_name)
            pq.write_to_dataset(
                table,
                root_path=path,
                basename_template=templ,
            )
        # input is list of pyarrow tables
        elif isinstance(table, list):  # noqa
            for idx, t in enumerate(table):
                seqnum = f"{idx:03}"  # 000, 001, 002...
                obj_name = f"{prefix}-{seqnum}-{cur_date}_utc_{ingestion_flag}.parquet"
                logger.info("Writing Arrow table to %s/%s", path, obj_name)
                pq.write_to_dataset(
                    t,
                    root_path=path,
                    basename_template=obj_name,
                )

    @classmethod
    def write_csv(
        cls,
        table: Union[pa.Table, list[pa.Table]],
        path: str,  # path in local or can be S3 URI
        prefix: str,
        ingestion_flag: str,
    ):
        cur_date = dateobj.get_timestamp_as_str(ts_format="date_filename")
        # input is a single pyarrow table object
        if isinstance(table, pa.Table):
            obj_name = f"{prefix}-000-{cur_date}_utc_{ingestion_flag}.csv"
            output_path = f"{path}/{obj_name}"
            logger.info("Writing Arrow table to %s", output_path)
            csv.write_csv(table, output_file=output_path)
        # input is list of pyarrow tables
        elif isinstance(table, list):  # noqa
            for idx, t in enumerate(table):
                seqnum = f"{idx:03}"  # 000, 001, 002...
                obj_name = f"{prefix}-{seqnum}-{cur_date}_utc_{ingestion_flag}.parquet"
                output_path = f"{path}/{obj_name}"
                logger.info("Writing Arrow table to %s", output_path)
                csv.write_csv(t, output_file=output_path)

    @classmethod
    def read_from_s3(cls, s3uri: str) -> pq.ParquetDataset:
        logger.info("Reading parquet file into PyArrow table")
        fs = s3fs.S3FileSystem(anon=False, use_ssl=True)
        return pq.ParquetDataset(s3uri, filesystem=fs)

    @classmethod
    def convert_int64_to_timestamp(cls, col: pa.Table.column) -> pa.Array:
        """Convert to datetime object with second precision (floored, not rounded)"""
        ts_list = col.to_pylist()
        ts = [
            datetime.datetime.fromtimestamp(x // 1e9).astimezone(datetime.timezone.utc)
            for x in ts_list
        ]
        return pa.array(ts)
