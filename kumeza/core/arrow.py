import logging
from typing import Any, Union

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from kumeza.core.data import ArrowToHiveMapping
from kumeza.utils.common.date_object import DateObject


logger = logging.getLogger(__name__)

dateobj = DateObject()


class ArrowConverter:

    @classmethod
    def from_python_list(cls, result_sets: list[dict[str, Any]]) -> pa.Table:
        logger.info("Converting input to PyArrow table")
        return pa.Table.from_pylist(result_sets)


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
    def write_to_s3(
        cls,
        table: Union[pa.Table, list[pa.Table]],
        s3uri: str,
        table_name: str,
        ingestion_flag: str,
    ):
        logger.info("Writing Arrow table to %s", s3uri)
        cur_date = dateobj.get_current_timestamp(ts_format="date_filename")
        if isinstance(table, pa.Table):
            pq.write_to_dataset(
                table,
                root_path=s3uri,
                basename_template=f"{table_name}-00{{i}}-{cur_date}_utc_{ingestion_flag}.parquet",
            )
        # input is list of pyarrow tables
        elif isinstance(table, list):  # noqa
            for idx, t in enumerate(table):
                seqnum = f"{idx:03}"  # 000, 001, 002...
                pq.write_to_dataset(
                    t,
                    root_path=s3uri,
                    basename_template=f"{table_name}-{seqnum}-{cur_date}_utc_{ingestion_flag}.parquet",
                )

    @classmethod
    def read_from_s3(cls, s3uri: str) -> pq.ParquetDataset:
        logger.info("Reading parquet file into PyArrow table")
        fs = s3fs.S3FileSystem(anon=False, use_ssl=True)
        return pq.ParquetDataset(s3uri, filesystem=fs)
