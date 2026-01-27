# on_cloud/extract/extractor.py
from __future__ import annotations

import os
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from utils.metadata.helper_metadata import (
    read_metadata_json,
    list_data_files,
    build_spark_schema_from_metadata,
    is_s3_like,
)
from utils.logger.helper_logger import LoggerManager
from utils.others.helper_functions import get_name_function


class Extractor:
    """
    Lee {root}/{batch_id}_{fecinfo}/ con:
      - metadata.json
      - data/ (*.parquet | *.csv[.gz] | *.txt[.gz] | *.dat[.gz])
    'root' prioriza paths.path_data_s3 (s3://), fallback paths.path_data_onprem.
    """

    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        # compat: en tu config el logger puede estar como log o _logger
        self.logger: LoggerManager = getattr(config, "log", getattr(config, "_logger", None))
        self.namef = get_name_function()

    def _base_root(self) -> str:
        paths = self.config._global.get_properties("paths") or {}
        path_s3 = (paths.get("path_data_s3") or "").strip()
        if path_s3:
            return path_s3.rstrip("/") + "/"
        path_local = (paths.get("path_data_onprem") or "").strip()
        if not path_local:
            raise ValueError("Extractor: paths.path_data_s3 ni paths.path_data_onprem configurados.")
        return path_local.rstrip("/") + "/"

    def _resolve_folder(self) -> str:
        if not getattr(self.config, "batch_id", None) or not getattr(self.config, "fecinfo", None):
            raise AttributeError("Extractor requiere config.batch_id y config.fecinfo.")
        root = self._base_root()
        return f"{root}{self.config.batch_id}_{self.config.fecinfo}"

    def _read_parquet(self, folder: str) -> DataFrame:
        data_dir = folder.rstrip("/") + "/data"
        pattern = data_dir  # Spark puede leer el directorio parquet
        self.logger.registrar("INFO", f"[{self.namef}] - Extract parquet => {pattern}")
        return self.spark.read.parquet(pattern)

    def _read_text_like(self, folder: str, md: Dict) -> DataFrame:
        data_dir = folder.rstrip("/") + "/data"
        meta = md.get("metadata", {}) if isinstance(md, dict) else {}
        fmt  = (meta.get("format") or "text").lower()
        comp = (meta.get("compression") or "").lower()
        delim = meta.get("delimiter") or "|"

        schema = build_spark_schema_from_metadata(md)

        # armar extensiones según compresión
        if comp == "gzip":
            exts = (".csv.gz", ".txt.gz", ".dat.gz", ".gz")
        else:
            exts = (".csv", ".txt", ".dat")

        files = list_data_files(self.spark, data_dir, exts)
        self.logger.registrar("INFO", f"[{self.namef}] - Extract text => {len(files)} files in {data_dir}")

        reader = self.spark.read.format("csv").option("sep", delim).option("header", "false")
        if comp == "gzip":
            reader = reader.option("compression", "gzip")
        if schema is not None:
            reader = reader.schema(schema)

        return reader.load(files)

    def run(self) -> DataFrame:
        self.logger.registrar("INFO", f"[{self.namef}] - Module Extract (gzip/text/parquet)")
        folder = self._resolve_folder()

        md = read_metadata_json(self.spark, folder)
        meta = md.get("metadata", {}) if isinstance(md, dict) else {}
        fmt = (meta.get("format") or "").lower()

        if fmt == "parquet":
            df = self._read_parquet(folder)
        else:
            df = self._read_text_like(folder, md)

        self.logger.registrar("INFO", f"[{self.namef}] - Extract OK. rows={df.count()}")
        return df
