# on_cloud/load/loader.py
from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame

from utils.others.helper_functions import get_name_function
from load.s3.load_s3 import LoadS3
from load.athena.load_athena import LoadAthena
from load.teradata.load_teradata import LoadTeradata


def _split_partition_by(s: Optional[str]) -> Optional[list]:
    if not s:
        return None
    parts = [x.strip() for x in s.split(",") if x.strip()]
    return parts or None


class Loader:
    """
    Ejecuta loads del JSON:
      - S3
      - athena
      - teradata
    """

    def __init__(self, spark, config, logger=None):
        self.spark = spark
        self.config = config
        self.logger = logger or getattr(config, "_logger", None)

    def _dispatch_load(self, load_type: str, df: DataFrame, props: dict, lcfg: Optional[dict] = None):
        lt = (load_type or "").lower()
        if lt == "s3":
            pby = _split_partition_by(props.get("partition_by") or props.get("partitions"))
            if pby:
                props = dict(props)
                props["partition_by"] = ",".join(pby)
            return LoadS3(self.spark, self.config, self.logger).run(df, props, lcfg or {})
        elif lt == "athena":
            return LoadAthena(self.spark, self.config, self.logger).run(df, props, lcfg or {})
        elif lt == "teradata":
            return LoadTeradata(self.spark, self.config, self.logger).run(df, props, lcfg or {})
        else:
            raise ValueError(f"Loader: load_type no soportado: {load_type}")

    def run_many(self, dfs_by_id: Dict[str, DataFrame], df_fallback: Optional[DataFrame] = None):
        namef = get_name_function()
        self.logger.registrar("INFO", f"[load.loader.run_many] - Loader.run_many (multi-transform)")

        loads = self.config._process.get_module("load") or []
        if not loads:
            self.logger.registrar("INFO", f"[{namef}] - No hay loads definidos.")
            return

        for ld in loads:
            ltype = ld.get("load_type")
            lprops = dict(ld.get("load_properties") or {})
            t_id = str(ld.get("transform_id") or "")
            df = None
            if t_id and t_id in dfs_by_id:
                df = dfs_by_id[t_id]
            elif df_fallback is not None:
                df = df_fallback

            if df is None:
                raise ValueError("No hay DataFrame para el load.")

            self._dispatch_load(ltype, df, lprops, lcfg=None)
