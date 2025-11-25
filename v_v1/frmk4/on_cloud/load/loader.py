# on_cloud/load/loader.py
from typing import Dict, Optional
from pyspark.sql import DataFrame
from load.s3.load_s3 import LoadS3
from load.redshift.load_redshift import LoadRedshift
from load.teradata.load_teradata import LoadTeradata
from load.postgres.load_postgres import LoadPostgres   # <-- NUEVO
from utils.others.helper_functions import get_name_function

class Loader:
    def __init__(self, sparkSession, config):
        self.spark = sparkSession
        self.config = config
        self.log = config.log

        self._s3 = LoadS3(self.spark, self.config, self.log)
        self._rs = LoadRedshift(self.spark, self.config, self.log)
        self._td = LoadTeradata(self.spark, self.config, self.log)
        self._pg = LoadPostgres(self.spark, self.config, self.log)  # <-- NUEVO

    def run(self, df: DataFrame):
        # compat (no lo usaremos, pero lo dejamos)
        namef = get_name_function()
        self.log.registrar("INFO", f"[{namef}] - Loader.run(df) (modo compat)")
        loads = self.config._process.get_module("load")
        for lcfg in loads:
            ltype = (lcfg.get("load_type") or "").lower()
            props = lcfg.get("load_properties", {}) or {}
            self._dispatch_load(ltype, df, props, namef, transform_id=None)

    def run_many(self, dfs_by_id: Dict[str, DataFrame], df_fallback: Optional[DataFrame] = None):
        namef = get_name_function()
        self.log.registrar("INFO", f"[{namef}] - Loader.run_many (multi-transform)")

        loads = self.config._process.get_module("load")
        last_df = df_fallback or (dfs_by_id[list(dfs_by_id.keys())[-1]] if dfs_by_id else None)

        for lcfg in loads:
            ltype = (lcfg.get("load_type") or "").lower()
            props = lcfg.get("load_properties", {}) or {}
            t_id = lcfg.get("transform_id")  # opcional

            df_to_load = dfs_by_id.get(t_id) if t_id else last_df
            if df_to_load is None:
                raise ValueError(f"No DF para load_type={ltype} (transform_id='{t_id}').")

            self._dispatch_load(ltype, df_to_load, props, namef, transform_id=t_id)

        self.log.registrar("INFO", f"[{namef}] - Finalización Load (multi-transform).")

    def _dispatch_load(self, ltype: str, df: DataFrame, props: dict, name_function: str, transform_id: Optional[str]):
        tag = f"transform_id={transform_id}" if transform_id else "transform_id=<default>"
        self.log.registrar("INFO", f"[{name_function}] - Ejecutando load_type={ltype} ({tag})")

        if ltype == "s3":
            self._s3.run(df, props)
        elif ltype == "redshift":
            self._rs.run(df, props)
        elif ltype == "teradata":
            self._td.run(df, props)
        elif ltype in ("postgres", "jdbc_postgres", "jdbc"):
            # pasamos también metadata para columnas si se activa add_metadata
            self._pg.run(df, props, fecinfo=self.config.fecinformacion, batch_id=self.config.id_process)
        elif ltype == "athena":
            self._s3.run(df, props)
            self.log.registrar("WARNING", f"[{name_function}] - ATHENA mapeado a S3 (CTAS pendiente).")
        else:
            self.log.registrar("WARNING", f"[{name_function}] - load_type desconocido: {ltype}")
