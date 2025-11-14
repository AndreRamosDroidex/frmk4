from typing import Dict, Optional
from pyspark.sql import DataFrame
from load.s3.load_s3 import LoadS3
from load.redshift.load_redshift import LoadRedshift
from load.teradata.load_teradata import LoadTeradata
from utils.others.helper_functions import get_name_function

class Loader:
    """
    Orquestador de cargas.
    - Lee process_modules.load
    - Para cada item usa el DataFrame según transform_id (si viene).
    - Soporta S3 / Redshift / Teradata (Athena mapeado a S3 como POC).
    - Mantiene compat: .run(df) ejecuta con un único DF.
    """

    def __init__(self, sparkSession, config):
        self.spark = sparkSession
        self.config = config
        self.log = config.log

        # Instancias de loaders
        self._s3 = LoadS3(self.spark, self.config, self.log)
        self._rs = LoadRedshift(self.spark, self.config, self.log)
        self._td = LoadTeradata(self.spark, self.config, self.log)

    # ----------------------------
    # Compatibilidad legacy: un DF
    # ----------------------------
    def run(self, df: DataFrame):
        """
        MODO LEGACY: ejecuta todos los loads usando un único DataFrame.
        """
        name_function = get_name_function()
        self.log.registrar("INFO", f"[{name_function}] - Loader.run(df) (modo compat)")

        loads = self.config._process.get_module("load")
        if not loads:
            self.log.registrar("WARNING", f"[{name_function}] - No hay 'load' configurado.")
            return

        for lcfg in loads:
            ltype = (lcfg.get("load_type") or "").lower()
            props = lcfg.get("load_properties", {}) or {}
            self._dispatch_load(ltype, df, props, name_function, transform_id=None)

        self.log.registrar("INFO", f"[{name_function}] - Finalización Load (compat).")

    # ------------------------------------
    # Nuevo: múltiples DFs por transform_id
    # ------------------------------------
    def run_many(self, dfs_by_id: Dict[str, DataFrame], df_fallback: Optional[DataFrame] = None):
        """
        Ejecuta cargas respetando transform_id:
        - Si el item de 'load' tiene 'transform_id', usa dfs_by_id[transform_id].
        - Si no tiene 'transform_id', usa df_fallback; si no hay fallback, usa
          el último DF del dict (orden por clave).
        """
        name_function = get_name_function()
        self.log.registrar("INFO", f"[{name_function}] - Loader.run_many (multi-transform)")

        if not dfs_by_id and df_fallback is None:
            raise ValueError("No hay DataFrames de entrada para cargar.")

        loads = self.config._process.get_module("load")
        if not loads:
            self.log.registrar("WARNING", f"[{name_function}] - No hay 'load' configurado.")
            return

        # último DF como fallback si no se especifica transform_id
        last_df = None
        if dfs_by_id:
            # tomar el último por orden de aparición de clave (estable en py3.7+)
            last_key = list(dfs_by_id.keys())[-1]
            last_df = dfs_by_id[last_key]

        for lcfg in loads:
            ltype = (lcfg.get("load_type") or "").lower()
            props = lcfg.get("load_properties", {}) or {}
            t_id = lcfg.get("transform_id")  # opcional

            # Selección del DF
            if t_id:
                if t_id not in dfs_by_id:
                    self.log.registrar(
                        "WARNING",
                        f"[{name_function}] - transform_id='{t_id}' no existe. "
                        f"Se usará fallback."
                    )
                df_to_load = dfs_by_id.get(t_id) or df_fallback or last_df
            else:
                df_to_load = df_fallback or last_df

            if df_to_load is None:
                raise ValueError(
                    f"No se encontró DataFrame para load_type='{ltype}' "
                    f"(transform_id='{t_id}')."
                )

            self._dispatch_load(ltype, df_to_load, props, name_function, transform_id=t_id)

        self.log.registrar("INFO", f"[{name_function}] - Finalización Load (multi-transform).")

    # ----------------------------
    # Interno: enrutamiento loader
    # ----------------------------
    def _dispatch_load(self, ltype: str, df: DataFrame, props: dict, name_function: str, transform_id: Optional[str]):
        tag = f"transform_id={transform_id}" if transform_id else "transform_id=<default>"
        self.log.registrar("INFO", f"[{name_function}] - Ejecutando load_type={ltype} ({tag})")

        if ltype == "s3":
            self._s3.run(df, props)
        elif ltype == "redshift":
            self._rs.run(df, props)
        elif ltype == "teradata":
            self._td.run(df, props)
        elif ltype == "athena":
            # POC: escribir a S3 según props; CTAS/Glue vendrá luego
            self._s3.run(df, props)
            self.log.registrar("WARNING", f"[{name_function}] - ATHENA mapeado a S3 (CTAS pendiente).")
        else:
            self.log.registrar("WARNING", f"[{name_function}] - load_type desconocido: {ltype}")
