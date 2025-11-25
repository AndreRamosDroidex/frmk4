# on_cloud/extract/extractor.py
import os
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class Extractor:
    """
    Extrae datos desde filesystem local (EC2) usando rutas parametrizadas:
      1) Si existe <paths.path_data_onprem>/data.parquet  -> lee Parquet
      2) Si no, intenta <paths.path_data_onprem>/data.txt o data.csv con delimitador '|'
         - intenta header=true; si no hay header real, renombra columnas a col_1, col_2, ...
    """

    def __init__(self, sparkSession, config):
        self.config = config
        self.log = config.log
        self.spark = sparkSession

    def _log(self, level: str, msg: str):
        name_function = get_name_function()
        self.log.registrar(level, f"[{name_function}] - {msg}")

    def run(self) -> DataFrame:
        self._log("INFO", "Module Extract")

        # Rutas parametrizadas (sin hardcode)
        paths = self.config.get_merged_properties_config_app("paths")
        base_dir = (paths.get("path_data_onprem") or "").rstrip("/")
        if not base_dir:
            raise ValueError("No se encontró 'path_data_onprem' en config (config_app:name='paths').")

        # 1) Intentar Parquet
        path_parquet = f"{base_dir}/data.parquet"
        if os.path.exists(path_parquet) or os.path.isdir(path_parquet):
            self._log("INFO", f"Leyendo Parquet: {path_parquet}")
            df = self.spark.read.format("parquet").load(path_parquet)
            self._log("INFO", f"Extract OK (Parquet). rows={df.count()}")
            return df

        # 2) Intentar TXT/CSV en el mismo directorio (con '|')
        candidates = [f"{base_dir}/data.txt", f"{base_dir}/data.csv"]
        file_path = next((p for p in candidates if os.path.exists(p)), None)
        if not file_path:
            raise FileNotFoundError(
                f"No se encontró archivo de entrada. Esperado uno de: "
                f"{path_parquet}, {candidates[0]} o {candidates[1]}"
            )

        delimiter = "|"
        self._log("INFO", f"Leyendo texto delimitado: {file_path} (delimiter='{delimiter}', header=true)")
        df = (self.spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", delimiter)
              .csv(file_path))

        # Si las columnas quedaron como _c0, _c1... => no había header real
        if all(c.startswith("_c") for c in df.columns):
            self._log("WARNING", "No se detectó header válido. Releyendo con header=false y asignando col_1, col_2, ...")
            df = (self.spark.read
                  .option("header", "false")
                  .option("inferSchema", "true")
                  .option("delimiter", delimiter)
                  .csv(file_path))
            new_cols = [f"col_{i+1}" for i in range(len(df.columns))]
            for old, new in zip(df.columns, new_cols):
                df = df.withColumnRenamed(old, new)

        self._log("INFO", f"Extract OK (TXT/CSV). rows={df.count()}")
        return df
