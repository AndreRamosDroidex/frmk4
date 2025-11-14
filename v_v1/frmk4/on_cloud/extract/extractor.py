# on_cloud/extract/extractor.py
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class Extractor:
    def __init__(self, sparkSession, config):
        self.config = config
        self.log = config.log
        self.sparkSession = sparkSession

    def run(self) -> DataFrame:
        name_function = get_name_function()
        self.log.registrar("INFO", f"[{name_function}] - Module Extract")

        # Rutas parametrizadas (sin hardcode)
        paths = self.config.get_merged_properties_config_app("paths")
        base_dir = paths.get("path_data_onprem")
        if not base_dir:
            raise ValueError("No se encontró 'path_data_onprem' en config (config_app:name='paths').")

        path_parquet = f"{base_dir.rstrip('/')}/data.parquet"
        self.log.registrar("INFO", f"[{name_function}] - Leyendo Parquet: {path_parquet}")

        df = self.sparkSession.read.format("parquet").load(path_parquet)
        self.log.registrar("INFO", f"[{name_function}] - Extract OK. rows={df.count()}")
        return df
