# on_cloud/load/teradata/load_teradata.py
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function
import os

class LoadTeradata:
    """
    POC: en lugar de cargar a Teradata, escribe el DF en filesystem local.
    Compatible con Loader._dispatch_load(df, props).
    """
    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def run(self, df: DataFrame, props: dict):
        name_function = get_name_function()

        # Ruta destino local (se puede sobreescribir con props["local_path"])
        dest = props.get("local_path", "/home/ubuntu/frmk4/tmp/outputs/output_teradata/")
        dest = dest.rstrip("/") + "/"
        os.makedirs(dest, exist_ok=True)

        self.logger.registrar("INFO", f"[{name_function}] - (POC) Guardando resultados Teradata en: {dest}")
        df.write.mode("overwrite").parquet(dest)
        self.logger.registrar("INFO", f"[{name_function}] - Teradata (POC) OK: {dest}")
