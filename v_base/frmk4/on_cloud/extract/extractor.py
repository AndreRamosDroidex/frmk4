
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class Extractor:
    """
    Extrae datos desde archivos locales delimitados (por ejemplo '|')
    y los convierte a formato Parquet listo para enviar a S3 u otro destino.
    """

    def __init__(self, sparkSession, config):
        self.config = config
        self.log = config.log
        self.sparkSession = sparkSession
        
    def run(self) -> DataFrame:
        
        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        self.log.registrar("INFO",f"[{name_function}] - Module Exctract ")
        
        # Origenes
        self.origen = {
            "local-new-egde": False,
            "local-ec2": True,
            "s3": False
        }
        
        if self.origen.get("local-new-egde", False):
            # New Egde
            path_parquet = f"/data/desa/IBKProjects/DataHub/frmk4/tmp/inputs/processed-onprem/data.parquet"
        elif self.origen.get("local-ec2", False):
            # AWS - EC2
            path_parquet =  f"/home/ubuntu/frmk4/tmp/inputs/processed-onprem/data.parquet"
        elif self.origen.get("s3", False):
            # AWS - s3
            path_parquet = f"s3://.../data.parquet"
        
        df = self.sparkSession.read.format("parquet").load(path_parquet)
        return df
