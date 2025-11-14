import os
import sys

# Sube niveles para importar utils
from security.data_protection import encrypt_dataframe, decrypt_dataframe
from utils.spark.helper_spark import get_spark_session
from utils.others.helper_functions import get_name_function


class Extractor:
    """
    Extrae datos desde archivos locales delimitados (por ejemplo '|')
    y los convierte a formato Parquet listo para enviar a S3 u otro destino.
    """

    def __init__(self, config, log):
        self.config = config
        self.log = log
        
    def run(self) -> str:
        
        # harcode
        
        delimiter = "|"
        header = True
        
        # Origen 
        self.origen = {
            "local-new-egde": False,
            "local-ec2": True,
            "s3": False
        }
        
        if self.origen.get("local-new-egde", False):
            # New Egde
            input_file = f"/data/desa/IBKProjects/DataHub/frmk4/tmp/inputs/DATATEST_202510.txt"
            output_path = f"/data/desa/IBKProjects/DataHub/frmk4/tmp/inputs/processed-onprem"
        elif self.origen.get("local-ec2", False):
            # AWS - EC2
            input_file = f"/home/ubuntu/frmk4/tmp/inputs/DATATEST_202510.txt"
            output_path = f"/home/ubuntu/frmk4/tmp/inputs/processed-onprem"
        elif self.origen.get("s3", False):
            # AWS - s3
            input_file = f"s3://.../DATATEST_202510.txt"
            output_path = f"s3://.../processed-onprem"
        
        
        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        # Crear carpeta si no existe
        os.makedirs(output_path, exist_ok=True)

        config_module_extract = self.config._process.get_module("extract")
        self.log.registrar("INFO",f"[{name_function}] - Configuracion del modulo extraccion: {config_module_extract}")
        
        # Crear sesión Spark
        spark_session = get_spark_session(self.config, self.log)
        
        self.log.registrar("INFO",f"[{name_function}] - Leyendo archivo: {input_file}")

        # Leer archivo CSV delimitado
        df = spark_session.read.option("header", header).option("delimiter", delimiter).csv(input_file)
        self.log.registrar("INFO",f"[{name_function}] - Registros leídos: {df.count()}")
        
        # Guardar el dataframe original parquet (sobrescribiendo si ya existe)
        path_parquet = os.path.join(output_path, "data.parquet")
        df.write.mode("overwrite").parquet(path_parquet)

        
        # Encriptar
        df_encrypted = encrypt_dataframe(df, ["CODDOC","PUESTO"], self.config, self.log)
        self.log.registrar("INFO",f"[{name_function}] - === DataFrame Encriptado ===")
        for row in df_encrypted.limit(5).collect():
            self.log.registrar("INFO",f"[{name_function}] - DataFrame Row: {row}")
       
        ## Guardar valor encriptado como parquet (sobrescribiendo si ya existe)
        path_parquet = os.path.join(output_path, "data.parquet.encrypted")
        df_encrypted.write.mode("overwrite").parquet(path_parquet)
        
        ## Desencriptar
        df_visible = decrypt_dataframe(df_encrypted, ["CODDOC","PUESTO"], self.config, self.log)
        self.log.registrar("INFO",f"[{name_function}] - === DataFrame Desencriptado ===")
        for row in df_visible.limit(5).collect():
            self.log.registrar("INFO",f"[{name_function}] - DataFrame Row: {row}")
        
        ## Guardar valor encriptado como parquet (sobrescribiendo si ya existe)
        path_parquet = os.path.join(output_path, "data.parquet.desencriptado")
        df_visible.write.mode("overwrite").parquet(path_parquet)


        spark_session.stop()
        return path_parquet
