
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class Transformer:
    def __init__(self, sparkSession, config):
        self.sparkSession = sparkSession
        self.config = config
        self.log = config.log

    def run(self, df) -> DataFrame:

        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        self.log.registrar("INFO",f"[{name_function}] - Module Transformer")
        
        configModuleTranform = self.config._process.get_module("transform")
        self.log.registrar("INFO",f"[{name_function}] - Configuracion del modulo Tranform: {configModuleTranform}")
        
        df.createOrReplaceTempView("source_table")
        sql_query = "SELECT codmes,coddoc FROM source_table"
        self.log.registrar("INFO",f"[{name_function}] - Ejecutando SQL:")
        transformed_df = self.sparkSession.sql(sql_query)

        self.log.registrar("INFO",f"[{name_function}] - Registros transformados: {transformed_df.count()}")
        
        return transformed_df
