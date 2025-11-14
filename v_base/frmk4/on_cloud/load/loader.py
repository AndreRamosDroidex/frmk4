from load.s3.load_s3 import LoadS3
from load.redshift.load_redshift import LoadRedshift
from load.teradata.load_teradata import LoadTeradata
from utils.others.helper_functions import get_name_function

class Loader:
    def __init__(self, sparkSession, config):
        self.sparkSession = sparkSession
        self.config = config
        self.log = config.log

    def run(self, df):
        
        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        self.log.registrar("INFO",f"[{name_function}] - Module Load")
        
        config_module_load = self.config._process.get_module("load")
        self.log.registrar("INFO",f"[{name_function}] - Configuracion del modulo Load: {config_module_load}")
        
        LoadS3(self.sparkSession, self.config, self.log).run(df)
        
        LoadRedshift(self.sparkSession, self.config, self.log).run(df)
        
        LoadTeradata(self.sparkSession, self.config, self.log).run(df)
        
        self.log.registrar("INFO",f"[{name_function}] - Finalizacion Load")
