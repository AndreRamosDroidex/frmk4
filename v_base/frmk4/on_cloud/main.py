from pyspark.sql import SparkSession
import sys
import os

# Importar los módulos
from extract.extractor import Extractor
from transform.transformer import Transformer
from load.loader import Loader

# Importar los Utils
from utils.logger.helper_logger import LoggerManager
from utils.config.helper_config import ConfigManager
from utils.others.helper_functions import get_name_function
from utils.spark.helper_spark import get_spark_session

def main():
    if len(sys.argv) < 3:
        print("Error de parametros")
        sys.exit(1)

    id_tracking = sys.argv[1]
    id_process = sys.argv[2]
    fecinformacion = sys.argv[3]

    # Obtener el nombre de esta funcion para usarlo en los logs
    name_function = get_name_function()

    ##  Crear el objeto del log
    log = LoggerManager(
            id_tracking = id_tracking,
            id_process = id_process,
            fecinformacion = fecinformacion,
            datacenter = "on_cloud"
     )
    
    ##  Crear el objeto del config
    configManager = ConfigManager(
            id_tracking = id_tracking,
            id_process = id_process,
            fecinformacion = fecinformacion,
            datacenter = "on_cloud",
            log = log
     )
    config = configManager.getConfig()
    
    try:
        # Inicio Proceso
        log.registrar("INFO",f"=== Iniciando extracción: {id_process} ({fecinformacion}) ===")
    
        # Crear sesión Spark
        spark_session = get_spark_session(config, log)
        
        # === Module Extract ===
        extractor = Extractor(spark_session, config)
        df_extracted = extractor.run()

        # === Module Transform ===
        transformer = Transformer(spark_session, config)
        df_transformed = transformer.run(df_extracted)

        # === Module Load ===
        loader = Loader(spark_session, config)
        loader.run(df_transformed)

        spark_session.stop()
    except Exception as e:
        log.registrar("ERROR",f"Error durante la extracción: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


