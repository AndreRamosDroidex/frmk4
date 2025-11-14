from pyspark.sql import SparkSession

from utils.others.helper_functions import get_name_function

def get_spark_session(config,log):
    
    try:
        
        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        # Inicializamos la session de spark
        id_spark =  f"{config.id_process}_{config.fecinformacion}_{config.datacenter}_{config.id_tracking}"
        log.registrar("INFO",f"[{name_function}] - Inicializando session SPARK")
        sparkBuilder = SparkSession.builder \
            .appName(id_spark) \
            .master("local[4]")
        
        #Obtener las configuraciones para el spark
        spark_configs = config.get_merged_properties_config_app("spark-config")
        log.registrar("INFO",f"[{name_function}] - spark_configs = {spark_configs}")
        
        # Agregar configuraciones desde el diccionario
        for key, value in spark_configs.items():
            sparkBuilder = sparkBuilder.config(key, value)
        
        # Crear la sesion
        spark_session = sparkBuilder.getOrCreate()
            
        log.registrar("INFO",f"[{name_function}] - SPARK inicializado")
        
    except Exception as e:
        log.registrar("ERROR",f"[{name_function}] - Error al inicializar Spark: {e}")

    return spark_session
