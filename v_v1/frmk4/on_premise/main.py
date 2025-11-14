import sys
import os
from extract.extractor import Extractor
from utils.logger.helper_logger import LoggerManager
from utils.config.helper_config import ConfigManager
from utils.others.helper_functions import get_name_function

def main():
    if len(sys.argv) < 3:
        print("Error # Parametros")
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
            datacenter = "on_premise"
     )
    
    ##  Crear el objeto del config
    configManager = ConfigManager(
            id_tracking = id_tracking,
            id_process = id_process,
            fecinformacion = fecinformacion,
            datacenter = "on_premise",
            log = log
     )
    config = configManager.getConfig()
    
    
    try:
        # Inicio Proceso
        log.registrar("INFO", f"[{name_function}] - === Iniciando extracción: {id_process} ({fecinformacion}) ===")
        
        # === Module Extract On-Premises ===
        extractor = Extractor(config,log)
        extractor.run()
        
        #log.info("Proceso completado exitosamente")
        log.registrar("INFO", f"[{name_function}] - Proceso completado exitosamente")
        
    except Exception as e:
        log.registrar("ERROR", f"[{name_function}] - Error durante la extracción: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
