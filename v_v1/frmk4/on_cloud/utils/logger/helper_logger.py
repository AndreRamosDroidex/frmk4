import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

class LoggerManager:
    """
    Administra un logger que escribe en archivo, consola y puede conectar
    a otros destinos (BD, CloudWatch, DynamoDB, etc.).
    """

    def __init__(self, id_tracking: str, id_process: str, fecinformacion: str, datacenter: str):
        
        # Atributos 
        self.id_tracking = id_tracking
        self.id_process = id_process
        self.fecinformacion = fecinformacion
        self.datacenter = datacenter
        
        # Destinos activos
        self.destinos = {
            "local-new-egde": False,
            "local-ec2": True,
            "local-emr": False,
            "redshift": False,
            "cloudwatch": False,
            "dynamo": False,
        }
        
        
        # --- Inicializa el logger local solo si el destino 'local' está habilitado
        self.logger = self._configure_logger_file_local() if ( self.destinos.get("local-new-egde", False) or self.destinos.get("local-ec2", False) or self.destinos.get("local-emr", False)) else None

        # --- Inicializar conexiones externas para guardar mensajes de logs
        self.db_conn = self._init_redshift() if self.destinos["redshift"] else None
        self.cw_client = self._init_cloudwatch() if self.destinos["cloudwatch"] else None
        self.ddb_client = self._init_dynamo() if self.destinos["dynamo"] else None

    # --------------------------------------------------------------------
    # Configuración del logger (archivo y consola)
    # --------------------------------------------------------------------
    def _configure_logger_file_local(self):
        """
        Configura el logger local (archivo + consola) solo si el destino local está habilitado.
        """
        
        # Nivel de log
        self.log_level = logging.INFO

        if self.destinos.get("local-new-egde", False):
            # Local New Egde
            self.path_log = "/data/desa/IBKProjects/DataHub/frmk4/tmp/log"
        if self.destinos.get("local-ec2", False):
            # Local New Egde
            self.path_log = "/home/ubuntu/frmk4/tmp/log"
        elif self.destinos.get("local-emr", False):
            # Local EMR
            self.path_log = "/tmp/logs"

        # Crear directorio si no existe
        os.makedirs(self.path_log, exist_ok=True)

        # Nombre de archivo log
        self.log_file_local = os.path.join(
            self.path_log,
            f"{self.id_process}_{self.fecinformacion}_{self.datacenter}_{self.id_tracking}.log"
        )

        logger = logging.getLogger(self.id_process)
        logger.setLevel(self.log_level)

        # Evitar duplicidad de handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        # Formato de log
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # --- Handler de archivo ---
        file_handler = RotatingFileHandler(
            self.log_file_local, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(self.log_level)
        logger.addHandler(file_handler)

        # --- Handler de consola (solo en modo desarrollo) ---
        if os.environ.get("ENV", "dev").lower() == "dev":
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(self.log_level)
            logger.addHandler(console_handler)

        return logger

    # --------------------------------------------------------------------
    # Inicializadores de conexiones externas (placeholders)
    # --------------------------------------------------------------------
    def _init_redshift(self):
        try:
            print("Inicializando conexión a BD...")
            # return create_engine(self.config.db_url).connect()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar conexión a BD: {e}")
        return None

    def _init_cloudwatch(self):
        try:
            print("Inicializando cliente CloudWatch...")
            # import boto3
            # return boto3.client("logs", region_name=self.config.aws_region)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar CloudWatch: {e}")
        return None

    def _init_dynamo(self):
        try:
            print("Inicializando cliente DynamoDB...")
            # import boto3
            # return boto3.resource("dynamodb", region_name=self.config.aws_region)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar DynamoDB: {e}")
        return None

    # --------------------------------------------------------------------
    # Escritura en archivo (local)
    # --------------------------------------------------------------------
    def _write_to_file(self, level, message, param_adic_1 = None):
        if not self.logger:
            return  # Evita conflicto si existe algun logger activo

        level = level.lower()
        log_func = getattr(self.logger, level, self.logger.info)
        log_func(message)

    # --------------------------------------------------------------------
    # Escritura en destinos externos (placeholders)
    # --------------------------------------------------------------------
    def _write_to_redshift(self, level, message, param_adic_1 = None):
        if not self.db_conn:
            return
        try:
            print(f"[DB LOG] Nivel: {level} - Mensaje: {message}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error al enviar log a Redshift: {e}")

    def _write_to_cloudwatch(self, level, message, param_adic_1 = None):
        if not self.cw_client:
            return
        try:
            print(f"[CLOUDWATCH LOG] Nivel: {level} - Mensaje: {message}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error al enviar log a CloudWatch: {e}")

    def _write_to_dynamo(self, level, message, param_adic_1 = None):
        if not self.ddb_client:
            return
        try:
            print(f"[DYNAMODB LOG] Nivel: {level} - Mensaje: {message}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error al enviar log a DynamoDB: {e}")

    # --------------------------------------------------------------------
    # Método público: registra mensaje en log y otros destinos
    # --------------------------------------------------------------------
    def registrar(self, level, message, param_adic_1 = None):
        """Registra el mensaje en todos los destinos habilitados."""
        level = level.lower()
        niveles_validos = {"info", "warning", "error"}

        if level not in niveles_validos:
            raise ValueError(
                f"Nivel de log inválido: '{level}'. "
                f"Debe ser uno de los siguientes valores: {', '.join(sorted(niveles_validos))}"
            )
            
        if self.destinos["local-new-egde"] or self.destinos["local-ec2"]:
            self._write_to_file(level, message, param_adic_1)
        if self.destinos["redshift"]:
            self._write_to_redshift(level, message, param_adic_1)
        if self.destinos["cloudwatch"]:
            self._write_to_cloudwatch(level, message, param_adic_1)
        if self.destinos["dynamo"]:
            self._write_to_dynamo(level, message, param_adic_1)

    # --------------------------------------------------------------------
    # Obtener logger
    # --------------------------------------------------------------------
    def getLogger(self):
        return self.logger

    # --------------------------------------------------------------------
    # Subir a S3
    # --------------------------------------------------------------------
    def upload_to_s3(self):
        import boto3
        """Sube el log local a S3 al final del proceso"""
        if not self.path_log.startswith("s3://"):
            return

        s3 = boto3.client("s3")

        path = self.path_log.replace("s3://", "")
        bucket = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:]).rstrip("/")

        key = f"{prefix}/{os.path.basename(self.log_file_local)}"

        s3.upload_file(self.log_file_local, bucket, key)
        print(f"Log subido a S3: s3://{bucket}/{key}")


# --- Ejemplo de uso ---
if __name__ == "__main__":
    print("test")