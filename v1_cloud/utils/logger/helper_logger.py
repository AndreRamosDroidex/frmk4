import logging
import os
from logging.handlers import RotatingFileHandler

class LoggerManager:
    def __init__(self, id_tracking: str, id_process: str, fecinformacion: str, datacenter: str):
        # Atributos
        self.id_tracking = id_tracking
        self.id_process = id_process
        self.fecinformacion = fecinformacion
        self.datacenter = datacenter

        # Base del proyecto: primero ENV, si no, se infiere desde este archivo
        self.base_root = os.environ.get(
            "FRMK4_BASE",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
        )

        # Destinos activos (solo local new-edge en este server)
        self.destinos = {
            "local-new-egde": True,
            "local-ec2": False,
            "local-emr": False,
            "redshift": False,
            "cloudwatch": False,
            "dynamo": False,
        }

        # Inicializar logger de archivo/consola si algún destino local está activo
        self.logger = (
            self._configure_logger_file_local()
            if (self.destinos.get("local-new-egde") or
                self.destinos.get("local-ec2") or
                self.destinos.get("local-emr"))
            else None
        )

        # Placeholders para otros destinos
        self.db_conn = self._init_redshift() if self.destinos["redshift"] else None
        self.cw_client = self._init_cloudwatch() if self.destinos["cloudwatch"] else None
        self.ddb_client = self._init_dynamo() if self.destinos["dynamo"] else None

    # --------------------------------------------------------------------
    # Configuración del logger (archivo y consola)
    # --------------------------------------------------------------------
    def _configure_logger_file_local(self):
        self.log_level = logging.INFO

        # Todas las variantes locales usan la misma carpeta relativa a base_root
        self.path_log = os.path.join(self.base_root, "tmp", "logs")
        os.makedirs(self.path_log, exist_ok=True)

        self.log_file_local = os.path.join(
            self.path_log,
            f"{self.id_process}_{self.fecinformacion}_{self.datacenter}_{self.id_tracking}.log"
        )

        logger = logging.getLogger(self.id_process)
        logger.setLevel(self.log_level)

        # Evitar duplicidad de handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler de archivo rotativo
        file_handler = RotatingFileHandler(
            self.log_file_local, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(self.log_level)
        logger.addHandler(file_handler)

        # Consola solo si ENV=dev (por defecto)
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
            # return create_engine(self.config.db_url).connect()
            pass
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar conexión a BD: {e}")
        return None

    def _init_cloudwatch(self):
        try:
            # import boto3
            # return boto3.client("logs", region_name=self.config.aws_region)
            pass
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar CloudWatch: {e}")
        return None

    def _init_dynamo(self):
        try:
            # import boto3
            # return boto3.resource("dynamodb", region_name=self.config.aws_region)
            pass
        except Exception as e:
            if self.logger:
                self.logger.warning(f"No se pudo inicializar DynamoDB: {e}")
        return None

    # --------------------------------------------------------------------
    # Escritura en archivo (local)
    # --------------------------------------------------------------------
    def _write_to_file(self, level, message, param_adic_1=None):
        if not self.logger:
            return
        level = level.lower()
        log_func = getattr(self.logger, level, self.logger.info)
        log_func(message)

    # --------------------------------------------------------------------
    # Método público: registra mensaje
    # --------------------------------------------------------------------
    def registrar(self, level, message, param_adic_1=None):
        level = level.lower()
        niveles_validos = {"info", "warning", "error"}
        if level not in niveles_validos:
            raise ValueError(f"Nivel inválido: '{level}'. Use: {', '.join(sorted(niveles_validos))}")

        if self.destinos["local-new-egde"] or self.destinos["local-ec2"] or self.destinos["local-emr"]:
            self._write_to_file(level, message, param_adic_1)

        if self.destinos["redshift"]:
            self._write_to_redshift(level, message, param_adic_1)
        if self.destinos["cloudwatch"]:
            self._write_to_cloudwatch(level, message, param_adic_1)
        if self.destinos["dynamo"]:
            self._write_to_dynamo(level, message, param_adic_1)

    # --------------------------------------------------------------------
    # Placeholders externos
    # --------------------------------------------------------------------
    def _write_to_redshift(self, level, message, param_adic_1=None):
        if not self.db_conn:
            return

    def _write_to_cloudwatch(self, level, message, param_adic_1=None):
        if not self.cw_client:
            return

    def _write_to_dynamo(self, level, message, param_adic_1=None):
        if not self.ddb_client:
            return

    def getLogger(self):
        return self.logger

    # Subir a S3 (si alguna vez apuntas path_log a s3://)
    def upload_to_s3(self):
        import boto3
        if not self.path_log.startswith("s3://"):
            return
        s3 = boto3.client("s3")
        path = self.path_log.replace("s3://", "")
        bucket = path.split("/")[0]
        prefix = "/".join(path.split("/")[1:]).rstrip("/")
        key = f"{prefix}/{os.path.basename(self.log_file_local)}"
        s3.upload_file(self.log_file_local, bucket, key)
        print(f"Log subido a S3: s3://{bucket}/{key}")
