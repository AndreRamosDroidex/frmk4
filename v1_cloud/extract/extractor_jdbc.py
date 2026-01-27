# on_cloud/extract/extractor_jdbc.py
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class ExtractorJDBC:
    """
    Extrae desde JDBC usando el primer bloque 'extract' (extract_type='jdbc')
    definido en el process. Soporta 'dbtable' o 'query'.
    """
    def __init__(self, sparkSession, config):
        self.spark = sparkSession
        self.config = config
        self.log = config.log

    def run(self) -> DataFrame:
        name_function = get_name_function()
        self.log.registrar("INFO", f"[{name_function}] - Module Extract (JDBC)")

        extracts = self.config._process.get_module("extract")
        if not extracts:
            raise ValueError("No hay módulo 'extract' en el proceso.")
        jdbc_cfg = next((e for e in extracts if (e.get("extract_type") or "").lower() == "jdbc"), None)
        if not jdbc_cfg:
            raise ValueError("No se encontró 'extract_type=jdbc' en process_modules.extract")

        conn_name = jdbc_cfg.get("data_connection")
        if not conn_name:
            raise ValueError("extract.jdbc: falta 'data_connection'.")

        # Traer conexión del GLOBAL
        conn = self.config._global.get_properties(conn_name)
        driver   = conn.get("driver")
        user     = conn.get("username")
        pwd      = conn.get("password")
        server   = conn.get("server")
        port     = conn.get("port")
        database = conn.get("database")
        url_tpl  = conn.get("connection_url")

        if not all([driver, user, pwd, server, port, database, url_tpl]):
            raise ValueError(f"Conexión '{conn_name}' incompleta en config-global.")

        url = (url_tpl
               .replace("{{server}}", str(server))
               .replace("{{port}}", str(port))
               .replace("{{database}}", str(database)))

        reader = (self.spark.read.format("jdbc")
                  .option("url", url)
                  .option("driver", driver)
                  .option("user", user)
                  .option("password", pwd))

        if jdbc_cfg.get("query"):
            self.log.registrar("INFO", f"[{name_function}] - JDBC query")
            df = reader.option("query", jdbc_cfg["query"]).load()
        elif jdbc_cfg.get("dbtable"):
            dbtable = jdbc_cfg["dbtable"]
            self.log.registrar("INFO", f"[{name_function}] - JDBC dbtable={dbtable}")
            df = reader.option("dbtable", dbtable).load()
        else:
            raise ValueError("extract.jdbc requiere 'dbtable' o 'query'.")

        self.log.registrar("INFO", f"[{name_function}] - Extract JDBC OK. rows={df.count()}")
        return df