# on_cloud/load/teradata/load_teradata.py
import os
import json
import boto3
from typing import Optional, Dict
from pyspark.sql import DataFrame, functions as F, types as T

from utils.others.helper_functions import get_name_function
from utils.jdbc.driver_loader import ensure_driver_loaded


def _get_region():
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"


def _get_secret_credentials(secret_name: str) -> Dict[str, str]:
    region = _get_region()
    session = boto3.session.Session(region_name=region)
    client = session.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_name)

    username = None
    password = None

    secret_string = resp.get("SecretString")
    if secret_string:
        try:
            payload = json.loads(secret_string)
            username = payload.get("username") or payload.get("user") or payload.get("usr")
            password = payload.get("password") or payload.get("pwd")
        except json.JSONDecodeError:
            password = secret_string
    else:
        import base64
        decoded = base64.b64decode(resp["SecretBinary"]).decode("utf-8")
        try:
            payload = json.loads(decoded)
            username = payload.get("username") or payload.get("user") or payload.get("usr")
            password = payload.get("password") or payload.get("pwd")
        except json.JSONDecodeError:
            password = decoded

    return {"username": username, "password": password}


def _render_url(tpl: str, props: Dict[str, str]) -> str:
    if "{{" not in tpl:
        return tpl
    out = tpl
    for k in ("server", "database", "port"):
        out = out.replace("{{" + k + "}}", str(props.get(k, "")))
    return out


def _fill_null_to_empty(df: DataFrame) -> DataFrame:
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    if str_cols:
        df = df.fillna("", subset=str_cols)
    return df


class LoadTeradata:
    """
    Carga vía JDBC a Teradata usando 'com.teradata.jdbc.TeraDriver'.
    - Credenciales desde Secrets Manager (siempre que 'secret_name' exista en props o en la conexión).
    - Soporta overwrite con truncate.
    - 'null_to_empty' = 'Y' para normalizar strings.
    """

    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def _find_conn_props(self, connection_name: str) -> Dict[str, str]:
        """
        Obtiene las propiedades de conexión desde el GlobalConfig correcto.
        - Fuente principal: self.config._global.get_properties(<name>)
        - Fallback (por compatibilidad): self.config.global_json (si existiera)
        """
        # Fuente correcta (GlobalConfig envuelto en helper_config)
        try:
            return self.config._global.get_properties(connection_name)  # ← ESTA es la ruta correcta
        except Exception as e:
            # Fallback por compatibilidad con versiones antiguas
            cfg = getattr(self.config, "global_json", {}) or {}
            arr = cfg.get("config_app", [])
            for item in arr:
                if item.get("name") == connection_name:
                    return item.get("properties", {})
            raise ValueError(
                f"No se encontró la conexión '{connection_name}' en config_app."
            ) from e

    def run(self, df: DataFrame, props: dict, lcfg: Optional[dict] = None):
        namef = get_name_function()

        connection_name = props.get("connection_name")
        if not connection_name:
            raise ValueError("LoadTeradata: 'connection_name' es obligatorio.")

        dbtable = props.get("dbtable")
        if not dbtable:
            raise ValueError("LoadTeradata: 'dbtable' es obligatorio.")

        mode = (props.get("mode") or "append").lower().strip()
        null_to_empty = (props.get("null_to_empty") or "N").upper().strip() == "Y"
        error_continue = (props.get("error_continue") or "n").lower().strip() in ("y", "yes", "true", "1")

        # 1) Conexión base desde config global
        conn = self._find_conn_props(connection_name)
        driver = conn.get("driver") or "com.teradata.jdbc.TeraDriver"
        url_tpl = conn.get("connection_url") or ""
        url = _render_url(url_tpl, conn)

        # 2) Credenciales con Secrets (override)
        secret_name = props.get("secret_name") or conn.get("secret_name") or os.getenv("TERADATA_SECRET_NAME") or "ibk/frmk4/db/teradata"
        try:
            creds = _get_secret_credentials(secret_name)
        except Exception as e:
            if error_continue:
                self.logger.registrar("WARNING", f"[{namef}] - No se pudo leer Secret '{secret_name}': {e}. Se intentará credenciales del config.")
                creds = {}
            else:
                raise

        username = creds.get("username") or conn.get("username")
        password = creds.get("password") or conn.get("password")
        if not password:
            raise ValueError("LoadTeradata: no se obtuvo 'password' ni por Secret ni por config.")
        if not username:
            self.logger.registrar("WARNING", f"[{namef}] - 'username' no presente; el driver podría requerirlo.")

        # 3) Registrar driver explícitamente (para DriverManager y para writer JDBC)
        ensure_driver_loaded(self.spark, self.logger)

        # 4) Limpieza opcional NULL->""
        out_df = _fill_null_to_empty(df) if null_to_empty else df

        # 5) Escribir JDBC
        self.logger.registrar("INFO", f"[{namef}] - Teradata url={url} table={dbtable} mode={mode} driver={driver}")
        writer = (out_df.write
                  .format("jdbc")
                  .option("url", url)
                  .option("dbtable", dbtable)
                  .option("driver", driver)
                  .option("user", username if username else "")
                  .option("password", password))

        if mode == "overwrite":
            writer = writer.mode("overwrite").option("truncate", "true")
        else:
            writer = writer.mode("append")

        try:
            writer.save()
            self.logger.registrar("INFO", f"[{namef}] - Carga Teradata completada.")
        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - Error JDBC Teradata: {e}. Continuando por error_continue.")
            else:
                raise
