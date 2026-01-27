# on_cloud/load/postgres/load_postgres.py
from typing import Optional, List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, TimestampType
from utils.others.helper_functions import get_name_function


def _render_url(tpl: str, kv: Dict[str, str]) -> str:
    url = tpl or ""
    for k, v in (kv or {}).items():
        url = url.replace(f"{{{{{k}}}}}", str(v))
    return url


class LoadPostgres:

    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    # ---------------------------
    # API
    # ---------------------------
    def run(
        self,
        df: DataFrame,
        props: dict,
        fecinfo: Optional[str] = None,
        batch_id: Optional[int] = None,
    ):
        namef = get_name_function()

        # --- 1) Cargar propiedades de conexión ---
        conn_name = (props.get("connection_name") or "postgres-cnx-01").strip()
        try:
            conn = self.config._global.get_properties(conn_name)
        except Exception:
            raise KeyError(f"No se encontró configuración global con name='{conn_name}'")

        user = conn.get("username")
        pwd = conn.get("password")
        driver = conn.get("driver") or "org.postgresql.Driver"
        url_tpl = conn.get("connection_url") or "jdbc:postgresql://{{server}}:{{port}}/{{database}}"

        url = _render_url(
            url_tpl,
            {
                "server": conn.get("server", ""),
                "port": conn.get("port", ""),
                "database": conn.get("database", ""),
            },
        )

        # --- 2) Propiedades de carga ---
        table = props.get("dbtable")
        if not table:
            raise ValueError("LoadPostgres: 'dbtable' es obligatorio en load_properties.")

        mode = (props.get("mode") or "append").lower()
        select_cols: Optional[List[str]] = props.get("select_cols")

        # --- 3) Columnas de auditoría con tipos explícitos (evita NullType/void) ---
        # Normaliza valores por si vienen None:
        fecinfo_val = str(fecinfo) if fecinfo is not None else ""
        batch_id_val = int(batch_id) if batch_id is not None else -1

        df_out = df
        if "fecinfo" not in df_out.columns:
            df_out = df_out.withColumn("fecinfo", lit(fecinfo_val).cast(StringType()))
        else:
            df_out = df_out.withColumn("fecinfo", col("fecinfo").cast(StringType()))

        if "batch_id" not in df_out.columns:
            df_out = df_out.withColumn("batch_id", lit(batch_id_val).cast(IntegerType()))
        else:
            df_out = df_out.withColumn("batch_id", col("batch_id").cast(IntegerType()))

        if "load_ts" not in df_out.columns:
            df_out = df_out.withColumn("load_ts", current_timestamp().cast(TimestampType()))
        else:
            df_out = df_out.withColumn("load_ts", col("load_ts").cast(TimestampType()))

        # Si el usuario pidió un orden/ subconjunto específico de columnas
        if select_cols:
            # Tomamos solo las que existan para evitar fallos por columnas inexistentes.
            ordered = [c for c in select_cols if c in df_out.columns]
            if not ordered:
                raise ValueError(
                    f"LoadPostgres: 'select_cols' no coincide con columnas del DF. DF cols={df_out.columns}"
                )
            df_out = df_out.select(*ordered)

        self.logger.registrar(
            "INFO",
            f"[{namef}] - PostgreSQL dest={table} mode={mode} "
            f"cols={list(df_out.columns)}",
        )

        # --- 4) Escritura JDBC ---
        (df_out.write
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", pwd)
            .option("driver", driver)
            # Opcionales útiles en PG (ajústalos si quieres):
            .option("truncate", "false")      # para overwrite con truncado (si aplica)
            .option("batchsize", "5000")
            .option("isolationLevel", "NONE")
            .mode(mode)
            .save()
        )

        self.logger.registrar(
            "INFO",
            f"[{namef}] - Carga PostgreSQL completada -> {table}"
        )
