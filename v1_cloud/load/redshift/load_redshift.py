# on_cloud/load/redshift/load_redshift.py
# -*- coding: utf-8 -*-
"""
Loader de Redshift con:
- overwrite => DELETE FROM <schema.tabla>
- load_position: "Y" => alinea por posición según columnas reales en Redshift
- null_to_empty: "Y" => solo aplica a columnas destino STRING (VARCHAR/CHAR/TEXT)
- Casteos de tipos al tipo destino (INTEGER, DECIMAL(p,s), etc.)
- Fija currentSchema para que Spark detecte correctamente la existencia de tablas
- Logs útiles: lista tablas del schema y columnas de la tabla destino
"""
from __future__ import annotations

from typing import Dict, List, Tuple, Any
import re

from pyspark.sql import DataFrame, functions as F, types as T

from utils.others.helper_functions import get_name_function


# -------------------------------
# Utilidades JDBC (DDL / SELECT)
# -------------------------------
def _exec_ddl(spark, url: str, jprops: Dict[str, str], sql: str) -> None:
    """Ejecuta un DDL/statement (DELETE, TRUNCATE, GRANT, etc.) contra Redshift."""
    jvm = spark._sc._jvm  # pylint: disable=protected-access
    driver_manager = jvm.java.sql.DriverManager

    props_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        props_java.setProperty(k, v)

    conn = None
    stmt = None
    try:
        conn = driver_manager.getConnection(url, props_java)
        stmt = conn.createStatement()
        stmt.execute(sql)
    finally:
        try:
            if stmt is not None:
                stmt.close()
        finally:
            if conn is not None:
                conn.close()


def _exec_query(spark, url: str, jprops: Dict[str, str], sql: str) -> List[List[str]]:
    """Ejecuta un SELECT sencillo y devuelve filas como lista de listas (strings)."""
    jvm = spark._sc._jvm  # pylint: disable=protected-access
    driver_manager = jvm.java.sql.DriverManager

    props_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        props_java.setProperty(k, v)

    conn = None
    stmt = None
    rs = None
    try:
        conn = driver_manager.getConnection(url, props_java)
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        md = rs.getMetaData()
        ncols = md.getColumnCount()
        out: List[List[str]] = []
        while rs.next():
            row = []
            for i in range(1, ncols + 1):
                row.append(rs.getString(i))
            out.append(row)
        return out
    finally:
        try:
            if rs is not None:
                rs.close()
        finally:
            try:
                if stmt is not None:
                    stmt.close()
            finally:
                if conn is not None:
                    conn.close()


# -------------------------------
# Parsing & props
# -------------------------------
def _jdbc_props(conn: Dict[str, str]) -> Dict[str, str]:
    """Propiedades JDBC base para Redshift."""
    driver = (conn.get("driver") or "com.amazon.redshift.jdbc.Driver").strip()
    user = (conn.get("username") or "").strip()
    pwd = (conn.get("password") or "").strip()
    database = (conn.get("database") or "").strip()
    port = (conn.get("port") or "").strip() or "5439"
    # currentSchema lo fijamos al escribir (conocemos schema en ese punto)
    return {
        "driver": driver,
        "user": user,
        "password": pwd,
        "TCPKeepAlive": "true",
        "loginTimeout": "10",
        # Spark mapea DATABASE/DB/… según driver; no es imprescindible aquí
        "DBNAME": database,
        "PORT": port,
    }


def _infer_schema_table(
    conn: Dict[str, str], dbtable: str
) -> Tuple[str, str, str]:
    """
    Devuelve (schema, table, qualified) a partir del 'dbtable'.
    Si 'dbtable' no tiene schema, usa conn['schema'].
    """
    raw = (dbtable or "").strip()
    if not raw:
        raise ValueError("Redshift: 'dbtable' es obligatorio.")

    if "." in raw:
        schema, table = raw.split(".", 1)
        schema = schema.strip()
        table = table.strip()
    else:
        schema = (conn.get("schema") or "").strip()
        table = raw
        if not schema:
            raise ValueError(
                "Redshift: falta el schema (en dbtable o en la conexión 'schema')."
            )

    qualified = f"{schema}.{table}"
    return schema, table, qualified


# -------------------------------
# Metadatos de destino
# -------------------------------
def list_tables_in_schema(
    spark, url: str, jprops: Dict[str, str], schema: str
) -> List[str]:
    """
    Lista nombres de tablas del schema (solo tablas base y vistas).
    """
    sql = (
        "SELECT table_name "
        "FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' "
        "AND table_type IN ('BASE TABLE','VIEW') "
        "ORDER BY table_name"
    )
    rows = _exec_query(spark, url, jprops, sql)
    return [r[0] for r in rows]


def get_table_columns_order(
    spark, url: str, jprops: Dict[str, str], schema: str, table: str
) -> List[Tuple[str, str]]:
    """
    Retorna columnas de la tabla Redshift en orden (col_name, data_type_str).
    Usa information_schema.columns.
    """
    sql = (
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    )
    rows = _exec_query(spark, url, jprops, sql)
    out: List[Tuple[str, str]] = []
    for col_name, data_type, charlen, nump, nums in rows:
        dtype = (data_type or "").lower()
        # Normaliza DECIMAL/NUMERIC con precisión/escala
        if dtype in ("numeric", "decimal"):
            try:
                p = int(nump) if nump else None
                s = int(nums) if nums else 0
                if p:
                    dtype = f"decimal({p},{s})"
            except Exception:  # pylint: disable=broad-except
                dtype = "decimal"
        elif dtype in ("character varying", "varchar", "character", "char"):
            try:
                l = int(charlen) if charlen else None
                if l:
                    base = "varchar" if "vary" in dtype else "char"
                    dtype = f"{base}({l})"
            except Exception:  # pylint: disable=broad-except
                dtype = "varchar"
        out.append((col_name, dtype))
    return out


# -------------------------------
# Alineación / Limpieza / Casteo
# -------------------------------
_STRING_DEST = {"character varying", "varchar", "character", "char", "text"}


def _is_string_dest(dtype: str) -> bool:
    d = (dtype or "").lower()
    if d.startswith("varchar(") or d.startswith("char("):
        return True
    return d in _STRING_DEST


_NUMERIC_RE = re.compile(r"^(?:decimal|numeric)\((\d+),(\d+)\)$", re.IGNORECASE)


def _parse_redshift_dtype_to_spark(dtype: str) -> T.DataType:
    """
    Mapea tipo Redshift (string) a tipo Spark.
    """
    d = (dtype or "").lower().strip()
    if d in ("smallint",):
        return T.ShortType()
    if d in ("integer", "int4", "int"):
        return T.IntegerType()
    if d in ("bigint", "int8"):
        return T.LongType()
    if d in ("real", "float4"):
        return T.FloatType()
    if d in ("double precision", "float8"):
        return T.DoubleType()
    if d.startswith("decimal") or d.startswith("numeric"):
        m = _NUMERIC_RE.match(d)
        if m:
            p = int(m.group(1))
            s = int(m.group(2))
            return T.DecimalType(precision=p, scale=s)
        # fallback
        return T.DecimalType(38, 10)
    if d in ("boolean", "bool"):
        return T.BooleanType()
    if d in ("date",):
        return T.DateType()
    if "timestamp" in d:
        return T.TimestampType()
    # varchar/char/text/etc.
    return T.StringType()


def align_df_by_position(df: DataFrame, target_cols_in_order: List[str]) -> DataFrame:
    """
    Reordena/renombra por posición:
    - recorta o rellena con NULL si DF tiene menos/más columnas
    - renombra las columnas del DF a los nombres del destino (por posición)
    """
    curr_cols = df.columns
    tgt = list(target_cols_in_order)

    # Recortar o rellenar
    if len(curr_cols) < len(tgt):
        # agregar NULLs al final
        add_count = len(tgt) - len(curr_cols)
        for i in range(add_count):
            df = df.withColumn(f"_pad_null_{i}", F.lit(None))
        curr_cols = df.columns
    elif len(curr_cols) > len(tgt):
        # recortar
        curr_cols = curr_cols[: len(tgt)]
        df = df.select(*curr_cols)

    # Renombrar por posición
    for src, dest in zip(df.columns, tgt):
        if src != dest:
            df = df.withColumnRenamed(src, dest)

    # Reordenar exactamente como destino
    df = df.select(*tgt)
    return df


def sanitize_by_dest_types_for_null_and_blanks(
    df: DataFrame,
    dest_types: Dict[str, str],
    null_to_empty: bool,
) -> DataFrame:
    """
    - Para destino STRING y null_to_empty=True => NULL -> '' (solo STRING destino)
    - Para destino NO-STRING => '' -> NULL
    """
    out = df
    for col, dtype in dest_types.items():
        if col not in out.columns:
            continue
        if _is_string_dest(dtype):
            if null_to_empty:
                out = out.withColumn(col, F.when(F.col(col).isNull(), F.lit("")).otherwise(F.col(col)))
        else:
            # Forzar '' -> NULL antes de castear a numérico/fecha/etc.
            out = out.withColumn(
                col,
                F.when(F.trim(F.col(col)) == F.lit(""), F.lit(None)).otherwise(F.col(col)),
            )
    return out


def cast_df_to_target_types(df: DataFrame, dest_types: Dict[str, str]) -> DataFrame:
    """Castea las columnas presentes en el DF al tipo Spark equivalente del destino."""
    out = df
    for col, dtype in dest_types.items():
        if col not in out.columns:
            continue
        spark_t = _parse_redshift_dtype_to_spark(dtype)
        out = out.withColumn(col, F.col(col).cast(spark_t))
    return out


# -------------------------------
# Clase principal
# -------------------------------
class LoadRedshift:
    """Loader Redshift con soporte de overwrite, load_position y null_to_empty."""

    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    def run(self, df: DataFrame, props: Dict[str, Any], lcfg: Dict[str, Any]) -> None:
        """
        props: diccionario del bloque 'load' (o equivalente)
        lcfg:  flags auxiliares (p.e., load_position, null_to_empty) si vienen separados
        """
        namef = get_name_function()

        # ---------------------------
        # Conexión
        # ---------------------------
        load_props = props.get("load_properties") or props
        conn_name = (load_props.get("connection_name") or "redshift-cnx-01").strip()
        try:
            conn = self.config._global.get_properties(conn_name)  # pylint: disable=protected-access
        except Exception as exc:  # pylint: disable=broad-except
            raise KeyError(f"No se encontró configuración global con name='{conn_name}'") from exc

        server = (conn.get("server") or "").strip()
        port = (conn.get("port") or "5439").strip()
        database = (conn.get("database") or "").strip()
        if not server or not database:
            raise ValueError("Redshift: 'server' y 'database' son obligatorios en la conexión.")

        url = f"jdbc:redshift://{server}:{port}/{database}"
        jprops = _jdbc_props(conn)

        # ---------------------------
        # Tabla destino
        # ---------------------------
        dbtable = (load_props.get("dbtable") or props.get("dbtable") or "").strip()
        schema, table, qualified = _infer_schema_table(conn, dbtable)

        # currentSchema asegura que Spark/driver consulte existencia en el schema correcto
        jprops_write = dict(jprops)
        jprops_write["currentSchema"] = schema

        # ---------------------------
        # Flags de carga
        # ---------------------------
        mode = (load_props.get("mode") or props.get("mode") or "append").lower()
        lp_flag = (props.get("load_position") or lcfg.get("load_position") or "N").upper()
        nte_flag = (props.get("null_to_empty") or lcfg.get("null_to_empty") or "N").upper()

        # ---------------------------
        # Logs de esquema y columnas
        # ---------------------------
        try:
            tables = list_tables_in_schema(self.spark, url, jprops_write, schema)
            self.logger.registrar(
                "INFO",
                f"[Redshift] Tablas en schema '{schema}': " + ", ".join(tables) if tables else "(sin tablas)"
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.registrar("INFO", f"[Redshift] No se pudo listar tablas: {exc}")

        dest_cols: List[Tuple[str, str]] = get_table_columns_order(
            self.spark, url, jprops_write, schema, table
        )
        if not dest_cols:
            raise ValueError(f"Redshift: no se encontraron columnas para {qualified}")

        # Diccionario col->dtype y lista ordenada
        dest_types = {c: t for c, t in dest_cols}
        dest_order = [c for c, _ in dest_cols]
        self.logger.registrar(
            "INFO",
            "[Redshift] Columnas en "
            f"{qualified}: "
            + ", ".join(
                f"{i+1}.{c}({t})" for i, (c, t) in enumerate(dest_cols)
            ),
        )

        # ---------------------------
        # Transformaciones previas a insert
        # ---------------------------
        df_out = df

        # Alineación por posición
        if lp_flag == "Y":
            df_out = align_df_by_position(df_out, dest_order)
            self.logger.registrar(
                "INFO",
                "[load.redshift.load_redshift.run] - Redshift load_position=Y -> alineación por posición aplicada."
            )
        else:
            # Si no se alinea por posición, al menos reordenar por nombre si es posible
            common = [c for c in dest_order if c in df_out.columns]
            if common:
                df_out = df_out.select(*common)

        # null_to_empty
        df_out = sanitize_by_dest_types_for_null_and_blanks(
            df_out, dest_types, null_to_empty=(nte_flag == "Y")
        )

        # casteos al tipo destino
        df_out = cast_df_to_target_types(df_out, dest_types)

        # ---------------------------
        # Overwrite (DELETE FROM)
        # ---------------------------
        if mode == "overwrite":
            self.logger.registrar(
                "INFO",
                f"[load.redshift.load_redshift.run] - Redshift overwrite => DELETE FROM {qualified}"
            )
            _exec_ddl(self.spark, url, jprops_write, f"DELETE FROM {qualified}")
            write_mode = "append"
        else:
            write_mode = "append"

        # ---------------------------
        # Escribir (append)
        # ---------------------------
        self.logger.registrar(
            "INFO",
            f"[{namef}] - Redshift dest={qualified} mode={write_mode} cols={list(df_out.columns)}"
        )

        # IMPORTANTE: usar nombre calificado y currentSchema
        df_out.write.jdbc(
            url=url,
            table=qualified,
            mode=write_mode,
            properties=jprops_write,
        )

        self.logger.registrar("INFO", f"[{namef}] - Carga Redshift completada -> {qualified}")
