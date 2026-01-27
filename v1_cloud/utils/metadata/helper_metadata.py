# on_cloud/utils/metadata/helper_metadata.py
from __future__ import annotations

import json
import os
import re
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, functions as F, types as T

# ---------------------------------------------------------------------------
# Helpers de paths / Hadoop FS (para soportar S3 y filesystem local)
# ---------------------------------------------------------------------------
def is_s3_like(path: str) -> bool:
    p = (path or "").strip().lower()
    return p.startswith("s3://") or p.startswith("s3a://")

def _hadoop_fs(spark, path: str):
    """
    Devuelve (fs, jpath) asegurando que el FileSystem se resuelva
    por el esquema del path (s3, s3a, file, etc.) para evitar:
    'Wrong FS: s3://..., expected: file:///'
    """
    jvm = spark._sc._jvm
    conf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    jpath = Path(path)
    uri = jpath.toUri()                 # <- usar el URI del path
    fs = FileSystem.get(uri, conf)      # <- en vez de FileSystem.get(conf)

    return fs, jpath

def _exists(spark, path: str) -> bool:
    if is_s3_like(path):
        fs, jpath = _hadoop_fs(spark, path)
        return fs.exists(jpath)
    return os.path.exists(path)

def _list_dir(spark, path: str) -> List[str]:
    if is_s3_like(path):
        fs, jpath = _hadoop_fs(spark, path)
        status = fs.listStatus(jpath)
        return [str(st.getPath().toString()) for st in status]
    else:
        return [os.path.join(path, p) for p in os.listdir(path)]

def _read_text_file(spark, path: str) -> str:
    if is_s3_like(path):
        df = spark.read.text(path)
        return "\n".join([r[0] for r in df.collect()])
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# ---------------- metadata.json & data files ----------------

def read_metadata_json(spark, folder: str) -> Dict:
    meta_path = folder.rstrip("/").rstrip("\\") + "/metadata.json"
    if not _exists(spark, meta_path):
        raise FileNotFoundError(f"metadata.json no encontrado en: {meta_path}")
    txt = _read_text_file(spark, meta_path)
    return json.loads(txt)

def list_data_files(
    spark,
    data_dir: str,
    exts: Sequence[str] = (".csv.gz", ".gz", ".parquet", ".csv", ".txt", ".dat", ".txt.gz", ".dat.gz"),
) -> List[str]:
    if not _exists(spark, data_dir):
        raise FileNotFoundError(f"Carpeta data no existe: {data_dir}")
    children = _list_dir(spark, data_dir)
    exts_low = tuple(e.lower() for e in exts)
    out = []
    for p in children:
        lp = p.lower()
        # si es carpeta parquet, dejamos el dir completo
        if lp.endswith("/"):
            continue
        if any(lp.endswith(e) for e in exts_low):
            out.append(p)
    # Si no hay matches y es parquet, Spark puede leer el directorio
    if not out and data_dir.lower().endswith("/data"):
        out = [data_dir]
    return out

# ---------------- Spark schema from metadata ----------------

_SPARK_TYPE_MAP = {
    "STRING": T.StringType(),
    "INT": T.IntegerType(),
    "INTEGER": T.IntegerType(),
    "BIGINT": T.LongType(),
    "LONG": T.LongType(),
    "DOUBLE": T.DoubleType(),
}

def _parse_decimal(dec_str: str) -> Optional[Tuple[int, int]]:
    s = (dec_str or "").strip().upper()
    if not s.startswith(("DECIMAL", "NUMERIC")):
        return None
    try:
        inside = s[s.index("(")+1:s.index(")")]
        p, sc = inside.split(",")
        return int(p.strip()), int(sc.strip())
    except Exception:
        return None

def _to_spark_type(db_t: str) -> T.DataType:
    s = (db_t or "").upper().strip()
    dec = _parse_decimal(s)
    if dec:
        return T.DecimalType(dec[0], dec[1])
    if "BIGINT" in s:
        return T.LongType()
    if "SMALLINT" in s:
        return T.ShortType()
    if re.search(r"\bINT\b", s):
        return T.IntegerType()
    if any(x in s for x in ["DOUBLE", "FLOAT", "REAL"]):
        return T.DoubleType()
    if "DATE" in s:
        return T.DateType()
    if "TIMESTAMP" in s or re.fullmatch(r"TIME(ZONE)?", s):
        return T.TimestampType()
    return T.StringType()

def build_spark_schema_from_metadata(md: Dict) -> Optional[T.StructType]:
    meta = md.get("metadata") or {}
    cols = meta.get("columns")
    if not cols:
        return None
    # ordenar por order si existe
    sortable = []
    for c in cols:
        if isinstance(c, dict):
            try:
                order = int(str(c.get("order") or "0"))
            except Exception:
                order = 0
            sortable.append((order, c))
        else:
            sortable.append((999999, {"name": str(c), "data_type": "STRING"}))
    cols_sorted = [c for _, c in sorted(sortable, key=lambda x: x[0])]

    fields: List[T.StructField] = []
    for c in cols_sorted:
        name = (c.get("name") or "").strip()
        dt = (c.get("data_type") or "STRING").strip().upper()
        if not name:
            continue
        dec = _parse_decimal(dt)
        if dec:
            fields.append(T.StructField(name, T.DecimalType(dec[0], dec[1]), True))
        else:
            fields.append(T.StructField(name, _to_spark_type(dt), True))
    return T.StructType(fields)

# ---------------- JDBC metadata helpers (si los usas en load) ----------------

def align_df_by_position(df: DataFrame, target_cols: List[str]) -> DataFrame:
    src_cols = df.columns
    n = len(src_cols); m = len(target_cols)
    tmp = df
    for i in range(min(n, m)):
        tmp = tmp.withColumnRenamed(src_cols[i], f"__tmp_{i}__")
    for i in range(min(n, m)):
        tmp = tmp.withColumnRenamed(f"__tmp_{i}__", target_cols[i])
    select_exprs = [F.col(c) for c in target_cols[:min(n, m)]]
    for i in range(min(n, m), m):
        select_exprs.append(F.lit(None).alias(target_cols[i]))
    return tmp.select(*select_exprs)

# ---------------------------------------------------------------------------
# Limpieza/casteo según tipos destino (para loaders JDBC)
# ---------------------------------------------------------------------------

def sanitize_by_dest_types_for_null_and_blanks(
    df: DataFrame,
    dest_types: Dict[str, str],
    null_to_empty: bool
) -> DataFrame:
    """
    - Numéricos/fecha destino: '' -> NULL siempre (evita fallos de cast).
    - STRING destino + null_to_empty=True: NULL -> ''.
    """
    out = df
    for col in out.columns:
        t = dest_types.get(col)
        if not t:
            continue
        tt = t.upper()
        is_string  = any(x in tt for x in ["CHAR", "CLOB", "TEXT", "STRING", "VARCHAR"])
        is_numeric = any(x in tt for x in ["INT", "DEC", "NUM", "DOUBLE", "FLOAT", "REAL", "BIGINT", "SMALLINT"])
        is_date    = ("DATE" in tt) or ("TIMESTAMP" in tt) or ("TIME" in tt)

        if is_numeric or is_date:
            out = out.withColumn(
                col, F.when(F.trim(F.col(col)) == "", F.lit(None)).otherwise(F.col(col))
            )
        elif is_string and null_to_empty:
            out = out.withColumn(
                col, F.when(F.col(col).isNull(), F.lit("")).otherwise(F.col(col))
            )
    return out

def cast_df_to_target_types(df: DataFrame, dest_types: Dict[str, str]) -> DataFrame:
    out = df
    for col in out.columns:
        t = dest_types.get(col)
        if not t:
            continue
        spark_t = _to_spark_type(t)
        out = out.withColumn(col, F.col(col).cast(spark_t))
    return out

# ---------------------------------------------------------------------------
# Metadata JDBC de tabla (orden y tipos)
# ---------------------------------------------------------------------------

def _split_schema_table(fullname: str) -> Tuple[Optional[str], str]:
    if "." in fullname:
        parts = fullname.split(".")
        schema = parts[0]
        tname = ".".join(parts[1:]) if len(parts) > 2 else parts[1]
        return schema, tname
    return None, fullname

def _get_columns_and_types(spark, url: str, jprops: Dict[str, str], table: str):
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager

    jprops_java = jvm.java.util.Properties()
    for k, v in (jprops or {}).items():
        jprops_java.setProperty(k, v)

    conn = None
    rs = None
    try:
        conn = DriverManager.getConnection(url, jprops_java)
        meta = conn.getMetaData()
        schema, tname = _split_schema_table(table)
        rs = meta.getColumns(None, schema, tname, "%")

        cols = []
        types = {}
        while rs.next():
            name = rs.getString("COLUMN_NAME")
            type_name = rs.getString("TYPE_NAME")
            size = 0
            scale = 0
            try:
                size = rs.getInt("COLUMN_SIZE")
            except Exception:
                size = 0
            try:
                scale = rs.getInt("DECIMAL_DIGITS")
            except Exception:
                scale = 0
            ordinal = rs.getInt("ORDINAL_POSITION")

            up = (type_name or "").upper()
            if ("DEC" in up) or ("NUM" in up):
                tstr = f"DECIMAL({size},{scale})" if size > 0 else "DECIMAL"
            elif "CHAR" in up:
                tstr = f"{up}({size})" if size > 0 else up
            else:
                tstr = up

            types[name] = tstr
            cols.append((ordinal, name))

        cols.sort(key=lambda x: x[0])
        return cols, types
    finally:
        try:
            if rs is not None:
                rs.close()
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

def get_table_columns_order(spark, url: str, jprops: Dict[str, str], table: str) -> List[str]:
    cols, _ = _get_columns_and_types(spark, url, jprops, table)
    return [c for (_, c) in cols]

def get_table_columns_types(spark, url: str, jprops: Dict[str, str], table: str) -> Dict[str, str]:
    _, types = _get_columns_and_types(spark, url, jprops, table)
    return types
