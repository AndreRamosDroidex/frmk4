# on_cloud/load/s3/load_s3.py
from __future__ import annotations

import re
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sha2
from utils.others.helper_functions import get_name_function


def _parse_column_list(spec: dict) -> List[str]:
    cols = (spec.get("columns") or "").strip()
    if not cols:
        return []
    return [c.strip() for c in cols.split(",") if c.strip()]


def _apply_data_protect(df: DataFrame, spec: dict, logger, namef: str) -> DataFrame:
    """
    Soporta tipo: 'volt-sha-256' => SHA-256 del valor string.
    """
    if not spec:
        return df

    t = (spec.get("type") or "").strip().lower()
    cols = _parse_column_list(spec)
    if not cols:
        return df

    if t in ("volt-sha-256", "sha-256", "sha256"):
        for c in cols:
            if c in df.columns:
                df = df.withColumn(c, sha2(col(c).cast("string"), 256))
            else:
                logger.registrar("WARN", f"[{namef}] - DataProtect: columna '{c}' no existe en DF (omitida).")
        logger.registrar("INFO", f"[{namef}] - DataProtect aplicado (sha-256) a columnas: {cols}")
        return df

    logger.registrar("WARN", f"[{namef}] - DataProtect: tipo '{t}' no soportado (omitido).")
    return df


def _expand_name_with_fecinfo(raw_name: str, fecinfo: str) -> str:
    """
    Reglas:
      - Si trae placeholder {fecinfo} => lo sustituye.
      - Si termina en '_abc' => lo reemplaza por '_<fecinfo>'.
      - Si nada de lo anterior => deja igual.
    """
    if not raw_name:
        return raw_name
    name = raw_name
    if "{fecinfo}" in name:
        name = name.replace("{fecinfo}", str(fecinfo))
    elif re.search(r"_abc$", name, flags=re.IGNORECASE):
        name = re.sub(r"_abc$", f"_{fecinfo}", name, flags=re.IGNORECASE)
    return name


class LoadS3:
    """
    Escribe un DF a S3 en parquet/csv (default parquet).

    Props soportadas en load_properties:
      - bucket (OBLIG.)  e.g. s3://.../pre-stage/analytics/d_datalake
      - name (OPCIONAL)  e.g. USER_COB_..._abc -> se expandirá a _<fecinfo>
                         Si omitido/None, se escribe directo en 'bucket' (Location completo).
      - format           parquet|csv (default parquet)
      - mode             overwrite|append (default overwrite)
      - partition_by     "col1,col2" (opcional)
      - auto_partition   "Y|N" (default N) -> agrega batch_id/fecinformacion + partitionBy
      - data_protect_columns: {"columns":"c1,c2","type":"volt-sha-256"}
      - error_continue   "y|n" (default n)
    """

    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def run(self, df: DataFrame, props: Dict, lcfg: Dict):
        namef = get_name_function()

        bucket = (props.get("bucket") or "").strip()
        if not bucket:
            raise ValueError("LoadS3: 'bucket' es obligatorio (s3://bucket/prefix).")

        # 'name' ahora es opcional: si falta/None, escribimos directo al 'bucket'
        raw_name = (props.get("name") or "").strip()
        fmt = (props.get("format") or "parquet").strip().lower()
        mode = (props.get("mode") or "overwrite").strip().lower()
        error_continue = (props.get("error_continue") or "n").strip().lower() in ("y", "yes", "true", "1")

        # Por default NO auto-partition (para mantener carpeta ..._<fecinfo> si la usas)
        auto_partition = (props.get("auto_partition") or "n").strip().lower() in ("y", "yes", "true", "1")

        # Protecciones
        df_out = df
        dp_spec = props.get("data_protect_columns") or {}
        if isinstance(dp_spec, dict) and dp_spec:
            df_out = _apply_data_protect(df_out, dp_spec, self.logger, namef)

        # Expandir name con fecinfo (si aplica) — y permitir None
        fecinfo = str(getattr(self.config, "fecinfo", getattr(self.config, "fecinformacion", "")))
        name_final = _expand_name_with_fecinfo(raw_name, fecinfo) if raw_name else None

        # Particiones
        pby_raw = (props.get("partition_by") or "").strip()
        partition_by = [p.strip() for p in pby_raw.split(",") if p.strip()] if pby_raw else []

        # Auto-partition opcional
        if auto_partition:
            if getattr(self.config, "batch_id", None) is not None and "batch_id" not in df_out.columns:
                df_out = df_out.withColumn("batch_id", lit(str(self.config.batch_id)))
                partition_by.append("batch_id")
            if getattr(self.config, "fecinfo", None) is not None and "fecinformacion" not in df_out.columns:
                df_out = df_out.withColumn("fecinformacion", lit(str(self.config.fecinfo)))
                partition_by.append("fecinformacion")

        # Si name_final es None, escribimos directo al 'bucket' (que ya es s3://bucket/prefix)
        out_dir = f"{bucket.rstrip('/')}/{name_final}" if name_final else bucket.rstrip('/')

        self.logger.registrar(
            "INFO",
            f"[{namef}] - S3 dest={out_dir} mode={mode} format={fmt} "
            f"partition_by={partition_by if partition_by else 'none'} cols={list(df_out.columns)}"
        )

        try:
            writer = df_out.write.mode(mode)
            if fmt == "csv":
                writer = writer.format("csv").option("header", "true")
            else:
                writer = writer.format("parquet")

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(out_dir)
            self.logger.registrar("INFO", f"[{namef}] - Carga S3 completada -> {out_dir}")

        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - S3 load falló pero error_continue=Y. Detalle: {e}")
                return
            raise
