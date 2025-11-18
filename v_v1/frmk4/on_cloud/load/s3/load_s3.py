from typing import Optional, List
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct
from utils.others.helper_functions import get_name_function

_VALID_CODECS = {"gzip", "bzip2", "lz4", "snappy", "zstd", "deflate", "uncompressed"}

def _normalize_path(bucket: str, name: Optional[str]) -> str:
    # bucket viene tipo s3://mi-bucket/prefix/opcional
    bucket = bucket.rstrip("/")
    if name:
        return f"{bucket}/{name}"
    return bucket

def _normalize_partitions(partitions) -> List[str]:
    if not partitions:
        return []
    if isinstance(partitions, str):
        return [p.strip() for p in partitions.split(",") if p.strip()]
    if isinstance(partitions, list):
        return [str(p).strip() for p in partitions if str(p).strip()]
    return []

class LoadS3:
    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def run(self, df: DataFrame, props: dict):
        name_function = get_name_function()

        bucket = props.get("bucket")
        if not bucket:
            raise ValueError("LoadS3: 'bucket' es obligatorio.")

        name = props.get("name")  # opcional
        fmt = (props.get("format") or "parquet").lower()
        mode = (props.get("mode") or "overwrite").lower()
        compression = (props.get("compression") or "").lower().strip()
        partitions = _normalize_partitions(props.get("partitions"))

        dest = _normalize_path(bucket, name)
        self.logger.registrar("INFO", f"[{name_function}] - S3 destino: {dest}")
        self.logger.registrar("INFO", f"[{name_function}] - format={fmt}, mode={mode}, partitions={partitions}, compression={compression or '<default>'}")

        # Writer base
        writer = df.write.mode(mode)

        # Particiones si fueron solicitadas y existen en el DF
        if partitions:
            cols_presentes = [c for c in partitions if c in df.columns]
            faltantes = set(partitions) - set(cols_presentes)
            if faltantes:
                self.logger.registrar("WARNING", f"[{name_function}] - Particiones ignoradas por no existir en DF: {sorted(faltantes)}")
            if cols_presentes:
                writer = writer.partitionBy(*cols_presentes)

        # Compresión si viene y es válida (Spark entiende 'compression' para parquet/csv/json/text)
        if compression:
            if compression not in _VALID_CODECS:
                self.logger.registrar("WARNING", f"[{name_function}] - Codec '{compression}' no estándar; se intentará igual.")
            writer = writer.option("compression", compression)

        # Formato y preprocesos especiales
        if fmt == "parquet":
            writer.parquet(dest)
        elif fmt == "csv":
            # CSV típico: delimitador coma, header por defecto
            writer.option("header", "true").csv(dest)
        elif fmt == "json":
            writer.json(dest)
        elif fmt == "text":
            # Spark exige UNA columna string llamada 'value'
            df_out = df.select(to_json(struct(*df.columns)).alias("value"))
            df_out.write.mode(mode).option("compression", compression if compression else None).text(dest)
        else:
            # fallback genérico
            self.logger.registrar("WARNING", f"[{name_function}] - Formato '{fmt}' no reconocido. Se usará parquet.")
            writer.parquet(dest)

        self.logger.registrar("INFO", f"[{name_function}] - Escritura a S3 completada: {dest}")
