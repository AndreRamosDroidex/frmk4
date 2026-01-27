# on_cloud/load/athena/load_athena.py
from __future__ import annotations

import os
import time
from urllib.parse import urlparse
from typing import List, Dict, Optional

import boto3
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from utils.others.helper_functions import get_name_function
from load.s3.load_s3 import LoadS3, _apply_data_protect  # reutilizamos protección


def _get_region():
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"


def _parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"URI no es s3:// : {uri}")
    p = urlparse(uri)
    bucket = p.netloc
    prefix = p.path.lstrip("/")
    return bucket, prefix


def _get_glue_table(glue, schema: str, table: str) -> Dict:
    return glue.get_table(DatabaseName=schema, Name=table)["Table"]


def _get_table_location(tbl: Dict) -> str:
    loc = tbl["StorageDescriptor"]["Location"]
    if not loc.startswith("s3://"):
        raise ValueError(f"Location de la tabla no es s3:// : {loc}")
    return loc.rstrip("/")


def _get_partition_cols(tbl: Dict) -> List[str]:
    pks = tbl.get("PartitionKeys") or []
    return [(pk.get("Name") or "").strip() for pk in pks if (pk.get("Name") or "").strip()]


def _ensure_partitions_exact_case(df: DataFrame, part_cols_glue: List[str]):
    if not part_cols_glue:
        return df
    df_cols_lower = {c.lower(): c for c in df.columns}
    out = df
    for pc in part_cols_glue:
        pc_low = pc.lower()
        if pc_low in df_cols_lower and df_cols_lower[pc_low] != pc:
            out = out.withColumnRenamed(df_cols_lower[pc_low], pc)
    return out


class LoadAthena:
    """
    Escribe el DF en el 'Location' S3 de una tabla Glue/Athena y luego ejecuta MSCK REPAIR TABLE.
    """

    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def _athena_msck_and_wait(self, schema: str, table: str, out_loc: str,
                              wait_seconds: int = 2, max_attempts: int = 60, wait: bool = True):
        import botocore
        region = _get_region()
        athena = boto3.client("athena", region_name=region)
        q = f"MSCK REPAIR TABLE {schema}.{table}"
        self.logger.registrar("INFO", f"[{get_name_function()}] - Ejecutando Athena: {q}")
        resp = athena.start_query_execution(
            QueryString=q,
            QueryExecutionContext={"Database": schema},
            ResultConfiguration={"OutputLocation": out_loc}
        )
        qid = resp.get("QueryExecutionId")
        if not wait or not qid:
            return
        try:
            for _ in range(max_attempts):
                st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
                if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    self.logger.registrar("INFO", f"[{get_name_function()}] - MSCK state={st}")
                    break
                time.sleep(wait_seconds)
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "AccessDeniedException":
                self.logger.registrar(
                    "WARNING",
                    f"[{get_name_function()}] - Sin permiso athena:GetQueryExecution; "
                    "se continúa sin esperar el MSCK."
                )
            else:
                raise

    def run(self, df: DataFrame, props: dict, lcfg: Optional[dict] = None):
        namef = get_name_function()
        schema = (props.get("schema") or "").strip()
        table = (props.get("table") or "").strip()
        if not schema or not table:
            raise ValueError("LoadAthena: 'schema' y 'table' son obligatorios.")

        mode = (props.get("mode") or "append").lower().strip()
        error_continue = (props.get("error_continue") or "n").lower().strip() in ("y", "yes", "true", "1")

        # Protección de datos si aplica
        try:
            df = _apply_data_protect(df, props.get("data_protect_columns"), self.logger, namef)
        except Exception as e:
            if error_continue:
                self.logger.registrar("WARNING", f"[{namef}] - data_protect falló: {e}. Continuando por error_continue.")
            else:
                raise

        # 1) Metadatos Glue
        try:
            region = _get_region()
            glue = boto3.client("glue", region_name=region)
            tbl = _get_glue_table(glue, schema, table)
            s3_location = _get_table_location(tbl)
            part_cols_glue = _get_partition_cols(tbl)
            self.logger.registrar("INFO", f"[{namef}] - Glue: location={s3_location} partitions={part_cols_glue if part_cols_glue else '[]'}")
        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - No se pudo obtener metadatos Glue: {e}. Continuando por error_continue.")
                return
            else:
                raise

        # 2) Garantizar particiones
        try:
            fecinfo_val = getattr(self.config, "fecinformacion", None) or getattr(self.config, "fecinfo", None)
            df_cols_lower = {c.lower(): c for c in df.columns}
            part_cols_lower = [pc.lower() for pc in (part_cols_glue or [])]

            if "fecinformacion" in part_cols_lower:
                if "fecinformacion" not in df_cols_lower and fecinfo_val is not None:
                    df = df.withColumn("fecinformacion", lit(str(fecinfo_val)))
                    self.logger.registrar("INFO", f"[{namef}] - Columna partición 'fecinformacion' añadida con valor {fecinfo_val}")

            df = _ensure_partitions_exact_case(df, part_cols_glue)

            missing_others = [pc for pc in (part_cols_glue or []) if pc not in df.columns]
            if missing_others:
                self.logger.registrar("WARNING", f"[{namef}] - Columnas de partición faltantes en DF (se ignorarán): {missing_others}")
        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - Error preparando columnas de partición: {e}. Continuando por error_continue.")
            else:
                raise

        # 3) Escribir DF directo al Location
        s3_props = {
            "bucket": s3_location,
            "name": None,  # directo al Location
            "format": (props.get("format") or "parquet"),
            "mode": mode,
            "error_continue": "y" if error_continue else "n",
            "data_protect_columns": props.get("data_protect_columns")
        }
        if part_cols_glue:
            s3_props["partition_by"] = ",".join(part_cols_glue)

        try:
            LoadS3(self.spark, self.config, self.logger).run(df, s3_props, lcfg or {})
        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - Error escribiendo en {s3_location}: {e}. Continuando por error_continue.")
            else:
                raise

        # 4) MSCK REPAIR TABLE
        try:
            if props.get("athena_output"):
                out_loc = props["athena_output"]
            else:
                out_loc = os.getenv("ATHENA_OUTPUT") or os.getenv("ATHENA_RESULTS_S3")
                if not out_loc:
                    wg = os.getenv("ATHENA_WORKGROUP") or "primary"
                    try:
                        region = _get_region()
                        ath = boto3.client("athena", region_name=region)
                        wg_resp = ath.get_work_group(WorkGroup=wg)
                        out_loc = (wg_resp.get("WorkGroup", {})
                                   .get("Configuration", {})
                                   .get("ResultConfiguration", {})
                                   .get("OutputLocation"))
                    except Exception:
                        out_loc = None
                if not out_loc:
                    bkt, _ = _parse_s3_uri(s3_location)
                    out_loc = f"s3://{bkt}/athena-query-results/"

            self._athena_msck_and_wait(schema, table, out_loc)
            self.logger.registrar("INFO", f"[{namef}] - MSCK REPAIR TABLE finalizado (output: {out_loc})")
        except Exception as e:
            if error_continue:
                self.logger.registrar("ERROR", f"[{namef}] - Error ejecutando MSCK REPAIR: {e}. Continuando por error_continue.")
            else:
                raise
