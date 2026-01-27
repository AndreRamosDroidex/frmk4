# on_cloud/main.py
import sys
import os
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from utils.logger.helper_logger import LoggerManager
from utils.config.helper_config import ConfigManager
from extract.extractor import Extractor
from transform.transformer import Transformer
from load.loader import Loader


def build_spark(config) -> SparkSession:
    """
    Crea la SparkSession y agrega automáticamente los JARs locales de Teradata.
    Busca *.jar en on_cloud/jars/ (al lado de main.py) o en TERADATA_JARS_LOCAL.
    """
    # Config base desde tu "spark-config"
    try:
        spark_cfg = config.get_merged_properties_config_app("spark-config") or {}
    except Exception:
        spark_cfg = {}

    # Descubrir JARs locales
    base_dir = Path(__file__).resolve().parent
    jars: list[str] = []

    if not jars:
        jars_dir = base_dir / "jars"
        if jars_dir.is_dir():
            for j in jars_dir.glob("*.jar"):
                jars.append(str(j.resolve()))

    # Construcción de la sesión
    builder = SparkSession.builder.appName("FRMK4-OnCloud")

    # Aplicar config de spark-config
    for k, v in spark_cfg.items():
        builder = builder.config(k, v)

    # Inyectar JARs si hay
    if jars:
        jars_csv = ",".join(jars)
        builder = (
            builder
            .config("spark.jars", jars_csv)
            .config("spark.driver.extraClassPath", jars_csv)    # refuerzo por si tu runtime lo requiere
            .config("spark.executor.extraClassPath", jars_csv)  # refuerzo por si tu runtime lo requiere
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def _collect_referenced_transform_ids(config) -> set:
    referenced = set()
    loads = config._process.get_module("load") or []
    for ld in loads:
        t_id = ld.get("transform_id")
        if t_id is not None:
            referenced.add(str(t_id))
    return referenced


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id_tracking')
    parser.add_argument('--id_process')
    parser.add_argument('--fecinformacion')
    args = parser.parse_args()

    id_tracking = args.id_tracking
    batch_id = args.id_process
    fecinfo = args.fecinformacion

    # Tablas Dynamo por defecto (ajusta si aplica)
    os.environ.setdefault("DDB_PROCESS_TABLE", "aw-dev-db-frmk4-config-process-demo")
    os.environ.setdefault("DDB_GLOBAL_TABLE",  "aw-dev-db-frmk4-config-global-demo")
    os.environ.setdefault("DDB_GLOBAL_PK", "state_machine")
    os.environ.setdefault("DDB_GLOBAL_PK_VALUE", "aw-dev-sfn-frmk4-process-onpremise-demo")

    datacenter = "AWS"

    log = LoggerManager(id_tracking, batch_id, fecinfo, datacenter)
    cm = ConfigManager(id_tracking, batch_id, fecinfo, datacenter, log=log)
    config = cm.getConfig()

    # Atributos estándar
    setattr(config, "batch_id", batch_id)
    setattr(config, "fecinfo",  fecinfo)
    setattr(config, "fecinformacion", fecinfo)

    spark = build_spark(config)
    log.registrar("INFO", f"[main] - Spark OK. args(id_tracking={id_tracking}, batch_id={batch_id}, fecinfo={fecinfo})")

    try:
        # Extract
        df_base = Extractor(spark, config).run()
        log.registrar("INFO", f"[main] - Extract OK. rows={df_base.count()}")

        # Transform
        referenced_ids = _collect_referenced_transform_ids(config)
        dfs_by_id, df_last = Transformer(spark, config).run(df_base, only_ids=referenced_ids)
        log.registrar("INFO", "[main] - Transform OK." if referenced_ids else "[main] - Transform SKIPPED.")

        # Load
        Loader(spark, config, log).run_many(dfs_by_id, df_fallback=df_base)
        log.registrar("INFO", "[main] - Load OK.")
        log.registrar("INFO", "[main] - Proceso finalizado correctamente.")
    except Exception as e:
        log.registrar("ERROR", f"[main] - Proceso falló: {e}")
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass
