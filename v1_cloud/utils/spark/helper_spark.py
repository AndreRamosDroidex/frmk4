# utils/spark/helper_spark.py
from __future__ import annotations

from typing import Dict
from pyspark.sql import SparkSession

from utils.jdbc.driver_loader import ensure_driver_loaded, _find_teradata_jars

def _apply_props(builder, props: Dict[str, str]):
    for k, v in (props or {}).items():
        builder = builder.config(k, v)
    return builder

def get_spark_session(config, logger=None):
    builder = SparkSession.builder.appName("FRMK4")

    # spark-config del JSON global
    spark_cfg = {}
    try:
        spark_cfg = config.get_config("spark-config")
    except Exception:
        spark_cfg = {}
    builder = _apply_props(builder, spark_cfg)

    # Agregar JARs Teradata si están locales (./jars o rutas comunes)
    tera_jars = _find_teradata_jars(logger)
    if tera_jars:
        jars_csv = ",".join(tera_jars)
        classpath = jars_csv.replace(",", ":")
        builder = builder.config("spark.jars", jars_csv) \
                         .config("spark.driver.extraClassPath", classpath) \
                         .config("spark.executor.extraClassPath", classpath)

    spark = builder.getOrCreate()

    # Registrar driver en el driver JVM
    if tera_jars:
        try:
            ensure_driver_loaded(spark, "com.teradata.jdbc.TeraDriver", logger, caller="helper_spark")
        except Exception:
            pass

    return spark
