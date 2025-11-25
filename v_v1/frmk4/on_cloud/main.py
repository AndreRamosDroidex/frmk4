# on_cloud/main.py
import sys
from pyspark.sql import SparkSession

from utils.logger.helper_logger import LoggerManager
from utils.config.helper_config import ConfigManager
from extract.extractor import Extractor
from extract.extractor_jdbc import ExtractorJDBC
from transform.transformer import Transformer
from load.loader import Loader


def build_spark(config) -> SparkSession:
    """
    Construye la SparkSession usando las propiedades del bloque
    config_app:name='spark-config' del config-global y/o process.
    """
    spark_cfg = config.get_merged_properties_config_app("spark-config")
    builder = SparkSession.builder.appName("FRMK4-OnCloud")
    for k, v in spark_cfg.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    # Recomendación: silenciar verbosidad de logs de Spark si molesta
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    # argv esperado desde el .sh:
    # script, ID_TRACKING, BATCH_ID, FECINFORMACION
    if len(sys.argv) >= 4:
        _, id_tracking, batch_id, fecinfo = sys.argv
    else:
        # fallback si ejecutas a mano
        id_tracking = "manual"
        batch_id = sys.argv[1] if len(sys.argv) > 1 else "4001"
        fecinfo   = sys.argv[2] if len(sys.argv) > 2 else "202510"

    datacenter = "AWS"

    # Logger + Config
    log = LoggerManager(id_tracking, batch_id, fecinfo, datacenter)
    config = ConfigManager(id_tracking, batch_id, fecinfo, datacenter, log=log).getConfig()

    spark = build_spark(config)
    log.registrar("INFO", f"[main] - Spark OK. args(id_tracking={id_tracking}, batch_id={batch_id}, fecinfo={fecinfo})")

    try:
        # -------------------- Extract --------------------
        extract_cfgs = config._process.get_module("extract")
        if extract_cfgs:
            # Si el process define extract -> JDBC
            df_base = ExtractorJDBC(spark, config).run()
        else:
            # Fallback: parquet local (on-prem procesado)
            df_base = Extractor(spark, config).run()
        log.registrar("INFO", f"[main] - Extract OK. rows={df_base.count()}")

        # ------------------- Transform -------------------
        dfs_by_id, df_last = Transformer(spark, config).run(df_base)
        log.registrar("INFO", "[main] - Transform OK.")

        # --------------------- Load ----------------------
        # Multi-transform: si un load no tiene transform_id, usa df_fallback (df_base)
        Loader(spark, config).run_many(dfs_by_id, df_fallback=df_base)
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
