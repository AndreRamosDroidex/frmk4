# on_cloud/main.py
import sys
from utils.config.helper_config import ConfigManager
from utils.logger.helper_logger import LoggerManager
from utils.spark.helper_spark import SparkManager
from extract.extractor import Extractor
from transform.transformer import Transformer
from load.loader import Loader

def main():
    if len(sys.argv) < 4:
        raise SystemExit("Uso: python main.py <ID_TRACKING> <BATCH_ID> <FECINFORMACION>")
    id_tracking, batch_id, fecinformacion = sys.argv[1], sys.argv[2], sys.argv[3]
    datacenter = "ec2"

    log = LoggerManager(id_tracking, batch_id, fecinformacion, datacenter)
    cfg_mgr = ConfigManager(id_tracking, batch_id, fecinformacion, datacenter, log)
    config = cfg_mgr.getConfig()

    spark = SparkManager(config).get_session()
    log.registrar("INFO", "[main] - Spark session inicializada.")

    try:
        # Extract
        df_base = Extractor(spark, config).run()
        log.registrar("INFO", f"[main] - Extract OK. rows={df_base.count()}")

        # Transform (PARALLEL + transform_type support)
        dfs_by_id, df_last = Transformer(spark, config).run(df_base)
        log.registrar("INFO", f"[main] - Transform OK. transforms={len(dfs_by_id)}")

        # Load (usar DF BASE como fallback para loads sin transform_id)
        Loader(spark, config).run_many(dfs_by_id, df_fallback=df_base)
        log.registrar("INFO", "[main] - Load OK.")

        log.registrar("INFO", "[main] - FRMK4 On-Cloud finalizado OK.")
    except Exception as e:
        log.registrar("ERROR", f"[main] - Proceso falló: {e}")
        raise
    finally:
        try:
            spark.stop()
            log.registrar("INFO", "[main] - Spark detenido.")
        except Exception:
            pass

if __name__ == "__main__":
    main()
