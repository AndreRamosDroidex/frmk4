# utils/jdbc/driver_loader.py
# -*- coding: utf-8 -*-
"""
Cargador tolerante de drivers JDBC (Teradata) para Spark/EMR.

Objetivos:
- Soportar llamadas con firmas variadas sin crashear:
    ensure_driver_loaded(spark, log)
    ensure_driver_loaded(spark, log, ["/path/tdgssconfig.jar","/path/terajdbc4.jar"])
    ensure_driver_loaded(spark, log, driver_class="com.teradata.jdbc.TeraDriver")
    ensure_driver_loaded(spark, [".../terajdbc4.jar",".../tdgssconfig.jar"])   # 2º arg = jars
    ensure_driver_loaded(spark, "com.teradata.jdbc.TeraDriver")                # 2º arg = driver

- Evitar la inyección runtime si los JARs ya vinieron con --jars (short-circuit).

- Usar spark._jsc.addJar(...) (no SparkContext.addJar) para compatibilidad con Spark 3/EMR.

- Descubrir JARs en ubicaciones comunes si no se proporcionan rutas.

Vars de entorno soportadas:
  - TERA_JDBC_JARS="path1.jar,path2.jar"
  - TERA_JDBC_JARS_DIRS="/home/hadoop:/usr/lib/spark/jars:/usr/lib/hadoop/lib"

Nota:
  La inyección runtime de JARs desde S3 no es fiable; preferir siempre --jars con URIs s3://.
"""

from typing import Iterable, List, Optional, Union
import os


__all__ = ["ensure_driver_loaded", "discover_teradata_jars"]


# ------------------------ utilidades de logging ------------------------ #

class _NoopLogger:
    def info(self, *a, **k):  # pragma: no cover
        pass

    def warning(self, *a, **k):  # pragma: no cover
        pass

    def error(self, *a, **k):  # pragma: no cover
        pass


def _as_list(x: Optional[Union[str, Iterable[str]]]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [x]
    try:
        return list(x)
    except TypeError:
        return [str(x)]


def _split_env_list(val: Optional[str]) -> List[str]:
    if not val:
        return []
    # Admite separadores comunes: coma, punto y coma, espacio y ':'
    raw = [p for chunk in val.replace(";", ",").replace(":", ",").split(",") for p in chunk.split()]
    return [p.strip() for p in raw if p.strip()]


def _existing(paths: Iterable[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        if p.lower().startswith("s3://"):
            # Para runtime addJar preferimos rutas locales; mantén las s3:// solo para --jars
            continue
        if os.path.exists(p):
            out.append(p)
    return out


# ------------------------ descubrimiento de JARs ------------------------ #

_DEFAULT_JAR_NAMES = (
    "terajdbc4.jar",
    "tdgssconfig.jar",
    # agregamos algunos alias comunes por si acaso
    "terajdbc.4.jar",
    "tdgssconfig-*.jar",
)


def _glob_candidates(dirpath: str, names: Iterable[str]) -> List[str]:
    try:
        import glob
        out: List[str] = []
        for n in names:
            out.extend(glob.glob(os.path.join(dirpath, n)))
        return out
    except Exception:
        return []


def discover_teradata_jars(
    search_dirs: Optional[List[str]] = None,
    extra_names: Optional[List[str]] = None,
) -> List[str]:
    """
    Busca JARs de Teradata en rutas conocidas y las provistas por el usuario.
    Devuelve una lista (posiblemente vacía) sin duplicados.
    """
    dirs: List[str] = []

    # 1) dirs desde env
    dirs.extend(_split_env_list(os.getenv("TERA_JDBC_JARS_DIRS")))

    # 2) dirs comunes en EMR/Spark
    dirs.extend([
        "/home/hadoop",
        "/usr/lib/spark/jars",
        "/usr/lib/hadoop/lib",
        "/usr/lib/hadoop/client",
        "/usr/lib/tez/lib",
        "/usr/share/aws/emr/emrfs/lib",
        "/tmp",
    ])

    # 3) dirs del caller
    if search_dirs:
        dirs.extend(search_dirs)

    # normaliza y dedup
    seen = set()
    norm_dirs = []
    for d in dirs:
        nd = os.path.abspath(d)
        if nd not in seen:
            seen.add(nd)
            norm_dirs.append(nd)

    names = list(_DEFAULT_JAR_NAMES)
    if extra_names:
        for n in extra_names:
            if n not in names:
                names.append(n)

    candidates: List[str] = []
    for d in norm_dirs:
        candidates.extend(_glob_candidates(d, names))

    # también admite rutas directas desde env TERA_JDBC_JARS
    candidates.extend(_split_env_list(os.getenv("TERA_JDBC_JARS")))

    # solo existentes (locales)
    exists = _existing(candidates)

    # ordenar priorizando tdgssconfig antes de terajdbc para visibilidad
    key_order = {"tdgssconfig": 0, "terajdbc": 1}
    exists.sort(key=lambda p: key_order.get("tdgssconfig" if "tdgss" in p.lower() else "terajdbc", 99))
    # dedup manteniendo orden
    dedup: List[str] = []
    seen = set()
    for p in exists:
        if p not in seen:
            seen.add(p)
            dedup.append(p)

    return dedup


# ------------------------ lectura de conf Spark ------------------------ #

def _spark_conf_get(spark, k: str, default: str = "") -> str:
    try:
        return spark.sparkContext.getConf().get(k, default) or default
    except Exception:
        return default


def _jars_already_configured(spark) -> bool:
    """
    Devuelve True si detectamos terajdbc/tdgss en spark.jars o classpaths.
    """
    conf_fields = [
        "spark.jars",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
    ]
    joined = " ".join(_spark_conf_get(spark, f, "") for f in conf_fields).lower()
    return ("terajdbc" in joined) or ("tdgss" in joined)


# ------------------------ inyección runtime de JARs ------------------------ #

def _add_runtime_jars(spark, jar_paths: List[str], log) -> None:
    """
    Intenta inyectar JARs en runtime vía JavaSparkContext.addJar.
    Ignora rutas s3:// aquí (usa --jars para eso).
    """
    if not jar_paths:
        return

    local_jars = [p for p in jar_paths if not p.lower().startswith("s3://")]
    if not local_jars:
        log.warning("Solo recibí JARs s3:// para inyección runtime; omitiendo. Usa --jars mejor.")
        return

    try:
        jsc = getattr(spark, "_jsc", None)
        if jsc is None:
            raise RuntimeError("spark._jsc no disponible para addJar")
        for jp in local_jars:
            jsc.addJar(jp)
        log.info(f"JARs inyectados en runtime: {local_jars}")
    except Exception as e:
        log.warning(f"No se pudo inyectar JARs en runtime: {e}")


def _verify_driver(spark, driver_class: str, log) -> None:
    try:
        spark._jvm.java.lang.Class.forName(driver_class)
        log.info(f"Driver JDBC verificado: {driver_class}")
    except Exception as e:
        log.warning(f"No se pudo verificar el driver {driver_class}: {e}")


# ------------------------ API principal ------------------------ #

def ensure_driver_loaded(
    spark,
    logger_or_jars: Optional[Union[object, List[str], str]] = None,
    jar_paths: Optional[List[str]] = None,
    driver_class: str = "com.teradata.jdbc.TeraDriver",
    search_dirs: Optional[List[str]] = None,
) -> None:
    """
    Firma tolerante:

      ensure_driver_loaded(spark, log)
      ensure_driver_loaded(spark, log, ["/path/tdgssconfig.jar","/path/terajdbc4.jar"])
      ensure_driver_loaded(spark, log, driver_class="com.teradata.jdbc.TeraDriver")
      ensure_driver_loaded(spark, [".../terajdbc4.jar",".../tdgssconfig.jar"])   # 2º arg = jars
      ensure_driver_loaded(spark, "com.teradata.jdbc.TeraDriver")                # 2º arg = driver

    Comportamiento:
      - Si detecta terajdbc/tdgss ya configurados vía --jars/extraClassPath => no inyecta.
      - Si no hay JARs configurados:
          * Usa jar_paths provistos o los descubre en rutas conocidas.
          * Intenta inyectar en runtime (solo rutas locales).
      - Siempre intenta verificar que el driver exista en el classpath actual.
    """
    # 1) Normaliza el logger y firma flexible
    log = None

    if hasattr(logger_or_jars, "info"):
        # 2º arg era un logger
        log = logger_or_jars  # type: ignore[assignment]
    elif isinstance(logger_or_jars, (list, tuple, set)):
        # 2º arg eran JARs
        jar_paths = list(logger_or_jars)  # type: ignore[assignment]
        log = _NoopLogger()
    elif isinstance(logger_or_jars, str):
        # 2º arg era driver_class
        driver_class = logger_or_jars
        log = _NoopLogger()
    else:
        log = _NoopLogger()

    # 2) Short-circuit si ya vinieron por --jars o extraClassPath
    if _jars_already_configured(spark):
        log.info("Teradata JARs presentes vía configuración (--jars/extraClassPath); no se inyecta en runtime.")
        _verify_driver(spark, driver_class, log)
        return

    # 3) Si no vinieron, computa jar_paths
    candidate_jars: List[str] = _as_list(jar_paths)

    if not candidate_jars:
        # desde env TERA_JDBC_JARS
        candidate_jars.extend(_split_env_list(os.getenv("TERA_JDBC_JARS")))

    if not candidate_jars:
        # discovery en rutas comunes
        candidate_jars = discover_teradata_jars(search_dirs=search_dirs)

    # 4) Inyección runtime (si hay algo local)
    if candidate_jars:
        _add_runtime_jars(spark, candidate_jars, log)
    else:
        log.warning(
            "No se encontraron JARs de Teradata para inyección runtime. "
            "Sugerencia: pásalos con --jars (tdgssconfig.jar, terajdbc4.jar) "
            "o define TERA_JDBC_JARS/TERA_JDBC_JARS_DIRS."
        )

    # 5) Verificación del driver
    _verify_driver(spark, driver_class, log)
