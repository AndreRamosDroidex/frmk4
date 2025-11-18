import os
from typing import Dict, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from utils.others.helper_functions import get_name_function

class Transformer:
    def __init__(self, sparkSession, config):
        self.spark = sparkSession
        self.config = config
        self.log = config.log

        # Paralelismo por defecto (puedes sobreescribirlo en config_app:name="transform")
        tr_cfg = {}
        try:
            tr_cfg = self.config._process.get_config_app("transform")
            if not tr_cfg:
                tr_cfg = self.config._global.get_properties("transform") if hasattr(self.config._global, "config_app") else {}
        except Exception:
            pass
        self.parallelism = int(tr_cfg.get("parallelism", 4))

    # on_cloud/transform/transformer.py  (solo este método cambia)
    def _load_sql_text(self, sql_or_filename: str, ttype: str) -> str:
        effective_type = (ttype or "code").lower()

        if effective_type == "file":
            paths = self.config.get_merged_properties_config_app("paths")
            sql_dir = paths.get("path_transform_sql", "")
            file_path = os.path.join(sql_dir, sql_or_filename)
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"No se encontró el archivo SQL: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        else:
            if ttype is None and isinstance(sql_or_filename, str) and sql_or_filename.lower().endswith(".sql"):
                self.log.registrar(
                    "WARNING",
                    "[Transformer] 'transform_type' no especificado; "
                    f"se asumirá 'code' (inline) aunque termine en .sql: {sql_or_filename}"
                )
            return sql_or_filename


    def _execute_one_transform(self, t_id: str, sql_text: str) -> Tuple[str, DataFrame, int]:
        # Reemplazar el token de entrada por la vista base
        query = sql_text.replace("{{SOURCE_INPUT}}", "source_table")

        # Ejecutar SQL
        df_t = self.spark.sql(query)

        # Cache para acelerar usos posteriores y permitir materialización concurrente
        df_t = df_t.cache()

        # Registrar vista temporal (útil para debug/encadenar)
        df_t.createOrReplaceTempView(f"tx_{t_id}")

        # Materializar esta transform (acción)
        rows = df_t.count()
        return (t_id, df_t, rows)

    # ---------------- public API ----------------
    def run(self, df_input: DataFrame) -> Tuple[Dict[str, DataFrame], DataFrame]:
        name_function = get_name_function()
        self.log.registrar("INFO", f"[{name_function}] - Module Transformer (parallel)")

        transforms = self.config._process.get_module("transform")
        if not transforms:
            raise ValueError("No hay bloques 'transform' en process_modules.")

        # Vista base disponible para todos los SQL (Dataframe Base Original)
        df_input.createOrReplaceTempView("source_table")

        #tareas
        tasks: List[Tuple[str, str]] = []
        order_ids: List[str] = []
        for i, tcfg in enumerate(transforms, start=1):
            t_id = str(tcfg.get("transform_id") or i)
            t_type = tcfg.get("transform_type")  # "code" | "file" | None
            raw = tcfg.get("transform_sql")
            if not raw:
                self.log.registrar("WARNING", f"[{name_function}] - transform_id={t_id} sin 'transform_sql'. Se omite.")
                continue

            sql_text = self._load_sql_text(raw, t_type)
            tasks.append((t_id, sql_text))
            order_ids.append(t_id)

            mode_str = t_type if t_type else ("file" if raw.lower().endswith(".sql") else "code")
            self.log.registrar("INFO", f"[{name_function}] - Transform preparada id={t_id} (type={mode_str})")

        # Ejecuta en paralelo
        dfs_by_id: Dict[str, DataFrame] = {}
        with ThreadPoolExecutor(max_workers=max(1, self.parallelism)) as executor:
            futures = {executor.submit(self._execute_one_transform, t_id, sql_text): t_id for (t_id, sql_text) in tasks}
            for fut in as_completed(futures):
                t_id = futures[fut]
                try:
                    tid, df_t, rows = fut.result()
                    dfs_by_id[tid] = df_t
                    self.log.registrar("INFO", f"[{name_function}] - transform_id={tid} finalizada -> rows={rows}")
                except Exception as e:
                    self.log.registrar("ERROR", f"[{name_function}] - transform_id={t_id} falló: {e}")
                    raise

        # Último DF en el orden del JSON (si existe)
        last_df = dfs_by_id[order_ids[-1]] if order_ids else df_input

        return dfs_by_id, last_df
