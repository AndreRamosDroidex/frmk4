import json
import os
from pathlib import Path
from types import SimpleNamespace
from utils.others.helper_functions import get_name_function

# -------- util --------
def _to_namespace(data):
    if isinstance(data, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in data.items()})
    elif isinstance(data, list):
        return [_to_namespace(v) for v in data]
    return data

def _to_dict(obj):
    if isinstance(obj, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    elif isinstance(obj, list):
        return [_to_dict(v) for v in obj]
    return obj

def _json_dedecimalize(pyobj):
    # Dynamo trae Decimals; esto los convierte a str/float donde aplique
    return json.loads(json.dumps(pyobj, default=str))

# -------- cargas --------
def _load_json(origen, file_json):
    name_function = get_name_function()
    if origen.get("local-new-egde", False) or origen.get("local-ec2", False):
        if not os.path.exists(file_json):
            raise FileNotFoundError(f"No se encontró el archivo: {file_json}")
        with open(file_json, "r", encoding="utf-8") as f:
            return json.load(f)
    elif origen.get("s3", False):
        import boto3
        from urllib.parse import urlparse
        if file_json.startswith("s3://"):
            s3 = boto3.client("s3")
            p = urlparse(file_json)
            obj = s3.get_object(Bucket=p.netloc, Key=p.path.lstrip("/"))
            return json.loads(obj["Body"].read().decode("utf-8"))
        with open(file_json, "r", encoding="utf-8") as f:
            return json.load(f)
    raise ValueError("Origen de JSON no soportado.")

def _load_global_from_dynamo(table_name: str):
    import boto3
    name_function = get_name_function()
    if not table_name:
        raise ValueError("Dynamo global table name vacío.")
    ddb = boto3.resource("dynamodb")
    tbl = ddb.Table(table_name)
    # La PK es 'state_machine' (según tu ejemplo). Tomamos el primer item si hay varios.
    # Si el diseño tuviera 1 solo item, también funciona con scan(limit=1).
    resp = tbl.scan(Limit=1)
    items = resp.get("Items", [])
    if not items:
        raise RuntimeError(f"No se encontró item en Dynamo global: {table_name}")
    item = items[0]
    data = _json_dedecimalize(item)
    # Esperamos: { "config_values": [ { "framework": "...", "config_app": [...] } ], ... }
    cfg_values = data.get("config_values", [])
    if not cfg_values:
        raise RuntimeError("Campo 'config_values' vacío en Dynamo Global.")
    return cfg_values[0]

def _load_process_from_dynamo(table_name: str, batch_id: str):
    import boto3
    if not table_name:
        raise ValueError("Dynamo process table name vacío.")
    if not batch_id:
        raise ValueError("batch_id vacío para cargar process config.")
    ddb = boto3.resource("dynamodb")
    tbl = ddb.Table(table_name)
    # PK: 'batch_id' (string)
    resp = tbl.get_item(Key={"batch_id": batch_id})
    item = resp.get("Item")
    if not item:
        raise RuntimeError(f"No existe batch_id={batch_id} en {table_name}.")
    return _json_dedecimalize(item)

# -------- clases --------
class GlobalConfig(SimpleNamespace):
    def get_properties(self, name: str):
        for cfg in self.config_app:
            if getattr(cfg, "name", None) == name:
                return _to_dict(cfg.properties)
        raise KeyError(f"No se encontró configuración global con name='{name}'")

    def get_property(self, name: str, key: str, default=None):
        props = self.get_properties(name)
        return props.get(key, default)

class ProcessConfig(SimpleNamespace):
    def get_config_app(self, name: str):
        if not hasattr(self, "config_app") or not self.config_app:
            return {}
        for cfg in self.config_app:
            if getattr(cfg, "name", None) == name:
                return _to_dict(cfg.properties)
        return {}

    def get_module(self, module: str):
        if not hasattr(self, "process_modules") or not self.process_modules:
            return []
        result = []
        for mod in self.process_modules:
            if hasattr(mod, module):
                result.extend(_to_dict(getattr(mod, module, [])))
        return result

class ConfigManager:
    def __init__(self, id_tracking: str, id_process: str, fecinformacion: str, datacenter: str, log=None):
        name_function = get_name_function()
        self.id_tracking = id_tracking
        self.id_process = id_process          # aquí llega tu BATCH_ID
        self.fecinformacion = fecinformacion
        self.datacenter = datacenter
        self.log = log

        # ORIGEN: ahora Dynamo (EMR)
        self.origen = {
            "dynamo": True,
            "s3": False,
            "local-ec2": False,
            "local-new-egde": False
        }

        if self.origen.get("dynamo"):
            # Lee nombres de tablas desde env (setéalos en el .sh)
            ddb_global = os.environ.get("DDB_GLOBAL_TABLE")   # ej: aw-dev-db-frmk4-config-global-demo
            ddb_process = os.environ.get("DDB_PROCESS_TABLE") # ej: aw-dev-db-frmk4-config-process-demo

            global_item = _load_global_from_dynamo(ddb_global)
            process_item = _load_process_from_dynamo(ddb_process, self.id_process)

            # Global: esperamos la forma { "framework": "...", "config_app": [...] }
            self._global = GlobalConfig(**vars(_to_namespace(global_item)))
            # Process: el item completo como ProcessConfig (contiene process_modules, process_type, etc.)
            self._process = ProcessConfig(**vars(_to_namespace(process_item)))

        else:
            # Fallbacks anteriores (no usados en EMR)
            raise RuntimeError("ConfigManager: origen no-Dynamo no habilitado en esta build.")

        if self.log:
            self.log.registrar("INFO", f"[{name_function}] - Config cargada desde DynamoDB correctamente.")
        else:
            print("Config cargada desde DynamoDB correctamente.")

    def get_merged_properties_config_app(self, name: str):
        globalProps = self._global.get_properties(name)
        processProps = {}
        try:
            processProps = self._process.get_config_app(name)
        except KeyError:
            pass
        merged = {**globalProps, **processProps}
        return merged

    def getConfig(self):
        return self
