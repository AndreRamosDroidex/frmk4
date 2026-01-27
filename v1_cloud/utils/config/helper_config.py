# on_cloud/utils/config/helper_config.py
import json, time
import os
import boto3
from pathlib import Path
from types import SimpleNamespace
from utils.others.helper_functions import get_name_function

from decimal import Decimal

def _get_region():
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

def _boto_session():
    # centralizamos la región para TODOS los clientes boto3
    return boto3.session.Session(region_name=_get_region())
    
# ---------- Utilitarios de Namespace/Dict ----------
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

def _sanitize_decimals(obj):
    """Convierte recursivamente Decimal -> int/float y mantiene dict/list."""
    if isinstance(obj, dict):
        return {k: _sanitize_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_decimals(v) for v in obj]
    if isinstance(obj, Decimal):
        # Si es entero exacto, devuélvelo como int
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

# ---------- Cargas locales ----------
def _load_json_local(file_json: str):
    if not os.path.exists(file_json):
        raise FileNotFoundError(f"No se encontró el archivo: {file_json}")
    with open(file_json, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------- Carga Dynamo ----------
def _load_process_from_dynamo(table_name: str, batch_id: str):
    
    # ddb = boto3.resource("dynamodb")
    session = _boto_session()
    ddb = session.resource("dynamodb")
    table = ddb.Table(table_name)
    resp = table.get_item(Key={"batch_id": str(batch_id)})
    item = resp.get("Item")
    if not item:
        raise KeyError(f"No existe batch_id='{batch_id}' en DynamoDB '{table_name}'")
    return _sanitize_decimals(item)

def _load_global_from_dynamo(table_name: str, state_machine: str = None):
    
    session = _boto_session()
    ddb = session.resource("dynamodb")
    table = ddb.Table(table_name)

    scan = table.scan(Limit=50)
    items = scan.get("Items", [])
    if not items:
        raise KeyError(f"No hay items en DynamoDB '{table_name}'")

    if state_machine:
        for it in items:
            if it.get("state_machine") == state_machine:
                cfgs = it.get("config_values") or []
                if not cfgs:
                    raise KeyError("config_values vacío en item global")
                
                return _sanitize_decimals(cfgs[0])
        raise KeyError(f"No se encontró state_machine='{state_machine}' en '{table_name}'")
    else:
        cfgs = items[0].get("config_values") or []
        if not cfgs:
            raise KeyError("config_values vacío en item global")
        return _sanitize_decimals(cfgs[0])

# ---------- Clases de Config ----------
class GlobalConfig(SimpleNamespace):
    def get_properties(self, name: str):
        for cfg in getattr(self, "config_app", []):
            if getattr(cfg, "name", None) == name:
                return _to_dict(cfg.properties)
        raise KeyError(f"No se encontró configuración global con name='{name}'")

    def get_property(self, name: str, key: str, default=None):
        try:
            props = self.get_properties(name)
            return props.get(key, default)
        except KeyError:
            return default

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
                result.extend(_to_dict(getattr(mod, module)))
        return result

# ---------- ConfigManager ----------
class ConfigManager:
    """Carga config global y de proceso desde Dynamo o local, según CONFIG_SOURCE."""
    def __init__(self, id_tracking: str, id_process: str, fecinformacion: str, datacenter: str, log=None):
        name_function = get_name_function()

        self.id_tracking = id_tracking
        self.id_process = id_process       
        self.fecinformacion = fecinformacion
        self.datacenter = datacenter
        self.log = log

        #cargar config: 'dynamo' | 'local'
        source = os.getenv("CONFIG_SOURCE", "dynamo").lower()

        if source == "dynamo":
            ddb_process = os.getenv("DDB_PROCESS_TABLE", "aw-dev-db-frmk4-config-process-demo")
            ddb_global  = os.getenv("DDB_GLOBAL_TABLE",  "aw-dev-db-frmk4-config-global-demo")
            state_machine = os.getenv("DDB_GLOBAL_PK_VALUE", "aw-dev-sfn-frmk4-process-onpremise-demo")

            process_json = _load_process_from_dynamo(ddb_process, self.id_process)
            global_json  = _load_global_from_dynamo(ddb_global, state_machine)

            
            if self.log:
                self.log.registrar("INFO", "[helper_config] - JSON process (raw) => " + json.dumps(process_json, ensure_ascii=False))
                self.log.registrar("INFO", "[helper_config] - JSON global  (raw) => " + json.dumps(global_json,  ensure_ascii=False))
        else:
           
            project_root = Path(__file__).resolve().parents[3]  # .../frmk
            file_global_json  = project_root / "on_cloud" / "config" / "global"  / "config-global.json"
            file_process_json = project_root / "on_cloud" / "config" / "process" / f"config-process-{self.id_process}.json"

            global_json  = _load_json_local(str(file_global_json))
            process_json = _load_json_local(str(file_process_json))

        #Normalizar Namespaces
        self._global  = GlobalConfig(**vars(_to_namespace(global_json)))
        self._process = ProcessConfig(**vars(_to_namespace(process_json)))

        if self.log:
            self.log.registrar("INFO", f"[{name_function}] - Config cargada desde {source}.")
        else:
            print(f"Config cargada desde {source}.")

    def get_merged_properties_config_app(self, name: str):
        g = {}
        try:
            g = self._global.get_properties(name)
        except Exception:
            pass
        p = {}
        try:
            p = self._process.get_config_app(name)
        except Exception:
            pass
        return {**g, **p}

    def getConfig(self):
        return self
