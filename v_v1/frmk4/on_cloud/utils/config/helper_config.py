import json
import os
from pathlib import Path
from types import SimpleNamespace
from utils.others.helper_functions import get_name_function

# --- Utilitarios JSON ---

def _to_namespace(data):
    if isinstance(data, dict):
        return SimpleNamespace(**{k: _to_namespace(v) for k, v in data.items()})
    elif isinstance(data, list):
        return [_to_namespace(v) for v in data]
    else:
        return data


def _to_dict(obj):
    if isinstance(obj, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    elif isinstance(obj, list):
        return [_to_dict(v) for v in obj]
    else:
        return obj


def _load_json(origen, file_json):

    # Obtener el nombre de esta funcion para usarlo en los logs
    name_function = get_name_function()

    if origen.get("local-new-egde", False) or origen.get("local-ec2", False) :
        if not os.path.exists(file_json):
            raise FileNotFoundError(f"No se encontró el archivo: {file_json}")
        with open(file_json, "r", encoding="utf-8") as f:
            return json.load(f)
    elif origen.get("s3", False):
        import boto3
        from urllib.parse import urlparse
        if file_json.startswith("s3://"):
            s3 = boto3.client("s3")
            parsed = urlparse(file_json)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read().decode("utf-8"))
        else:
            with open(file_json, "r", encoding="utf-8") as f:
                return json.load(f)


# --- Clases de Configuración ---

class GlobalConfig(SimpleNamespace):
    """Configuración global con búsqueda por 'name'."""

    def get_properties(self, name: str):
        for cfg in self.config_app:
            if getattr(cfg, "name", None) == name:
                return _to_dict(cfg.properties)
        raise KeyError(f"No se encontró configuración global con name='{name}'")

    def get_property(self, name: str, key: str, default=None):
        props = self.get_properties(name)
        return props.get(key, default)


class ProcessConfig(SimpleNamespace):
    """Configuración de proceso con búsqueda por tipo, transform_id y load_type."""

    def get_config_app(self, name: str):
        """Obtiene propiedades del bloque 'config_app' del proceso (devuelve {} si no existe)."""
        if not hasattr(self, "config_app") or not self.config_app:
            return {}
        for cfg in self.config_app:
            if getattr(cfg, "name", None) == name:
                return _to_dict(cfg.properties)
        return {}

    def get_module(self, module: str):
        """
        Devuelve la lista de módulos dentro de 'process_modules' según el tipo (ej: 'extract', 'transform', 'load').
        Si no existe, devuelve una lista vacía.
        """
        if not hasattr(self, "process_modules") or not self.process_modules:
            return []

        result = []
        for mod in self.process_modules:
            if hasattr(mod, module):  # 
                modules_list = getattr(mod, module, [])
                result.extend(_to_dict(modules_list))
        return result


# --- Clase principal ---

class ConfigManager:
    """Maneja las configuraciones globales y de proceso."""

    def __init__(self, id_tracking: str, id_process: str, fecinformacion: str, datacenter: str, log = None):
        
        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()
        
        # Atributos 
        self.id_tracking = id_tracking
        self.id_process = id_process
        self.fecinformacion = fecinformacion
        self.datacenter = datacenter
        self.log = log
        
        # Origen activos
        self.origen = {
            "local-new-egde": False,
            "local-ec2": True,
            "s3": False
        }
        
        # Hardcode json 
        if self.origen.get("local-new-egde", False):
            # New Egde
            file_global_json = f"/data/desa/IBKProjects/DataHub/frmk4/on_cloud/config/global/config-global.json"
            file_process_json = f"/data/desa/IBKProjects/DataHub/frmk4/on_cloud/config/process/config-process-4001.json"
        elif self.origen.get("local-ec2", False):
            # AWS - EC2
            file_global_json = f"/home/ubuntu/frmk4/on_cloud/config/global/config-global.json"
            file_process_json = f"/home/ubuntu/frmk4/on_cloud/config/process/config-process-4001.json"
        elif self.origen.get("s3", False):
            # AWS - s3
            file_global_json = f"s3://xxxxxxxx/config-global.json"
            file_process_json = f"s3://xxxxxxxx/config-process-4001.json"
       
        config_global_json = _load_json(self.origen, file_global_json)
        self._global = GlobalConfig(**vars(_to_namespace(config_global_json)))
       
        config_process_json = _load_json(self.origen, file_process_json)
        self._process = ProcessConfig(**vars(_to_namespace(config_process_json)))

        if self.log:
            self.log.registrar("INFO",f"[{name_function}] - Configuraciones cargadas correctamente.")
        else:
            print("Configuraciones cargadas correctamente.")
            

    def get_merged_properties_config_app(self, name: str):
        """Fusiona la propiedad config_app que se pueden encontrar en la configuracion global y en el proceso (el proceso tiene prioridad)."""
        globalProps = self._global.get_properties(name)
        processProps = {}
        try:
            processProps = self._process.get_config_app(name)
        except KeyError:
            pass
        merged = {**globalProps, **processProps}
        return merged

    def getConfig(self):
        """Devuelve ambas configuraciones global y de proceso."""
        return self


# --- Ejemplo de uso ---
if __name__ == "__main__":
    print("Test")
