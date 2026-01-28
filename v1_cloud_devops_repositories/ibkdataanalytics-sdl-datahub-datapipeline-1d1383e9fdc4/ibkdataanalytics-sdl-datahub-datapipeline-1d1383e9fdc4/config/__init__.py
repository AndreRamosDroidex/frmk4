import os
import importlib
import sys


env = os.getenv("ENVIRONMENT").lower()
MODULE_NAME = f"config.constants.{env}_constants"
print(f"Importando: {MODULE_NAME}")

constants = importlib.import_module(MODULE_NAME)

for attr in dir(constants):
    if not attr.startswith("__"):
        globals()[attr] = getattr(constants, attr)

sys.modules["config.constants"] = sys.modules[__name__]
