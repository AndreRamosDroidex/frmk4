import inspect

def get_name_function():
    """Devuelve el nombre de la función que llama a esta función."""
    frame = inspect.currentframe().f_back
    func = frame.f_code.co_name
    module = inspect.getmodule(frame)
    module_name = module.__name__ if module else "unknown_module"
    return f"{module_name}.{func}"

