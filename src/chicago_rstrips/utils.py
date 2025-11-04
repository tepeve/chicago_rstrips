from pathlib import Path

def get_project_root():
    """Obtiene el directorio raíz del proyecto."""
    return Path(__file__).parent.parent.parent

def get_data_dir():
    """Obtiene el directorio de datos, creándolo si no existe."""
    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir

def get_raw_data_dir():
    """Obtiene el directorio de datos raw, creándolo si no existe."""
    raw_dir = get_data_dir() / "raw"
    raw_dir.mkdir(exist_ok=True)
    return raw_dir

def get_processed_data_dir():
    """Obtiene el directorio de datos procesados, creándolo si no existe."""
    processed_dir = get_data_dir() / "processed"
    processed_dir.mkdir(exist_ok=True)
    return processed_dir