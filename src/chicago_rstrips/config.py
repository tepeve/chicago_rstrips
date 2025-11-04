import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Variables de configuración
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")

# SOCRATA API TOKEN 
CHIC_TNP_API_URL = os.getenv("CHIC_TNP_API_URL")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

if not SOCRATA_APP_TOKEN:
    raise ValueError("La variable de entorno SOCRATA_APP_TOKEN no está configurada.")