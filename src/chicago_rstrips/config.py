import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Variables de configuración
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")

# SOCRATA API TOKEN 
# https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh
CHIC_TNP_API_URL = os.getenv("CHIC_TNP_API_URL")
# https://dev.socrata.com/docs/app-tokens
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

if not SOCRATA_APP_TOKEN:
    raise ValueError("La variable de entorno SOCRATA_APP_TOKEN no está configurada.")


# REDSHIFT CREDENTIALS
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DBNAME = os.getenv("REDSHIFT_DBNAME")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME")