import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Variables de configuración

# Date Range for Data Extraction
START_DATE="2025-09-01T00:00:00.000"
END_DATE="2025-09-30T23:59:00.000"

# ENDPOINT DE LA API DE TRIPS
# https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh
CHIC_TNP_API_URL="https://data.cityofchicago.org/api/v3/views/6dvr-xwnh/query.json"


# ENDPOINT DE LA API DE TRAFFIC TRACKINGS
# https://dev.socrata.com/foundry/data.cityofchicago.org/kf7e-cur8
CHIC_TRAFFIC_API_URL="https://data.cityofchicago.org/api/v3/views/kf7e-cur8/query.json"

# SECRETOS Y CREDENCIALES

# SOCRATA API TOKEN 
# https://dev.socrata.com/docs/app-tokens
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

if not SOCRATA_APP_TOKEN:
    raise ValueError("La variable de entorno SOCRATA_APP_TOKEN no está configurada.")

# WEATHER API KEY
# https://www.visualcrossing.com/resources/documentation/
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
if not WEATHER_API_KEY:
    raise ValueError("La variable de entorno WEATHER_API_KEY no está configurada.")

# REDSHIFT CREDENTIALS
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")


# Local Postgres (para desarrollo)
POSTGRES_LOCAL_HOST = os.getenv("POSTGRES_LOCAL_HOST", "localhost")
POSTGRES_LOCAL_PORT = int(os.getenv("POSTGRES_LOCAL_PORT", 5433))
POSTGRES_LOCAL_USER = os.getenv("POSTGRES_LOCAL_USER")
POSTGRES_LOCAL_PASSWORD = os.getenv("POSTGRES_LOCAL_PASSWORD")
POSTGRES_LOCAL_DB = os.getenv("POSTGRES_LOCAL_DB")
if not all([POSTGRES_LOCAL_USER, POSTGRES_LOCAL_PASSWORD, POSTGRES_LOCAL_DB]):
    raise ValueError("Las credenciales de la base de datos local Postgres no están completamente configuradas.")