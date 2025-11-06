# Usa la imagen oficial de Airflow con la versión que necesitas
FROM apache/airflow:2.11.0-python3.11

USER root
# instala dependencias del proyecto (si usas pyproject.toml, puedes instalar con pip)
COPY pyproject.toml /opt/airflow/

# Instalamos las dependencias de tu proyecto.
# Omitimos 'apache-airflow' porque ya está en la imagen base.
# Esto usa pip, que está incluido en la imagen.
RUN pip install --no-cache-dir \
    "boto3" \
    "numpy" \
    "pandas" \
    "psycopg2-binary" \
    "requests>=2.32.5" \
    "sodapy" \
    "python-dotenv" \
    "pyarrow" \
    "sqlalchemy"
# copiar el paquete y dags
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/


# Volvemos al usuario 'airflow' por seguridad y buenas prácticas
USER airflow