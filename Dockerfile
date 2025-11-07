# Usamos la imagen de docker de airflow como base
FROM apache/airflow:2.11.0-python3.11 AS builder

# Instalar paquetes del sistema como 'root'
USER root

# 'libpq-dev' es para psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


# Volvemos al usuario 'airflow' para instalar paquetes de Python
USER airflow

# instalamos 'uv'
RUN pip install --no-cache-dir uv

# Creamos un directorio de trabajo
WORKDIR /app

# Copiamos el toml con las dependencias y el codigo fuente para empaquetar
COPY pyproject.toml ./
COPY src ./src

# Le decimos a 'uv' que instale el proyecto local
# 'uv' verifica que paquetes vienen preinstalados en la imagen de docker
# y solo instala lo que faltan y el paquete del codigo fuente.
RUN uv pip install .

# Creamos la imagen final basada en Airflow
FROM apache/airflow:2.11.0-python3.11

# Copiamos las dependencias NUEVAS (boto3, sodapy, tu paquete)
COPY --from=builder /home/airflow/.local/lib/python3.11/site-packages /home/airflow/.local/lib/python3.11/site-packages
COPY --from=builder /app /app

# Copiar el paquete de código fuente del proyecto y los DAGs
COPY src /opt/airflow/src/
COPY dags /opt/airflow/dags/

USER airflow