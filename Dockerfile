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
RUN uv pip install .

# Creamos la imagen final basada en Airflow
FROM apache/airflow:2.11.0-python3.11

# Copiamos las dependencias
COPY --from=builder /home/airflow/.local/lib/python3.11/site-packages /home/airflow/.local/lib/python3.11/site-packages
COPY --from=builder /app /app

# Copiar el paquete de código fuente del proyecto y los DAGs
COPY src /opt/airflow/src/
COPY dags /opt/airflow/dags/

# Copiar entrypoint que garantiza permisos en los bind-mounts montados desde el host.
# El entrypoint intentará chown/chmod en los directorios montados antes de ejecutar el comando.
COPY docker/entrypoint.sh /opt/airflow/entrypoint.sh
USER root
RUN chmod +x /opt/airflow/entrypoint.sh

# Usar el entrypoint. Nota: el contenedor correrá el entrypoint como el usuario por defecto
# de la imagen (root si no se ha cambiado). El entrypoint aplica los permisos y delega
# la ejecución al comando final.
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]

# Dejar que el proceso principal (airflow webserver/scheduler) corra. En este entorno de
# desarrollo lo dejamos correr como root para asegurar que el entrypoint pueda ajustar
# permisos en los bind-mounts. En entornos más restrictivos se puede mejorar para
# dropear privilegios después del entrypoint.