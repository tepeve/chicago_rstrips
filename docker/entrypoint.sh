#!/bin/bash
set -euo pipefail

HOST_UID="${HOST_UID:-1000}"
HOST_GID="${HOST_GID:-1000}"

DIRS=(
  /opt/airflow/data
  /opt/airflow/sql
  /opt/airflow/outputs
  /opt/airflow/tests
  /opt/airflow/logs
)

for d in "${DIRS[@]}"; do
  mkdir -p "$d"
done

# Dar permisos amplios para que tanto el usuario del host como airflow puedan escribir
chown -R "${HOST_UID}:${HOST_GID}" "${DIRS[@]}" 2>/dev/null || true
chmod -R 777 "${DIRS[@]}" 2>/dev/null || true

# Ejecutar el comando directamente (sin cambiar usuario)
exec "$@"