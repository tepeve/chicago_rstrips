# Makefile para el proyecto chicago_rstrips

# Evita conflictos con archivos que tengan el mismo nombre que un target.
.PHONY: all build up run-project start stop down clean logs shell test help

# Target por defecto que se ejecuta con solo 'make'
all: help

# Construye o reconstruye las imágenes de los servicios
build: ## Construye o reconstruye las imágenes de los servicios
	@echo "Construyendo imágenes de Docker..."
	docker-compose build

# Levanta los servicios en segundo plano (detached mode)
up: ## Levanta los servicios en segundo plano (detached mode)
	@echo "Iniciando servicios con Docker Compose..."
	docker-compose up -d

# Comando principal para el usuario: construye y levanta los servicios
run-project: build up ## Comando principal para el usuario: construye y levanta los servicios
	@echo "Proyecto desplegado. Accede a Airflow en http://localhost:8080"
	@echo "Para ver logs en tiempo real, ejecuta 'make logs'"
	@echo "para detener los servicios, ejecuta 'make down'"
	@echo "para eliminar los contenedores, volúmenes e imágenes, ejecuta 'make clean'"
	docker-compose ps


# Alias para 'up'
start: up ## Alias para 'up'

# Detiene los servicios sin eliminarlos
stop: ## Detiene los servicios sin eliminarlos
	@echo "Deteniendo servicios..."
	docker-compose stop

# Detiene y elimina los contenedores y redes
down: ## Detiene y elimina los contenedores y redes
	@echo "Deteniendo y eliminando contenedores..."
	docker-compose down

# Limpieza completa: elimina contenedores, redes, volúmenes y imágenes
clean: ## Limpieza completa: elimina contenedores, redes, volúmenes e imágenes
	@echo "Realizando limpieza completa (contenedores, volúmenes e imágenes)..."
	docker-compose down -v --rmi all

# Muestra los logs de los servicios en tiempo real.
# Uso: make logs o make logs service=airflow-scheduler
logs: ## Muestra los logs de los servicios en tiempo real
	@echo "Mostrando logs... (Presiona Ctrl+C para salir)"
	docker-compose logs -f $(service)

# Accede a la terminal de un servicio.
# Uso: make shell service=airflow-scheduler
shell: ## Accede a la terminal de un servicio
	@echo "Accediendo a la terminal del servicio: $(service)..."
	@if [ -z "$(service)" ]; then \
        echo "Error: Debes especificar un servicio. Uso: make shell service=<nombre-del-servicio>"; \
        exit 1; \
    fi
	docker-compose exec $(service) bash

# Corre los tests unitarios usando pytest dentro del contenedor de Airflow
test: ## Corre los tests unitarios usando pytest dentro del contenedor de Airflow
	@echo "Corriendo tests con pytest dentro del contenedor..."
	docker-compose exec airflow-scheduler pytest
# Muestra esta ayuda
help: ## Muestra esta ayuda
	@echo "Comandos disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'