# Chicago Ridesharing Trips ETL

[![Tests](https://github.com/tu-usuario/chicago_rstrips/workflows/Tests/badge.svg)](https://github.com/tu-usuario/chicago_rstrips/actions)
[![codecov](https://codecov.io/gh/tu-usuario/chicago_rstrips/branch/main/graph/badge.svg)](https://codecov.io/gh/tu-usuario/chicago_rstrips)

Proyecto de data engineering que implementa un pipeline ETL/ELT para extraer, procesar y analizar datos abierto de viajes de proveedores de redes de transporte (TNP) en la ciudad de Chicago.

## 🎯 Propósito del Pipeline

El objetivo principal de este proyecto es construir un sistema escalable para la ingesta, procesamiento y almacenamiento de datos públicos sobre viajes de ridesharing (como Uber y Lyft) en Chicago. El pipeline integra múltiples fuentes de datos para enriquecer la información de los viajes y consolidarla en un Data Warehouse para su posterior análisis y explotación.

Los componentes principales del pipeline son:

*   **Extracción (E):** Consume datos de tres fuentes principales a través de sus APIs:
    1.  **Viajes (Trips):** Datos detallados de cada viaje desde el portal de datos abiertos de Chicago.
    2.  **Tráfico (Traffic):** Información sobre la congestión vehicular en tiempo real.
    3.  **Clima (Weather):** Condiciones meteorológicas históricas por hora.
*   **Carga (L):** Carga los datos crudos extraídos en un área de Staging en una base de datos PostgreSQL, actuando como una capa intermedia antes de la transformación. 
*   **Transformación (T):** Depura y modela los datos desde el área de Staging para poblar un Data Warehouse con un esquema dimensional (tablas de hechos y dimensiones), y finalmente, crea vistas materializadas (Data Marts) optimizadas para consultas analíticas y de negocio. 

## 📁 Estructura del Proyecto
```
chicago_rstrips/
├── src/chicago_rstrips/    # Código fuente
├── tests/                   # Tests unitarios e integración
├── dags/                    # DAGs de Airflow
├── sql/                     # Scripts SQL
└── data/                    # Datos locales (no versionado)
```

## 🚀 Instalación y Despliegue con Docker

El proyecto está contenedorizado con Docker y utiliza Docker Compose para orquestar los servicios necesarios (Airflow, PostgreSQL).

1.  **Clonar el Repositorio:**
    ```bash
    git clone "https://github.com/tepeve/chicago_rstrips.git"
    cd chicago-rstrips
    ```

2.  **Configurar Variables de Entorno:**
    El manejo de entornos y dependencias está gestionado con uv. Crea un archivo `.env` en la raíz del proyecto. Puedes crearlo desde cero con las siguientes variables necesarias para desplegar el sistema:
    ```env
    # Credenciales para las APIs
    SOCRATA_APP_TOKEN="TU_APP_TOKEN_DE_SOCRATA"
    WEATHER_API_KEY="TU_API_KEY_DE_VISUALCROSSING"

    # Configuración de la Base de Datos (usada por Airflow y los scripts)
    POSTGRES_LOCAL_USER=
    POSTGRES_LOCAL_PASSWORD=
    POSTGRES_LOCAL_HOST=
    POSTGRES_LOCAL_PORT=
    POSTGRES_LOCAL_DB=
    ```
    > **Nota:** Las credenciales de la base de datos deben coincidir con las definidas en `docker-compose.yml` para que Airflow pueda conectarse.

3.  **Inicializar Airflow y Levantar los Servicios:**
    El `entrypoint.sh` se encargará de inicializar la base de datos de Airflow.
    ```bash
    docker-compose up -d
    ```

4.  **Acceder a la UI de Airflow:**
    Abre tu navegador y ve a `http://localhost:8080`. El usuario y contraseña por defecto son `airflow`.

5.  **Ejecutar los DAGs:**
    *   **Cold Start:** Primero, activa y ejecuta manualmente el DAG `coldstart_etl_pipeline`. Este proceso inicializa la base de datos, carga todas las tablas y realiza unaprimera ingesta de datos históricos.
    *   **Batch Incremental:** Una vez que el `coldstart_etl_pipeline` haya finalizado con éxito, activa el DAG `batch_etl_pipeline`. Este se ejecutará diariamente (`@daily`) para procesar los nuevos datos de forma incremental.

## 🧬 Arquitectura y Flujo de Datos

El sistema se basa en dos DAGs de Airflow principales que orquestan todo el flujo ELT.

### Diagrama de Flujo de los DAGs

```mermaid
---
config:
  layout: dagre
---
flowchart LR
 subgraph Setup["Configuración Inicial"]
    direction TB
        DDL["setup_ddl<br>Crear Schemas y Tablas"]
  end
 subgraph Static["Datos Estáticos y Espaciales"]
    direction TB
        GEN_STATIC("generate_static_features<br>City, Stations, Voronoi")
        LOAD_STATIC[("load_static_features<br>Dim Spatial Tables")]
        LOAD_REGIONS[("load_regions<br>")]
        LOAD_LOCS[("load_locations<br>Staging DB")]
  end
 subgraph Ingestion["Extracción de datos"]
    direction TB
        EXT_TRIPS("extract_trips<br>by API SOCRATA")
        EXT_TRAFFIC("extract_traffic<br>by API SOCRATA")
        EXT_WEATHER("extract_weather<br>by Visual Crossing API")
  end
 subgraph Staging["Carga a Staging"]
        LOAD_TRIPS[("load_trips<br>Staging DB")]
        LOAD_TRAFFIC[("load_traffic<br>Staging DB")]
        LOAD_WEATHER[("load_weather<br>Staging DB")]
  end
 subgraph Facts["Capa de Hechos y Datamarts"]
        FACT_TRIPS[("fact_trips<br>Staging DB")]
        FACT_TRAFFIC[("fact_traffic<br>Staging DB")]
        FACT_WEATHER[("fact_weather<br>Staging DB")]
  end
    DDL --> GEN_STATIC & EXT_TRIPS & EXT_TRAFFIC & EXT_WEATHER
    GEN_STATIC --> LOAD_STATIC
    EXT_TRIPS --> LOAD_TRIPS
    EXT_TRIPS -.-> LOAD_LOCS
    EXT_TRAFFIC --> LOAD_TRAFFIC
    EXT_TRAFFIC -.-> LOAD_REGIONS
    EXT_WEATHER --> LOAD_WEATHER
    LOAD_TRIPS --> VERIFY{"Data Depuration<br>populate_initial_facts"}
    LOAD_TRAFFIC --> VERIFY
    LOAD_WEATHER --> VERIFY
    VERIFY --> FACT_TRIPS & FACT_TRAFFIC & FACT_WEATHER
    LOAD_STATIC --> EXT_WEATHER
     DDL:::setup
     GEN_STATIC:::extract
     LOAD_STATIC:::db
     LOAD_REGIONS:::db
     LOAD_LOCS:::db
     EXT_TRIPS:::extract
     EXT_TRAFFIC:::extract
     EXT_WEATHER:::extract
     LOAD_TRIPS:::db
     LOAD_TRAFFIC:::db
     LOAD_WEATHER:::db
     FACT_TRIPS:::db
     FACT_TRAFFIC:::db
     FACT_WEATHER:::db
     VERIFY:::check
    classDef setup fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef extract fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef db fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    classDef transform fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef check fill:#fff9c4,stroke:#fbc02d,stroke-width:2px,stroke-dasharray: 5 5
    style Static stroke:none
    style Setup stroke:none
    style Ingestion stroke:none
    style Staging stroke:none
    style Facts stroke:none

```

### Componentes del Pipeline

*   **DAGs de Airflow (`dags/`):**
    *   [`coldstart_etl_pipeline.py`](dags/coldstart_etl_pipeline.py): Orquesta la configuración inicial del entorno. Crea el esquema de la base de datos, carga dimensiones estáticas (límites de la ciudad, estaciones meteorológicas) y realiza la ingesta masiva de datos para el período histórico definido. **Se ejecuta una sola vez.**
    *   [`batch_etl_pipeline.py`](dags/batch_etl_pipeline.py): Orquesta el proceso incremental diario. Extrae, carga y transforma los datos correspondientes a las últimas 24 horas. Espera a que el `coldstart_etl_pipeline` haya terminado exitosamente antes de su primera ejecución.

*   **Scripts de Extracción (`src/chicago_rstrips/`):**
    *   [`extract_trips_data.py`](src/chicago_rstrips/extract_trips_data.py): Se conecta a la API de Socrata para obtener datos de viajes.
    *   [`extract_traffic_data.py`](src/chicago_rstrips/extract_traffic_data.py): Obtiene datos de congestión vehicular de la API de Socrata.
    *   [`extract_weather_data.py`](src/chicago_rstrips/extract_weather_data.py): Obtiene datos climáticos de la API de Visual Crossing.

*   **Scripts de Carga y Base de Datos (`src/chicago_rstrips/`):**
    *   [`db_loader.py`](src/chicago_rstrips/db_loader.py): Contiene funciones genéricas para interactuar con PostgreSQL, como ejecutar DDLs y cargar DataFrames.
    *   [`load_facts_to_staging.py`](src/chicago_rstrips/load_facts_to_staging.py): Scripts para cargar datos de hechos (trips, traffic) a las tablas de staging.
    *   [`load_dim_*.py`](src/chicago_rstrips/load_dim_dynamic_tables.py): Scripts para cargar datos a las tablas de dimensiones.

*   **Scripts SQL (`sql/`):**
    *   Contienen las sentencias DDL para crear los schemas, tablas y vistas materializadas, así como las sentencias DML (UPSERT) para mover datos de Staging al Data Warehouse.

### Modelo de Datos (PostgreSQL)

El pipeline construye y puebla una base de datos PostgreSQL con los siguientes schemas:

*   `staging`: Almacena los datos crudos extraídos de las APIs.
*   `dim_spatial`: Contiene dimensiones geoespaciales estáticas y dinámicas.
*   `fact_tables`: Contiene las tablas de hechos que registran los eventos de negocio.
*   `data_marts`: Contiene vistas materializadas para análisis.

```mermaid
erDiagram
    direction TB
    fact_tables_fact_trips["fact_tables.fact_trips"] {
        varchar trip_id PK
        timestamp trip_start_timestamp
        timestamp trip_end_timestamp
        varchar pickup_location_id FK
        varchar dropoff_location_id FK
        integer pickup_community_area
        integer dropoff_community_area
        float fare
        varchar batch_id
    }

    fact_tables_fact_traffic["fact_tables.fact_traffic"] {
        varchar record_id PK
        timestamp time
        integer region_id FK
        float speed
        varchar batch_id
    }

    fact_tables_fact_weather["fact_tables.fact_weather"] {
        varchar record_id PK
        timestamp datetime
        varchar station_id FK
        float temp
        float windspeed
        varchar batch_id
    }

    dim_spatial_trips_locations["dim_spatial.trips_locations"] {
        varchar location_id PK
        float longitude
        float latitude
        varchar original_text
    }

    dim_spatial_traffic_regions["dim_spatial.traffic_regions"] {
        integer region_id PK
        varchar region
        varchar geometry_wkt
    }

    dim_spatial_dim_weather_stations["dim_spatial.dim_weather_stations"] {
        varchar station_id PK
        varchar station_name
        float latitude
        float longitude
    }

    dim_spatial_dim_weather_voronoi_zones["dim_spatial.dim_weather_voronoi_zones"] {
        integer zone_id PK
        varchar station_id FK
        varchar geometry_wkt
    }

    %% Relaciones FK existentes
    fact_tables_fact_trips }o--|| dim_spatial_trips_locations : "pickup"
    fact_tables_fact_trips }o--|| dim_spatial_trips_locations : "dropoff"
    fact_tables_fact_traffic }o--|| dim_spatial_traffic_regions : "located in"
    fact_tables_fact_weather }o--|| dim_spatial_dim_weather_stations : "measured at"
    dim_spatial_dim_weather_voronoi_zones }o--|| dim_spatial_dim_weather_stations : "generated from"

    %% NUEVAS Relaciones Espaciales (Point in Polygon)
    dim_spatial_traffic_regions ||--o{ dim_spatial_trips_locations : "spatial join (contains)"
    dim_spatial_dim_weather_voronoi_zones ||--o{ dim_spatial_trips_locations : "spatial join (contains)"

```

## 📜 Descripción de Scripts Principales

A continuación se detallan los scripts más importantes del paquete `src/chicago_rstrips`.

#### `extract_trips_data.py`
*   **Overview:** Extrae datos de viajes de la API de Socrata para un rango de fechas, genera una dimensión de ubicaciones y guarda ambos como archivos Parquet.
*   **Lógica:**
    1.  Construye una query SoQL con el rango de fechas especificado.
    2.  Llama a la función `fetch_data_from_api` para obtener los datos.
    3.  Si se indica (`build_locations=True`), extrae las coordenadas de `pickup` y `dropoff`, las desduplica y crea un DataFrame de dimensión de ubicaciones con un `location_id` único.
    4.  Mapea los `location_id` de vuelta al DataFrame de viajes.
    5.  Guarda el DataFrame de viajes y el de ubicaciones en formato Parquet en el directorio `data/raw/`.

#### `extract_traffic_data.py`
*   **Overview:** Extrae datos de tráfico de la API de Socrata y genera una dimensión de regiones de tráfico.
*   **Lógica:**
    1.  Construye una query SoQL para obtener datos de tráfico por región.
    2.  Obtiene los datos a través de `fetch_data_from_api`.
    3.  Si se indica (`build_regions=True`), construye un GeoDataFrame con los polígonos de cada región de tráfico.
    4.  Guarda los datos de tráfico y la dimensión de regiones en archivos Parquet.

#### `extract_weather_data.py`
*   **Overview:** Extrae datos meteorológicos por hora para un conjunto de estaciones.
*   **Lógica:**
    1.  Obtiene la lista de estaciones meteorológicas desde la base de datos.
    2.  Itera sobre cada estación y llama a la API de Visual Crossing para obtener el historial climático por hora en el rango de fechas especificado.
    3.  Combina los resultados de todas las estaciones en un único DataFrame.
    4.  Guarda el DataFrame resultante en formato Parquet.

#### `create_location_static_features.py`
*   **Overview:** Enriquece los datos de ubicaciones (pickup/dropoff) asignándoles el área comunitaria (community area) a la que pertenecen mediante un join espacial.
*   **Lógica:**
    1.  Carga el archivo de límites geoespaciales de las community areas de Chicago.
    2.  Carga el DataFrame de las ubicaciones de viajes.
    3.  Convierte ambos DataFrames a GeoDataFrames de GeoPandas, asegurando que usen el mismo sistema de coordenadas (CRS).
    4.  Realiza un `spatial join` para determinar a qué community area pertenece cada punto de ubicación.
    5.  Guarda el DataFrame de ubicaciones enriquecido como un archivo Parquet en la carpeta `features/geospatial`.

#### `db_loader.py`
*   **Overview:** Proporciona utilidades para interactuar con la base de datos PostgreSQL.
*   **Lógica:**
    *   `get_engine()`: Crea y devuelve un motor de SQLAlchemy para conectarse a la base de datos usando las credenciales del archivo `.env`.
    *   `run_ddl(ddl_path)`: Lee un archivo `.sql` y lo ejecuta en la base de datos. Se usa para crear tablas y schemas.
    *   `load_parquet_to_postgres(...)`: Lee un archivo Parquet y lo carga en una tabla específica de PostgreSQL, gestionando la creación de la tabla si es necesario.

#### `upsert_fact_tables.sql`
*   **Overview:** Transfiere y transforma datos desde las tablas de `staging` a las tablas de hechos (`fact_tables`).
*   **Lógica:**
    1.  Utiliza una sentencia `INSERT INTO ... SELECT ...` para mover datos de `staging.stg_raw_trips` a `fact_tables.fact_trips`.
    2.  Aplica una cláusula `ON CONFLICT (trip_id) DO UPDATE` para manejar registros duplicados, actualizando los existentes (UPSERT).
    3.  Realiza el mismo proceso para las tablas de tráfico y clima.
    4.  Filtra los registros a procesar según la ventana de tiempo de ejecución del DAG.

## 🧪 Testing

### Local
```bash
# Instalar el paquete en modo editable
uv pip install -e .

# Tests unitarios (rápidos, sin BD)
pytest -m unit -v

# Tests con coverage
pytest -m unit --cov=chicago_rstrips --cov-report=html

# Ver reporte de coverage
open htmlcov/index.html
```

### CI/CD
Los tests se ejecutan automáticamente en GitHub Actions en cada push y pull request.



## 🔗 Enlaces Útiles

*   **Dataset de Viajes (TNP):**
    *   [Portal de Datos Abiertos](https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2025-/6dvr-xwnh/about_data)
    *   [Documentación de la API](https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh)
    *   [Manual de Reporte de TNP](https://chicago.github.io/tnp-reporting-manual/)
*   **Dataset de Tráfico:**
    *   [Documentación de la API](https://dev.socrata.com/foundry/data.cityofchicago.org/kf7e-cur8)
*   **API de Clima (Visual Crossing):**
    *   [Documentación de la API](https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/)
*   **Socrata (Plataforma de Datos Abiertos):**
    *   [Documentación de Queries (SoQL)](https://dev.socrata.com/docs/queries/)