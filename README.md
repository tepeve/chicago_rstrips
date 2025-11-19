# Chicago Ridesharing Trips ETL
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

## 📁 Estructura del Repositorio
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
    El manejo de entornos y dependencias está gestionado con uv. Crea un archivo `.env` en la raíz del proyecto. Tambíen podés crearlo desde cero pero vas a necesitar definir estas variables para desplegar el sistema:
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

    # Configuración del webserver de airflow
    AIRFLOW__WEBSERVER__SECRET_KEY=
    ```
    > **Nota:** Las credenciales de la base de datos deben coincidir con las definidas en `docker-compose.yml` para que Airflow pueda conectarse. Podés generar tu `AIRFLOW__WEBSERVER__SECRET_KEY` propia corriendo la siguiente línea y guardando su resultado en el .env. 
    ```bash
    openssl rand -hex 32
    ```

3.  **Desplegar el proyecto:**
    Con este comando podés construir las imágenes y levantar todos los servicios (Airflow, Postgres, etc.) en una línea. 

    ```bash
    make run-project
    ```

4.  **Acceder a la UI de Airflow:**
    Abrí tu navegador y andá a `http://localhost:8080`. El usuario y contraseña por defecto son `admin`.

5.  ⚠️**Cómo ejecutar los DAGs:**
    *   **Cold Start:** Primero, activá y ejecutá manualmente el DAG `coldstart_etl_pipeline`. Este proceso inicializa la base de datos, carga todas las tablas y realiza unaprimera ingesta de datos históricos.
    *   **Batch Incremental:** Una vez que el `coldstart_etl_pipeline` haya finalizado con éxito, activá el DAG `batch_etl_pipeline`. Este se ejecutará diariamente (`@daily`) para procesar los nuevos datos de forma incremental.

## 🧬 Arquitectura y Flujo de Datos

El sistema se basa en dos DAGs de Airflow principales que orquestan todo el flujo ELT. 

### Diagrama de Flujo de los DAGs (ejemplo: coldstart_etl_pipeline)

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
        MAP_LOCS{{"locations_mapped"}}
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
        ML_TABLE{{"ml_base_table"}}
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
    FACT_TRIPS --> ML_TABLE
    FACT_TRAFFIC --> ML_TABLE
    FACT_WEATHER --> ML_TABLE
    LOAD_STATIC --> EXT_WEATHER
    LOAD_STATIC -.-> MAP_LOCS
    LOAD_LOCS -.-> MAP_LOCS
    LOAD_REGIONS -.-> MAP_LOCS
    MAP_LOCS -.-> ML_TABLE

     DDL:::setup
     GEN_STATIC:::extract
     LOAD_STATIC:::db
     LOAD_REGIONS:::db
     LOAD_LOCS:::db
     MAP_LOCS:::setup
     EXT_TRIPS:::extract
     EXT_TRAFFIC:::extract
     EXT_WEATHER:::extract
     LOAD_TRIPS:::db
     LOAD_TRAFFIC:::db
     LOAD_WEATHER:::db
     FACT_TRIPS:::db
     FACT_TRAFFIC:::db
     FACT_WEATHER:::db
     ML_TABLE:::setup
     VERIFY:::check
    classDef setup fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef transform fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef extract fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef db fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
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

### Zoom-in a cómo se vinculan las facts tables de trips, traffic y weather:
Este modelo de asociación espacial se aplica en el script `join_spatial_dims.py` y se materializa en la tabla `dim_spatial.mapped_locations`. La dimensión temporal de la relación entre estas entidades se realiza en la vista materializada `datamarts.fact_trips_with_traffic_weather`.

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

## 🗃️ Visualización de la Base de Datos con Adminer

Para facilitar la exploración y validación de los datos, el `docker-compose` incluye **Adminer**, una herramienta ligera de gestión de bases de datos.

1.  **Acceder a Adminer:**
    Una vez que los contenedores estén corriendo, abrí tu navegador y andá a `http://localhost:8081`.

2.  **Iniciar Sesión:**
    Usá las siguientes credenciales para conectarte al Data Warehouse. Estos valores deben coincidir con los que definiste en tu archivo `.env`.

    *   **System:** `PostgreSQL`
    *   **Server:** `postgres_local_db` (es el nombre del servicio en `docker-compose.yml`)
    *   **Username:** El valor de `POSTGRES_LOCAL_USER`
    *   **Password:** El valor de `POSTGRES_LOCAL_PASSWORD`
    *   **Database:** El valor de `POSTGRES_LOCAL_DB`

    Una vez dentro, podrás navegar por los schemas (`staging`, `dim_spatial`, `fact_tables`, `data_marts`), ver el contenido de las tablas y ejecutar consultas SQL directamente desde la interfaz web.

## 📜 Descripción de Scripts Principales

A continuación se detallan los scripts más importantes del paquete `src/chicago_rstrips`.

#### `extract_trips_data.py`
*   **Overview:** Extrae datos de viajes de la API de Socrata para un rango de fechas, genera una dimensión de ubicaciones y guarda ambos como archivos Parquet.
*   **Lógica:**
    *  Construye una query SoQL con el rango de fechas especificado.
    *  Llama a la función `fetch_data_from_api` para obtener los datos.
    *  Si se indica (`build_locations=True`), extrae las coordenadas de `pickup` y `dropoff`, las desduplica y crea un DataFrame de dimensión de ubicaciones con un `location_id` único.
    *  Mapea los `location_id` de vuelta al DataFrame de viajes.
    *  Guarda el DataFrame de viajes y el de ubicaciones en formato Parquet en el directorio `data/raw/`.

#### `extract_traffic_data.py`
*   **Overview:** Extrae datos de tráfico de la API de Socrata y genera una dimensión de regiones de tráfico.
*   **Lógica:**
    *  Construye una query SoQL para obtener datos de tráfico por región.
    *  Obtiene los datos a través de `fetch_data_from_api`.
    *  Si se indica (`build_regions=True`), construye un GeoDataFrame con los polígonos de cada región de tráfico.
    *  Guarda los datos de tráfico y la dimensión de regiones en archivos Parquet.

#### `extract_weather_data.py`
*   **Overview:** Extrae datos meteorológicos por hora para un conjunto de estaciones.
*   **Lógica:**
    *  Obtiene la lista de estaciones meteorológicas desde la base de datos.
    *  Itera sobre cada estación y llama a la API de Visual Crossing para obtener el historial climático por hora en el rango de fechas especificado.
    *  Combina los resultados de todas las estaciones en un único DataFrame.
    *  Guarda el DataFrame resultante en formato Parquet.

#### `create_location_static_features.py`
*   **Overview:** Enriquece los datos de ubicaciones (pickup/dropoff) asignándoles el área comunitaria (community area) a la que pertenecen mediante un join espacial.
*   **Lógica:**
    *  Carga el archivo de polígonos con los límites de la Ciudad de Chicago.
    *  Definimos a mano cuatro ubicaciones de la Ciudad, según su cercanía al Lago Michigan y su latitud Norte-Sur que utilizaremos como estaciones meteorológicas.
    *  Tomamos esas ubicaciones como centroides para trazar un diagrama de Voronoi, que telesa en cuatro areas el plano de la Ciudad.
    *  Guardamos los DataFrames de ubicaciones como un archivo Parquet en la carpeta `features/geospatial`.

#### `db_loader.py`
*   **Overview:** Proporciona utilidades para interactuar con la base de datos PostgreSQL.
*   **Lógica:**
    *   `get_engine()`: Crea y devuelve un motor de SQLAlchemy para conectarse a la base de datos usando las credenciales del archivo `.env`.
    *   `run_ddl(ddl_path)`: Lee un archivo `.sql` y lo ejecuta en la base de datos. Se usa para crear tablas y schemas.
    *   `load_dataframe_to_postgres(...)`: Carga un DataFrame en una tabla de PostgreSQL. Si el modo es `append`, inspecciona la tabla de destino y carga solo las columnas que coinciden para evitar errores. Si es `replace`, elimina y vuelve a crear la tabla.    
    *   `load_parquet_to_postgres(...)`: Lee un archivo Parquet y lo carga en una tabla específica de PostgreSQL, gestionando la creación de la tabla si es necesario.

#### `join_spatial_dims.py`
*   **Overview:** Crea una tabla de mapeo (`dim_spatial.mapped_locations`) que vincula cada ubicación de viaje con su región de tráfico y zona meteorológica correspondiente.
*   **Lógica:**
    1.  Obtiene las ubicaciones de los viajes, los polígonos de las regiones de tráfico y las zonas de Voronoi de las estaciones meteorológicas desde la base de datos.
    2.  Realiza uniones espaciales (`spatial join`) para determinar qué región de tráfico y qué zona de Voronoi contiene cada punto de ubicación de viaje.
    3.  Guarda el resultado (un mapeo de `location_id` a `region_id` y `station_id`) en la tabla `dim_spatial.mapped_locations`.

#### `upsert_fact_tables.sql`
*   **Overview:** Transfiere y transforma datos desde las tablas de `staging` a las tablas de hechos (`fact_tables`).
*   **Lógica:**
    *  Utiliza una sentencia `INSERT INTO ... SELECT ...` para mover datos de `staging.stg_raw_trips` a `fact_tables.fact_trips`.
    *  Aplica una cláusula `ON CONFLICT (trip_id) DO UPDATE` para manejar registros duplicados, actualizando los existentes (UPSERT).
    *  Realiza el mismo proceso para las tablas de tráfico y clima.
    *  Filtra los registros a procesar según la ventana de tiempo de ejecución del DAG.

#### `create_data_marts.sql`
*   **Overview:** Crea vistas materializadas que integran las tablas tablas de hechos (`fact_tables`) para análisis futuros.
*   **Lógica:**
    *  Crea la vista materializada `dm_trips_hourly_pickup_stats` que agrega los viajes por hora y área de recogida para obtener estadísticas.
    *  Crea la vista materializada `fact_trips_with_traffic_weather` que enriquece cada viaje con los datos de tráfico y clima más cercanos en el tiempo.
    *  Crea una tabla base (`ml_base_table`) que integra las vistas anteriores y calcula características adicionales (features) usando funciones de ventana.

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

*   **Dataset de Viajes (TNP) de la Ciudad de Chicago:**
    *   [Portal de Datos Abiertos](https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2025-/6dvr-xwnh/about_data)
    *   [Documentación de la API](https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh)
    *   [Manual de Reporte de TNP](https://chicago.github.io/tnp-reporting-manual/)
*   **Dataset de Tráfico de la Ciudad de Chicago:**
    *   [Documentación de la API](https://dev.socrata.com/foundry/data.cityofchicago.org/kf7e-cur8)
*   **API de Clima (Visual Crossing):**
    *   [Documentación de la API](https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/)
*   **Socrata (Plataforma de Datos Abiertos):**
    *   [Documentación de Queries (SoQL)](https://dev.socrata.com/docs/queries/)
