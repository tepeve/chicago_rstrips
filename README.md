# chicago-rstrips

[![Tests](https://github.com/tu-usuario/chicago_rstrips/workflows/Tests/badge.svg)](https://github.com/tu-usuario/chicago_rstrips/actions)
[![codecov](https://codecov.io/gh/tu-usuario/chicago_rstrips/branch/main/graph/badge.svg)](https://codecov.io/gh/tu-usuario/chicago_rstrips)

Proyecto ETL para extraer trips desde Socrata y almacenarlos en bases postgre local (dev).

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

## 📁 Estructura del Proyecto
```
chicago_rstrips/
├── src/chicago_rstrips/    # Código fuente
├── tests/                   # Tests unitarios e integración
├── dags/                    # DAGs de Airflow
├── sql/                     # Scripts SQL
└── data/                    # Datos locales (no versionado)
```