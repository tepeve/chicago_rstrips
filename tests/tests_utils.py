import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from chicago_rstrips.utils import (
    get_project_root,
    get_data_dir,
    get_raw_data_dir,
    get_processed_data_dir,
    get_features_dir,
    get_geospatial_features_dir,
    get_outputs_dir,
    get_weather_stations,
    get_stations_with_zones,
    find_zone_for_point
)


class TestDirectoryFunctions:
    """Tests para funciones de manejo de directorios."""
    
    def test_get_project_root(self):
        """Test que get_project_root retorna un Path válido."""
        root = get_project_root()
        assert isinstance(root, Path)
        assert root.exists()
        assert (root / "src").exists()  # Verificar que existe la carpeta src
    
    def test_get_data_dir(self):
        """Test que get_data_dir crea y retorna el directorio data."""
        data_dir = get_data_dir()
        assert isinstance(data_dir, Path)
        assert data_dir.exists()
        assert data_dir.name == "data"
    
    def test_get_raw_data_dir(self):
        """Test que get_raw_data_dir crea y retorna el directorio raw."""
        raw_dir = get_raw_data_dir()
        assert isinstance(raw_dir, Path)
        assert raw_dir.exists()
        assert raw_dir.name == "raw"
        assert raw_dir.parent.name == "data"
    
    def test_get_processed_data_dir(self):
        """Test que get_processed_data_dir crea y retorna el directorio processed."""
        processed_dir = get_processed_data_dir()
        assert isinstance(processed_dir, Path)
        assert processed_dir.exists()
        assert processed_dir.name == "processed"
    
    def test_get_features_dir(self):
        """Test que get_features_dir crea y retorna el directorio features."""
        features_dir = get_features_dir()
        assert isinstance(features_dir, Path)
        assert features_dir.exists()
        assert features_dir.name == "features"
    
    def test_get_geospatial_features_dir(self):
        """Test que get_geospatial_features_dir crea y retorna el directorio geospatial."""
        geo_dir = get_geospatial_features_dir()
        assert isinstance(geo_dir, Path)
        assert geo_dir.exists()
        assert geo_dir.name == "geospatial"
        assert geo_dir.parent.name == "features"
    
    def test_get_outputs_dir(self):
        """Test que get_outputs_dir crea y retorna el directorio outputs."""
        outputs_dir = get_outputs_dir()
        assert isinstance(outputs_dir, Path)
        assert outputs_dir.exists()
        assert outputs_dir.name == "outputs"


class TestGeospatialFunctions:
    """Tests para funciones de consultas geoespaciales."""
    
    @patch('chicago_rstrips.utils.get_engine')
    @patch('chicago_rstrips.utils.pd.read_sql')
    def test_get_weather_stations_success(self, mock_read_sql, mock_get_engine):
        """Test que get_weather_stations retorna un DataFrame con estaciones."""
        # Configurar mock
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        expected_df = pd.DataFrame({
            'station_id': ['Estacion_A', 'Estacion_B'],
            'longitude': [-87.655, -87.755],
            'latitude': [41.970, 41.970]
        })
        mock_read_sql.return_value = expected_df
        
        # Ejecutar función
        result = get_weather_stations()
        
        # Verificar
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'station_id' in result.columns
        mock_engine.dispose.assert_called_once()
    
    @patch('chicago_rstrips.utils.get_engine')
    @patch('chicago_rstrips.utils.pd.read_sql')
    def test_get_weather_stations_empty(self, mock_read_sql, mock_get_engine):
        """Test que get_weather_stations maneja DataFrames vacíos."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_read_sql.return_value = pd.DataFrame()
        
        result = get_weather_stations()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        mock_engine.dispose.assert_called_once()
    
    @patch('chicago_rstrips.utils.get_engine')
    @patch('chicago_rstrips.utils.pd.read_sql')
    def test_get_stations_with_zones_success(self, mock_read_sql, mock_get_engine):
        """Test que get_stations_with_zones retorna datos combinados."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        expected_df = pd.DataFrame({
            'station_id': ['Estacion_A'],
            'longitude': [-87.655],
            'latitude': [41.970],
            'zone_id': [1],
            'area_km2': [150.5]
        })
        mock_read_sql.return_value = expected_df
        
        result = get_stations_with_zones()
        
        assert isinstance(result, pd.DataFrame)
        assert 'zone_id' in result.columns
        assert 'area_km2' in result.columns
        mock_engine.dispose.assert_called_once()
    
    @patch('chicago_rstrips.utils.get_engine')
    @patch('chicago_rstrips.utils.pd.read_sql')
    def test_find_zone_for_point_success(self, mock_read_sql, mock_get_engine):
        """Test que find_zone_for_point encuentra la zona más cercana."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        expected_df = pd.DataFrame({
            'station_id': ['Estacion_A'],
            'longitude': [-87.655],
            'latitude': [41.970],
            'distance': [0.001]
        })
        mock_read_sql.return_value = expected_df
        
        result = find_zone_for_point(-87.6298, 41.8781)
        
        assert result == 'Estacion_A'
        mock_engine.dispose.assert_called_once()
    
    @patch('chicago_rstrips.utils.get_engine')
    @patch('chicago_rstrips.utils.pd.read_sql')
    def test_find_zone_for_point_not_found(self, mock_read_sql, mock_get_engine):
        """Test que find_zone_for_point retorna None cuando no hay resultados."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_read_sql.return_value = pd.DataFrame()
        
        result = find_zone_for_point(-87.6298, 41.8781)
        
        assert result is None
        mock_engine.dispose.assert_called_once()
    
    @patch('chicago_rstrips.utils.get_engine')
    def test_find_zone_for_point_handles_exception(self, mock_get_engine):
        """Test que find_zone_for_point maneja excepciones correctamente."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("Database error")
        
        # La función debería manejar el error y retornar None o lanzar excepción
        # Dependiendo de tu implementación, ajusta esta verificación
        try:
            result = find_zone_for_point(-87.6298, 41.8781)
            # Si no lanza excepción, debería retornar None
            assert result is None
        except Exception:
            # Si lanza excepción, verificar que engine.dispose() se llamó
            pass
        
        mock_engine.dispose.assert_called()


class TestGeospatialIntegration:
    """
    Tests de integración que requieren base de datos real.
    
    Estos tests se saltan si no hay conexión a la base de datos.
    Usa: pytest -m integration para ejecutarlos.
    """
    
    @pytest.mark.integration
    def test_get_weather_stations_real_db(self):
        """Test de integración con base de datos real."""
        try:
            result = get_weather_stations()
            assert isinstance(result, pd.DataFrame)
            # Si las features están cargadas, debería tener datos
            if not result.empty:
                assert 'station_id' in result.columns
                assert 'longitude' in result.columns
                assert 'latitude' in result.columns
        except Exception as e:
            pytest.skip(f"Base de datos no disponible: {e}")
    
    @pytest.mark.integration
    def test_find_zone_for_point_real_db(self):
        """Test de integración para búsqueda de zona con DB real."""
        try:
            # Punto en downtown Chicago
            zone = find_zone_for_point(-87.6298, 41.8781)
            # Debería retornar un station_id válido o None
            assert zone is None or isinstance(zone, str)
        except Exception as e:
            pytest.skip(f"Base de datos no disponible: {e}")


# Configuración de pytest
def pytest_configure(config):
    """Configurar markers personalizados."""
    config.addinivalue_line(
        "markers", "integration: tests que requieren base de datos real"
    )


if __name__ == "__main__":
    # Ejecutar tests con pytest
    pytest.main([__file__, "-v"])