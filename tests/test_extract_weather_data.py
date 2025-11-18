import pytest
import pandas as pd
import pandas.testing
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

# CORRECCIÓN: Importar desde utils
from chicago_rstrips.utils import transform_dataframe_types
from chicago_rstrips.extract_weather_data import extract_weather_data

class TestTransformWeatherDataTypes:
    """Tests para la función transform_dataframe_types aplicada a datos de clima."""

    @pytest.fixture
    def sample_type_mapping(self):
        """Mapeo de tipos para datos de clima."""
        return {
            "record_id": 'string',
            "datetime": 'datetime64[ns]',
            "station_id": 'string',
            "temp": 'float64',
            "feelslike": 'float64',
            "precipprob": 'float64',
            "windspeed": 'float64',
            "winddir": 'string',
            "conditions": 'string',
            "batch_id": 'string'  
        }

    @pytest.fixture
    def sample_raw_data(self):
        """Datos crudos de clima simulando una respuesta de API."""
        return pd.DataFrame({
            'record_id': ['A_20250901', 'B_20250901'],
            'datetime': ['2025-09-01', '2025-09-01'],
            'station_id': ['Estacion_A', 'Estacion_B'],
            'temp': ['22.5', '21.0'],
            'feelslike': ['23.0', '20.5'],
            'precipprob': ['10.0', '5.0'],
            'windspeed': ['15.0', '12.0'],
            'winddir': ['180', '270'],
            'conditions': ['Partially cloudy', 'Clear'],
            'batch_id': ['test_batch_001', 'test_batch_002']  
        })

    @pytest.fixture
    def expected_transformed_data(self):
        """El DataFrame esperado después de la transformación."""
        return pd.DataFrame({
            'record_id': pd.Series(['A_20250901', 'B_20250901'], dtype='string'),
            'datetime': pd.to_datetime(['2025-09-01', '2025-09-01']),
            'station_id': pd.Series(['Estacion_A', 'Estacion_B'], dtype='string'),
            'temp': pd.Series([22.5, 21.0], dtype='float64'),
            'feelslike': pd.Series([23.0, 20.5], dtype='float64'),
            'precipprob': pd.Series([10.0, 5.0], dtype='float64'),
            'windspeed': pd.Series([15.0, 12.0], dtype='float64'),
            'winddir': pd.Series(['180', '270'], dtype='string'),
            'conditions': pd.Series(['Partially cloudy', 'Clear'], dtype='string'),
            'batch_id': pd.Series(['test_batch_001', 'test_batch_002'], dtype='string')  # AGREGADO
        })

    @pytest.fixture
    def sample_malformed_data(self):
        """Datos con valores malformados."""
        return pd.DataFrame({
            'record_id': ['A_20250901', 'B_20250901'],
            'datetime': ['not-a-date', '2025-09-01'],
            'station_id': ['Estacion_A', 'Estacion_B'],
            'temp': ['high', '21.0'],
            'feelslike': ['23.0', '20.5'],
            'precipprob': ['low', '5.0'],
            'windspeed': ['fast', '12.0'],
            'winddir': ['180', '270'],
            'conditions': ['Partially cloudy', 'Clear'],
            'batch_id': ['test_batch_001', 'test_batch_002']  
        })

    @pytest.mark.unit
    def test_correct_data_types_assignment(self, sample_raw_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        for col, dtype in sample_type_mapping.items():
            assert str(df_transformed[col].dtype) == dtype

    @pytest.mark.unit
    def test_transformation_correctness(self, sample_raw_data, expected_transformed_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        pd.testing.assert_frame_equal(df_transformed, expected_transformed_data)

    @pytest.mark.unit
    def test_malformed_data_handling(self, sample_malformed_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_malformed_data.copy(), sample_type_mapping)
        assert pd.isna(df_transformed['datetime'].iloc[0])
        assert pd.isna(df_transformed['temp'].iloc[0])
        assert pd.isna(df_transformed['precipprob'].iloc[0])
        assert pd.isna(df_transformed['windspeed'].iloc[0])

class TestExtractWeatherData:
    """Tests para la función extract_weather_data completa."""

    @pytest.fixture
    def temp_output_dir(self):
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_api_response(self):
        return [{
            'datetime': '2025-09-01',
            'temp': 22.5,
            'feelslike': 23.0,    
            'precipprob': 10.0,     
            'windspeed': 15.0,       
            'winddir': '180',        
            'conditions': 'Clear'
        }]

    @pytest.fixture
    def mock_stations_response(self):
        return pd.DataFrame({
            'station_id': ['Estacion_A'],
            'longitude': [-87.655],
            'latitude': [41.970]
        })

    @pytest.mark.unit
    def test_parquet_preserves_data_types(self, mock_api_response, mock_stations_response, temp_output_dir, monkeypatch):
        def mock_fetch(*args, **kwargs):
            return {'days': mock_api_response}
        
        def mock_get_stations(*args, **kwargs):
            return mock_stations_response

        def mock_get_dir():
            return temp_output_dir

        monkeypatch.setattr('chicago_rstrips.extract_weather_data.fetch_weather_api', mock_fetch)
        monkeypatch.setattr('chicago_rstrips.extract_weather_data.get_weather_stations', mock_get_stations)
        monkeypatch.setattr('chicago_rstrips.extract_weather_data.get_raw_data_dir', mock_get_dir)

        output_path = extract_weather_data(
            output_filename="test_weather.parquet",
            start_timestamp="2025-09-01",
            end_timestamp="2025-09-01",
            batch_id="test_batch_001"
        )
        df_from_parquet = pd.read_parquet(output_path)

        assert pd.api.types.is_datetime64_any_dtype(df_from_parquet['datetime'])
        assert df_from_parquet['temp'].dtype == 'float64'
        assert 'station_id' in df_from_parquet.columns
        assert len(df_from_parquet) >= 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])