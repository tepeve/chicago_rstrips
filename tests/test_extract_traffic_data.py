import pytest
import pandas as pd
import pandas.testing
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

# CORRECCIÓN: Importar desde utils, no desde extract_traffic_data
from chicago_rstrips.utils import transform_dataframe_types
from chicago_rstrips.extract_traffic_data import extract_traffic_data
from chicago_rstrips.utils import get_raw_data_dir

class TestTransformTrafficDataTypes:
    """Tests para la función transform_dataframe_types aplicada a datos de tráfico."""

    @pytest.fixture
    def sample_type_mapping(self):
        """Mapeo de tipos para datos de tráfico."""
        return {
            'time': 'datetime64[ns]',
            'region_id': 'Int64',
            'bus_count': 'Int64',
            'num_reads': 'Int64',
            'region': 'string',
            'record_id': 'string',
            'speed': 'float64',
            'batch_id': 'string' 
        }

    @pytest.fixture
    def sample_raw_data(self):
        """Datos crudos de tráfico simulando una respuesta de API."""
        return pd.DataFrame({
            'time': ['2025-09-01T10:00:00', '2025-09-01T10:00:00'],
            'region_id': ['1', '2'],
            'bus_count': ['10', '5'],
            'num_reads': ['100', '50'],
            'region': ['Loop', 'Near North Side'],
            'record_id': ['rec1', 'rec2'],
            'speed': ['25.5', '30.0'],
            'batch_id': ['test_batch_001', 'test_batch_001']  
        })

    @pytest.fixture
    def expected_transformed_data(self):
        """El DataFrame esperado después de la transformación."""
        return pd.DataFrame({
            'time': pd.to_datetime(['2025-09-01T10:00:00', '2025-09-01T10:00:00']),
            'region_id': pd.Series([1, 2], dtype='Int64'),
            'bus_count': pd.Series([10, 5], dtype='Int64'),
            'num_reads': pd.Series([100, 50], dtype='Int64'),
            'region': pd.Series(['Loop', 'Near North Side'], dtype='string'),
            'record_id': pd.Series(['rec1', 'rec2'], dtype='string'),
            'speed': pd.Series([25.5, 30.0], dtype='float64'),
            'batch_id': pd.Series(['test_batch_001', 'test_batch_001'], dtype='string')  # AGREGADO
        })

    @pytest.fixture
    def sample_malformed_data(self):
        """Datos con valores malformados."""
        return pd.DataFrame({
            'time': ['not-a-date', '2025-09-01T10:00:00'],
            'region_id': ['abc', '2'],
            'bus_count': ['10.5', '5'],
            'num_reads': ['100', '50'],
            'region': ['Loop', 'Near North Side'],
            'record_id': ['rec1', 'rec2'],
            'speed': ['-','30.0'],
            'batch_id': ['test_batch_001', 'test_batch_001'] 
        })

    @pytest.mark.unit
    def test_correct_data_types_assignment(self, sample_raw_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        for col, dtype in sample_type_mapping.items():
            assert str(df_transformed[col].dtype) == dtype

    @pytest.mark.unit
    def test_no_data_loss_during_transformation(self, sample_raw_data, sample_type_mapping):
        original_count = len(sample_raw_data)
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        assert len(df_transformed) == original_count

    @pytest.mark.unit
    def test_all_columns_preserved(self, sample_raw_data, sample_type_mapping):
        original_columns = set(sample_raw_data.columns)
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        assert set(df_transformed.columns) == original_columns

    @pytest.mark.unit
    def test_transformation_correctness(self, sample_raw_data, expected_transformed_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        pd.testing.assert_frame_equal(df_transformed, expected_transformed_data)

    @pytest.mark.unit
    def test_malformed_data_handling(self, sample_malformed_data, sample_type_mapping):
        df_transformed = transform_dataframe_types(sample_malformed_data.copy(), sample_type_mapping)
        assert pd.isna(df_transformed['time'].iloc[0])
        assert pd.isna(df_transformed['region_id'].iloc[0])
        assert pd.isna(df_transformed['speed'].iloc[0])

class TestExtractTrafficData:
    """Tests para la función extract_traffic_data completa."""

    @pytest.fixture
    def temp_output_dir(self):
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_api_response(self):
        return pd.DataFrame({
            'time': ['2025-09-01T10:00:00'],
            'region_id': ['1'],
            'speed': ['25.5'],
            'region': ['Loop'],
            'bus_count': ['10'],
            'num_reads': ['100'],
            'record_id': ['rec1'],
            'west': ['-87.7'],
            'east': ['-87.6'],
            'north': ['41.9'],
            'south': ['41.8'],
            'hour': ['10']  
        })

    @pytest.mark.unit
    def test_parquet_preserves_data_types(self, mock_api_response, temp_output_dir, monkeypatch):
        def mock_fetch(*args, **kwargs):
            return mock_api_response
        
        def mock_get_dir():
            return temp_output_dir

        monkeypatch.setattr('chicago_rstrips.extract_traffic_data.fetch_data_from_api', mock_fetch)
        monkeypatch.setattr('chicago_rstrips.extract_traffic_data.get_raw_data_dir', mock_get_dir)

        output_path = extract_traffic_data(
            output_filename="test_traffic.parquet",
            build_regions=False, 
            batch_id="test_batch_001"
        )
        df_from_parquet = pd.read_parquet(output_path)

        # Verificar tipos (Parquet convierte Int64 a int64)
        assert df_from_parquet['region_id'].dtype in ['int64', 'Int64']
        assert pd.api.types.is_datetime64_any_dtype(df_from_parquet['time'])
        assert df_from_parquet['speed'].dtype == 'float64'
        assert len(df_from_parquet) == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])