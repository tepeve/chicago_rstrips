import pytest
import pandas as pd
import pandas.testing  # Import clave para assert_frame_equal
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

# Asumimos que estas funciones están en el módulo importado
from chicago_rstrips.extract_trips_data import  extract_trips_data
from chicago_rstrips.utils import transform_dataframe_types



class TestTransformDataTypes:
    """Tests para la función transform_dataframe_types"""

    @pytest.fixture
    def sample_type_mapping(self):
        """
        Simula datos de type_mapping
        """
        return {
            'trip_id': 'string',
            'trip_start_timestamp': 'datetime64[ns]',
            'trip_end_timestamp': 'datetime64[ns]',
            'trip_seconds': 'Int64', 
            'trip_miles': 'float64',
            'percent_time_chicago': 'float64',
            'percent_distance_chicago': 'float64',
            'pickup_community_area': 'Int64',
            'dropoff_community_area': 'Int64',
            'fare': 'float64',
            'tip': 'float64',
            'additional_charges': 'float64',
            'trip_total': 'float64',
            'shared_trip_authorized': 'boolean',
            'trips_pooled': 'Int64',
            'pickup_centroid_location': 'string',
            'dropoff_centroid_location': 'string',
            'batch_id': 'string'
        }

    @pytest.fixture
    def sample_raw_data(self):
        """
        Simula datos como vienen de la API Socrata (todos strings)
        """
        return pd.DataFrame({
            'trip_id': ['trip_001', 'trip_002', 'trip_003'],
            'trip_start_timestamp': ['2025-01-15T10:30:00', '2025-01-15T11:00:00', '2025-01-15T12:15:00'],
            'trip_end_timestamp': ['2025-01-15T10:45:00', '2025-01-15T11:20:00', '2025-01-15T12:30:00'],
            'trip_seconds': ['900', '1200', '900'],
            'trip_miles': ['2.5', '3.8', '1.2'],
            'percent_time_chicago': ['1.0', '0.95', '1.0'],
            'percent_distance_chicago': ['1.0', '0.98', '1.0'],
            'pickup_community_area': ['32', '8', '28'],
            'dropoff_community_area': ['8', '32', '6'],
            'fare': ['12.50', '15.75', '8.25'],
            'tip': ['2.50', '3.00', '1.50'],
            'additional_charges': ['0.50', '1.00', '0.25'],
            'trip_total': ['15.50', '19.75', '10.00'],
            'shared_trip_authorized': ['true', 'false', 'true'],
            'trips_pooled': ['1', '1', '2'],
            'batch_id': ['test_batch_001', 'test_batch_002', 'test_batch_003'],
            'pickup_centroid_location': ['{"type":"Point","coordinates":[-87.6298,41.8781]}', 
                                         '{"type":"Point","coordinates":[-87.6500,41.9000]}',
                                         '{"type":"Point","coordinates":[-87.6100,41.8500]}'],
            'dropoff_centroid_location': ['{"type":"Point","coordinates":[-87.6400,41.8900]}',
                                          '{"type":"Point","coordinates":[-87.6298,41.8781]}',
                                          '{"type":"Point","coordinates":[-87.6200,41.8600]}'],
        })

    @pytest.fixture
    def sample_data_with_nulls(self):
        """
        Simula datos con valores nulos/vacíos para probar el manejo de nullables
        """
        return pd.DataFrame({
            'trip_id': ['trip_001', 'trip_002', 'trip_003'],
            'trip_start_timestamp': ['2025-01-15T10:30:00', '2025-01-15T11:00:00', None],
            'trip_end_timestamp': ['2025-01-15T10:45:00', None, '2025-01-15T12:30:00'],
            'trip_seconds': ['900', None, '900'],
            'trip_miles': ['2.5', '3.8', None],
            'percent_time_chicago': ['1.0', None, '1.0'],
            'percent_distance_chicago': ['1.0', '0.98', None],
            'pickup_community_area': ['32', None, '28'],
            'dropoff_community_area': [None, '32', '6'],
            'fare': ['12.50', None, '8.25'],
            'tip': [None, '3.00', '1.50'],
            'additional_charges': ['0.50', None, '0.25'],
            'trip_total': ['15.50', '19.75', None],
            'shared_trip_authorized': ['true', 'false', None],
            'trips_pooled': ['1', None, '2'],
            'batch_id': ['test_batch_001', 'test_batch_002', None],
            'pickup_centroid_location': ['{"type":"Point","coordinates":[-87.6298,41.8781]}', None, None],
            'dropoff_centroid_location': [None, '{"type":"Point","coordinates":[-87.6298,41.8781]}', None]
        })

    @pytest.fixture
    def sample_malformed_data(self):
        """
        NUEVA FIXTURE: Simula datos con valores malformados (camino triste)
        """
        return pd.DataFrame({
            'trip_id': ['trip_malformed', 'trip_good'],
            'trip_start_timestamp': ['not-a-date', '2025-01-15T11:00:00'],
            'trip_end_timestamp': ['2025-01-15T10:45:00', '2025-01-15T11:20:00'],
            'trip_seconds': ['abc', '1200'],
            'trip_miles': ['2.5', '3.8'],
            'percent_time_chicago': ['1.0', '0.95'],
            'percent_distance_chicago': ['1.0', '0.98'],
            'pickup_community_area': ['32', '8'],
            'dropoff_community_area': ['8', '32'],
            'fare': ['12.xx', '15.75'],
            'tip': ['2.50', '3.00'],
            'additional_charges': ['0.50', '1.00'],
            'trip_total': ['15.50', '19.75'],
            'shared_trip_authorized': ['maybe', 'false'],
            'trips_pooled': ['1', '1'],
            'pickup_centroid_location': [None, None],
            'dropoff_centroid_location': [None, None],
            'batch_id': ['test_batch_0e', 'test_batch']
        })

    @pytest.fixture
    def expected_transformed_data(self):
        """El DataFrame esperado después de la transformación."""
        return pd.DataFrame({
            'trip_id': pd.Series(['trip_001', 'trip_002', 'trip_003'], dtype='string'),
            'trip_start_timestamp': pd.to_datetime(['2025-01-15T10:30:00', '2025-01-15T11:00:00', '2025-01-15T12:15:00']),
            'trip_end_timestamp': pd.to_datetime(['2025-01-15T10:45:00', '2025-01-15T11:20:00', '2025-01-15T12:30:00']),
            'trip_seconds': pd.Series([900, 1200, 900], dtype='Int64'),
            'trip_miles': pd.Series([2.5, 3.8, 1.2], dtype='float64'),
            'percent_time_chicago': pd.Series([1.0, 0.95, 1.0], dtype='float64'),
            'percent_distance_chicago': pd.Series([1.0, 0.98, 1.0], dtype='float64'),
            'pickup_community_area': pd.Series([32, 8, 28], dtype='Int64'),
            'dropoff_community_area': pd.Series([8, 32, 6], dtype='Int64'),
            'fare': pd.Series([12.50, 15.75, 8.25], dtype='float64'),
            'tip': pd.Series([2.50, 3.00, 1.50], dtype='float64'),
            'additional_charges': pd.Series([0.50, 1.00, 0.25], dtype='float64'),
            'trip_total': pd.Series([15.50, 19.75, 10.00], dtype='float64'),
            'shared_trip_authorized': pd.Series([True, False, True], dtype='boolean'),
            'trips_pooled': pd.Series([1, 1, 2], dtype='Int64'),
            'batch_id': pd.Series(['test_batch_001', 'test_batch_002', 'test_batch_003'], dtype='string'),  # ✅ MOVER AQUÍ
            'pickup_centroid_location': pd.Series([
                '{"type":"Point","coordinates":[-87.6298,41.8781]}',
                '{"type":"Point","coordinates":[-87.6500,41.9000]}',
                '{"type":"Point","coordinates":[-87.6100,41.8500]}'
            ], dtype='string'),
            'dropoff_centroid_location': pd.Series([
                '{"type":"Point","coordinates":[-87.6400,41.8900]}',
                '{"type":"Point","coordinates":[-87.6298,41.8781]}',
                '{"type":"Point","coordinates":[-87.6200,41.8600]}'
            ], dtype='string')
        })

    @pytest.mark.unit
    def test_correct_data_types_assignment(self, sample_raw_data,sample_type_mapping):
        """
        Test 1: Verificar que todos los tipos de datos se asignen
        correctamente usando un diccionario de esquema.
        """
        expected_schema = {
            'trip_id': 'string',
            'pickup_centroid_location': 'string',
            'dropoff_centroid_location': 'string',
            'trip_start_timestamp': 'datetime64[ns]',
            'trip_end_timestamp': 'datetime64[ns]',
            'trip_seconds': 'Int64',  
            'pickup_community_area': 'Int64',
            'dropoff_community_area': 'Int64',
            'trips_pooled': 'Int64',
            'trip_miles': 'float64',
            'percent_time_chicago': 'float64',
            'percent_distance_chicago': 'float64',
            'fare': 'float64',
            'tip': 'float64',
            'additional_charges': 'float64',
            'trip_total': 'float64',
            'shared_trip_authorized': 'boolean',
            'batch_id': 'string'
        }
        
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        
        for col_name, expected_type in expected_schema.items():
            if expected_type == 'datetime64[ns]':
                assert pd.api.types.is_datetime64_any_dtype(df_transformed[col_name]), \
                    f"Columna '{col_name}' debería ser datetime"
            else:
                assert df_transformed[col_name].dtype == expected_type, \
                    f"Columna '{col_name}' debería ser {expected_type} pero es {df_transformed[col_name].dtype}"

    @pytest.mark.unit
    def test_no_data_loss_during_transformation(self, sample_raw_data, sample_type_mapping):
        """
        Test 2: Verificar que no se pierdan registros durante la transformación
        """
        original_count = len(sample_raw_data)
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        transformed_count = len(df_transformed)
        
        assert original_count == transformed_count, \
            f"Se perdieron registros: original={original_count}, transformado={transformed_count}"

    @pytest.mark.unit
    def test_all_columns_preserved(self, sample_raw_data, sample_type_mapping):
        """
        Test 3: Verificar que todas las columnas se mantengan después de la transformación
        """
        original_columns = set(sample_raw_data.columns)
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        transformed_columns = set(df_transformed.columns)
        
        assert original_columns == transformed_columns, \
            f"Columnas perdidas: {original_columns - transformed_columns}"

    @pytest.mark.unit
    def test_transformation_correctness(self, sample_raw_data, expected_transformed_data, sample_type_mapping):
        """
        Test 4: Verificar que la transformación sea EXACTA usando assert_frame_equal.
        Esto valida valores, dtypes, columnas e índice, todo en uno.
        """
        df_transformed = transform_dataframe_types(sample_raw_data.copy(), sample_type_mapping)
        
        # Compara todo. Es estricto y la mejor forma de validar un DF.
        pd.testing.assert_frame_equal(df_transformed, expected_transformed_data)

    @pytest.mark.unit
    def test_null_handling(self, sample_data_with_nulls, sample_type_mapping):
        """
        Test 5: Verificar que los valores nulos se manejen correctamente con tipos nullable
        """
        df_transformed = transform_dataframe_types(sample_data_with_nulls.copy(), sample_type_mapping)
        
        # Verificar que nullable integers acepten NA
        assert pd.isna(df_transformed['trip_seconds'].iloc[1])
        assert pd.isna(df_transformed['pickup_community_area'].iloc[1])
        
        # Verificar que floats acepten NaN
        assert pd.isna(df_transformed['trip_miles'].iloc[2])
        assert pd.isna(df_transformed['fare'].iloc[1])
        
        # Verificar que datetime acepte NaT
        assert pd.isna(df_transformed['trip_start_timestamp'].iloc[2])
        
        # Verificar que boolean nullable acepte NA
        assert pd.isna(df_transformed['shared_trip_authorized'].iloc[2])

    @pytest.mark.unit
    def test_malformed_data_handling(self, sample_malformed_data, sample_type_mapping):
        """
        Test 6: Verificar que los datos malformados se coercionen a NA/NaT
        (asumiendo que la función usa errors='coerce')
        """
        df_transformed = transform_dataframe_types(sample_malformed_data.copy(), sample_type_mapping)

        # Verificar que los valores malformados se convirtieron en Nulos
        assert pd.isna(df_transformed['trip_start_timestamp'].iloc[0]), "Fecha malformada debe ser NaT"
        assert pd.isna(df_transformed['trip_seconds'].iloc[0]), "Int malformado debe ser NA"
        assert pd.isna(df_transformed['fare'].iloc[0]), "Float malformado debe ser NA"
        assert pd.isna(df_transformed['shared_trip_authorized'].iloc[0]), "Bool malformado debe ser NA"

        # Verificar que los valores buenos en filas malformadas aún se procesen
        assert df_transformed['trip_id'].iloc[0] == 'trip_malformed'
        assert df_transformed['trip_total'].iloc[0] == 15.50

        # Verificar que la fila completamente buena siga intacta
        assert df_transformed['trip_seconds'].iloc[1] == 1200
        assert df_transformed['shared_trip_authorized'].iloc[1] == False


class TestExtractTripsData:
    """Tests para la función extract_trips_data completa (con mock de API)"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """Crear directorio temporal para tests"""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def mock_api_response(self):
        """Mock de respuesta de la API"""
        return pd.DataFrame({
            'trip_id': ['trip_001', 'trip_002'],
            'trip_start_timestamp': ['2025-01-15T10:30:00', '2025-01-15T11:00:00'],
            'trip_end_timestamp': ['2025-01-15T10:45:00', '2025-01-15T11:20:00'],
            'trip_seconds': ['900', '1200'],
            'trip_miles': ['2.5', '3.8'],
            'percent_time_chicago': ['1.0', '0.95'],
            'percent_distance_chicago': ['1.0', '0.98'],
            'pickup_community_area': ['32', '8'],
            'dropoff_community_area': ['8', '32'],
            'fare': ['12.50', '15.75'],
            'tip': ['2.50', '3.00'],
            'additional_charges': ['0.50', '1.00'],
            'trip_total': ['15.50', '19.75'],
            'shared_trip_authorized': ['true', 'false'],
            'trips_pooled': ['1', '1'],
            'pickup_centroid_location': ['{"type":"Point","coordinates":[-87.6298,41.8781]}', 
                                         '{"type":"Point","coordinates":[-87.6500,41.9000]}'],
            'dropoff_centroid_location': ['{"type":"Point","coordinates":[-87.6400,41.8900]}',
                                          '{"type":"Point","coordinates":[-87.6298,41.8781]}']
        })

    @pytest.mark.unit
    def test_parquet_preserves_data_types(self, mock_api_response, temp_output_dir, monkeypatch):
        """Test 7: Verificar que los tipos se preserven al guardar y leer el parquet"""
        # Mock de fetch_data_from_api
        def mock_fetch(*args, **kwargs):
            return mock_api_response
        
        # Mock de get_raw_data_dir
        def mock_get_dir():
            return temp_output_dir

        monkeypatch.setattr('chicago_rstrips.extract_trips_data.fetch_data_from_api', mock_fetch)
        monkeypatch.setattr('chicago_rstrips.extract_trips_data.get_raw_data_dir', mock_get_dir)

        # Ejecutar extracción CON batch_id
        output_path = extract_trips_data(
            output_filename="test_output.parquet",
            batch_id="test_batch_001"  # ✅ AGREGAR batch_id
        )
        
        # Leer el parquet resultante
        df_from_parquet = pd.read_parquet(output_path)

        # Verificar tipos (Parquet puede convertir algunos tipos)
        assert pd.api.types.is_datetime64_any_dtype(df_from_parquet['trip_start_timestamp'])
        assert pd.api.types.is_datetime64_any_dtype(df_from_parquet['trip_end_timestamp'])
        assert df_from_parquet['trip_seconds'].dtype in ['int64', 'Int64']  # Parquet puede cambiar Int64 a int64
        assert df_from_parquet['fare'].dtype == 'float64'
        assert 'batch_id' in df_from_parquet.columns
        assert len(df_from_parquet) == 2

if __name__ == "__main__":
    pytest.main([__file__, "-v"])