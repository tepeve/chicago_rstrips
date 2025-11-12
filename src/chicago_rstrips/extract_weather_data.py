import pandas as pd
from sqlalchemy import create_engine, text




def get_weather_stations_data(connection_string):
    """
    """
    engine = create_engine(connection_string)
    
    query = text("""
        SELECT station_id, longitude, latitude
        FROM dim_spatial.weather_stations_points;
    """)
    
    df = pd.read_sql(query, engine)
    
    return df