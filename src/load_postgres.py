from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


def load_transport_report_etl(records_df):

    load_dotenv(".env.postgresql")

    # Create a connection string for SQLAlchemy
    conn_str = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    engine = create_engine(conn_str)

    target_columns = [
        "journey_id", "journey_date", "station_id", "station_name",
        "borough_id", "borough_name", "zone_id", "zone_name",
        "line_id", "line_name", "transport_mode", "passenger_count",
        "delay_minutes", "time_band", "entry_exit_flag"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="transport_report_etl", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    )

    