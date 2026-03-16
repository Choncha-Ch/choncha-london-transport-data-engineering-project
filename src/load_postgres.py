from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv(".env.postgresql")

# Create a connection string for SQLAlchemy
conn_str = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
engine = create_engine(conn_str)


def load_transport_report_etl(records_df):

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

# =========================================================

def load_raw_stations(records):

    records_df = pd.DataFrame(records)

    target_columns = [
        "station_id",
        "station_name",
        "borough_id",
        "zone_id",
        "line_id",
        "station_type"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="raw_stations", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    ) 


def load_raw_lines(records):

    records_df = pd.DataFrame(records)

    target_columns = [
        "line_id",
        "line_name",
        "transport_mode",
        "operator_id",
        "vehicle_type_id",
        "service_status",
        "snapshot_date"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="raw_lines", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    ) 


def load_raw_boroughs(records):

    records_df = pd.DataFrame(records)

    target_columns = [
      "borough_id",
      "borough_name",
      "region_group",
      "population_band",
      "avg_daily_ridership_band",
      "report_month"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="raw_boroughs", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    ) 


def load_raw_zones(records):

    records_df = pd.DataFrame(records)

    target_columns = [
      "zone_id",
      "zone_name",
      "fare_group",
      "peak_multiplier",
      "status_note",
      "report_date"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="raw_zones", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    ) 


def load_raw_journeys(records):

    records_df = pd.DataFrame(records)

    target_columns = [
      "journey_id",
      "station_id",
      "line_id",
      "passenger_count",
      "delay_minutes",
      "journey_date",
      "time_band",
      "entry_exit_flag"
    ]
    df_to_load = records_df[target_columns]
    
    # Load the data
    df_to_load.to_sql(
        name="raw_journeys", 
        con=engine, 
        if_exists='replace', 
        index=False          # Don't create a column for the DataFrame index
    ) 


# =========================================
# SQL Runner Helper: to help run SQL file

from pathlib import Path
import psycopg2


def run_sql_file(file_path):

    load_dotenv(".env.postgresql")
    
    connection = psycopg2.connect(
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )
    try:
        cursor = connection.cursor()

        # Read SQL file
        sql_path = Path(file_path)
        sql_content = sql_path.read_text(encoding="utf-8")

        # Execute and commit
        cursor.execute(sql_content)
        connection.commit()

    except Exception as e:
        connection.rollback()
        print(f"Error executing {file_path}: {e}")

    finally:
        # Always close the connection
        cursor.close()
        connection.close()