import pandas as pd
from extract import (
    read_csv_file,
    read_json_file
)
from transform_etl import run_etl_transform
from load_postgres import load_transport_report_etl


def main():
    stations_raw = read_csv_file("stations.csv")
    lines_raw = read_csv_file("lines.csv")
    boroughs_raw = read_csv_file("boroughs.csv")
    zones_raw = read_csv_file("zones.csv")
    journeys_raw = read_json_file("journeys.json")

    transport_report = run_etl_transform(
        stations_raw,
        lines_raw,
        boroughs_raw,
        zones_raw,
        journeys_raw
    )

    load_transport_report_etl(transport_report)

    print(f"ETL completed successfully. Loaded {len(transport_report)} rows into transport_report_etl.")


if __name__ == "__main__":
    main()