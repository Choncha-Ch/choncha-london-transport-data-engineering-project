from extract import (
    read_csv_file,
    read_json_file
)
from load_postgres import (
    load_raw_stations,
    load_raw_lines,
    load_raw_boroughs,
    load_raw_zones,
    load_raw_journeys,
    run_sql_file
)


def main():
    stations_raw = read_csv_file("stations.csv")
    lines_raw = read_csv_file("lines.csv")
    boroughs_raw = read_csv_file("boroughs.csv")
    zones_raw = read_csv_file("zones.csv")
    journeys_raw = read_json_file("journeys.json")

    load_raw_stations(stations_raw)
    load_raw_lines(lines_raw)
    load_raw_boroughs(boroughs_raw)
    load_raw_zones(zones_raw)
    load_raw_journeys(journeys_raw)

    run_sql_file("sql/elt_transform.sql")

    print("ELT completed successfully. Raw data loaded and SQL transformations applied.")


if __name__ == "__main__":
    main()