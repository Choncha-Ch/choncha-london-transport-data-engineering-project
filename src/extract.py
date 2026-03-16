import json
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd

raw_data = Path("data")/"raw"

# Open and Read CSV file into panda DataFrame
def read_csv_file(filename):

    file_path = raw_data/filename
    records = pd.read_csv(file_path)
    return records


# Open and Load (Parse) JSON file
def read_json_file(filename):
    file_path = raw_data/filename

    with open(file_path, "r", encoding="utf-8") as file:
        records = json.load(file)
    return records


# Open and Parse XML file
def read_schedules_xml():
    file_path = raw_data/"schedules.xml"

    tree = ET.parse(file_path)
    root = tree.getroot()

    # Put the content from xml into a List of Dictionaries format
    schedules = []

    for schedule in root.findall("schedule"):
        schedules.append({
            "schedule_id": schedule.findtext("schedule_id", default=""),
            "station_id": schedule.findtext("station_id", default=""),
            "line_id": schedule.findtext("line_id", default=""),
            "planned_start_time": schedule.findtext("planned_start_time", default=""),
            "planned_end_time": schedule.findtext("planned_end_time", default=""),
            "service_day": schedule.findtext("service_day", default=""),
            "status_note": schedule.findtext("status_note", default="")
        })

    return schedules


"""
# Test the extraction layer
if __name__ == "__main__":
    stations = read_csv_file("stations.csv")
    lines = read_csv_file("lines.csv")
    boroughs = read_csv_file("boroughs.csv")
    zones = read_csv_file("zones.csv")
    journeys = read_json_file("journeys.json")
    schedules = read_schedules_xml()

    print("Stations:", len(stations))
    print("Lines:", len(lines))
    print("Boroughs:", len(boroughs))
    print("Zones:", len(zones))
    print("Journeys:", len(journeys))
    print("Schedules:", len(schedules))
"""