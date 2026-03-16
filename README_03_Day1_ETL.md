# Day 1 ETL Tasks

## London Transport Data Engineering Project

Welcome to the **ETL part of Day 1**.

In this file, you will build the **local ETL version** of the London Transport Data Engineering Project.

ETL means:

* **Extract**
* **Transform**
* **Load**

That means your job in this part is to:

1. extract raw data from source files
2. transform and clean the data in Python
3. load the cleaned final result into PostgreSQL

This is a realistic pattern used in real data engineering work.

In many companies, ETL is used when teams want to clean and control the data before it enters the final reporting table.

So even though this project is guided, the logic itself is professional and realistic.

---

# 1. Day 1 ETL objective

Your goal in this part is to build a working ETL pipeline that uses London transport raw data and produces a clean reporting table in PostgreSQL.

By the end of this ETL task file, you should have:

* extracted selected raw source files
* cleaned and standardized important fields
* joined the main datasets together
* calculated useful business values
* loaded the final clean data into PostgreSQL
* validated the result with SQL queries

---

# 2. Important note before starting

This project contains **10 raw source files**, but that does **not** mean you must fully use all 10 files in the final ETL join today.

In real data engineering, not every source is always used equally in the first version of a pipeline.

For **Day 1 ETL**, we will focus mainly on the files that are most important for building the first reporting output:

* `stations.csv`
* `lines.csv`
* `journeys.json`
* `boroughs.csv`
* `zones.csv`

The other files are still important in the project and may be used later in extensions, validation, enrichment, ELT comparison, Spark, or Databricks stages.

That is realistic.

---

# 3. What final ETL output are we building today?

For Day 1 ETL, the final PostgreSQL table will be:

```text
transport_report_etl
```

This table should help answer business questions such as:

* Which stations are busiest?
* Which lines show the highest passenger traffic?
* Which boroughs show the most activity?
* Which zones appear most often?
* Which journeys have delays?

This is your Day 1 business-ready ETL output.

---

# 4. ETL flow for Day 1

Here is the flow you are building:

```text
Raw source files → Python extraction → Python cleaning and joining → PostgreSQL final reporting table
```

More specifically:

```text
stations.csv
lines.csv
journeys.json
boroughs.csv
zones.csv
      ↓
Python extraction
      ↓
Python transformation and joining
      ↓
transport_report_etl
```

Keep this order in mind.

The transformation happens **before** the final load into PostgreSQL.

That is what makes this ETL.

---

# 5. Step 1 - Create the ETL target table

## Your task

Open:

```text
sql/create_tables.sql
```

and add this SQL:

```sql
CREATE TABLE IF NOT EXISTS transport_report_etl (
    journey_id TEXT,
    journey_date DATE,
    station_id TEXT,
    station_name TEXT,
    borough_id TEXT,
    borough_name TEXT,
    zone_id TEXT,
    zone_name TEXT,
    line_id TEXT,
    line_name TEXT,
    transport_mode TEXT,
    passenger_count INTEGER,
    delay_minutes INTEGER,
    time_band TEXT,
    entry_exit_flag TEXT
);
```

## Why this matters

This is the final Day 1 ETL reporting table.

It represents the clean result that the business team could actually query later.

In ETL, we prepare the data first, then load it into the target.

---

# 6. Step 2 - Prepare the extraction file

## Your task

Open:

```text
src/extract.py
```

and start with these imports and folder path:

```python
import csv
import json
import xml.etree.ElementTree as ET
from pathlib import Path

RAW_DATA_FOLDER = Path("data") / "raw"
```

## Why this matters

The project uses multiple file formats.

Even though today’s main ETL join will use only some of the files, the extraction layer should still be built in a way that matches the real project structure.

That is a professional habit.

---

# 7. Step 3 - Add a CSV reader helper

## Your task

Still in `src/extract.py`, add this helper function:

```python
def read_csv_file(filename):
    file_path = RAW_DATA_FOLDER / filename
    records = []

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            records.append(row)

    return records
```

## What this code does

This function reads any CSV file from `data/raw/` and returns a list of dictionaries.

## Why this is useful

Several of the transport source files are CSV files, so one reusable CSV reader is cleaner than repeating the same code many times.

---

# 8. Step 4 - Add a JSON reader helper

## Your task

Now add this function:

```python
def read_json_file(filename):
    file_path = RAW_DATA_FOLDER / filename

    with open(file_path, mode="r", encoding="utf-8") as file:
        records = json.load(file)

    return records
```

## What this code does

This opens a JSON file and loads the content into Python.

## Why this matters

`journeys.json` and `disruptions.json` both use JSON, so this helper keeps the extraction code organized.

---

# 9. Step 5 - Add an XML reader for schedules

## Your task

Even though `schedules.xml` is not part of the main ETL join today, we still want the project structure to reflect the real raw data environment.

Add this XML reader too:

```python
def read_schedules_xml():
    file_path = RAW_DATA_FOLDER / "schedules.xml"
    tree = ET.parse(file_path)
    root = tree.getroot()

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
```

## Why this matters

This keeps the project honest.

You are working in a repository with 10 raw files, and your extraction layer should reflect that broader business environment, even if not every file is used in the first ETL result.

---

# 10. Step 6 - Add specific extraction functions for the Day 1 ETL join

## Your task

Now add these functions to `src/extract.py`:

```python
def read_stations_csv():
    return read_csv_file("stations.csv")


def read_lines_csv():
    return read_csv_file("lines.csv")


def read_boroughs_csv():
    return read_csv_file("boroughs.csv")


def read_zones_csv():
    return read_csv_file("zones.csv")


def read_journeys_json():
    return read_json_file("journeys.json")
```

## Why this matters

These are the main source files used for the Day 1 ETL reporting table.

Keeping separate named functions makes the code easier to understand.

---

# 11. Step 7 - Test the extraction layer

## Your task

At the bottom of `src/extract.py`, temporarily add:

```python
if __name__ == "__main__":
    stations = read_stations_csv()
    lines = read_lines_csv()
    boroughs = read_boroughs_csv()
    zones = read_zones_csv()
    journeys = read_journeys_json()
    schedules = read_schedules_xml()

    print("Stations:", len(stations))
    print("Lines:", len(lines))
    print("Boroughs:", len(boroughs))
    print("Zones:", len(zones))
    print("Journeys:", len(journeys))
    print("Schedules:", len(schedules))
```

## Why this matters

Always test extraction before continuing.

This helps you confirm that:

* file paths are correct
* the files load successfully
* the record counts look reasonable

That is a real engineering habit.

---

# 12. Step 8 - Start the transformation layer

## Your task

Open:

```text
src/transform_etl.py
```

and begin with a reusable text-cleaning helper:

```python
def clean_text(value):
    if value is None:
        return ""
    return str(value).strip().title()
```

## Why this matters

Raw data often contains:

* extra spaces
* lowercase/uppercase inconsistency
* missing values

A helper like this is simple, but very useful in real ETL work.

---

# 13. Step 9 - Clean station records

## Your task

Add this function:

```python
def clean_stations(stations):
    cleaned_stations = []
    seen_station_ids = set()

    for station in stations:
        station_id = str(station.get("station_id", "")).strip()

        if not station_id:
            continue

        if station_id in seen_station_ids:
            continue

        seen_station_ids.add(station_id)

        cleaned_stations.append({
            "station_id": station_id,
            "station_name": clean_text(station.get("station_name")),
            "borough_id": str(station.get("borough_id", "")).strip(),
            "zone_id": str(station.get("zone_id", "")).strip(),
            "line_id": str(station.get("line_id", "")).strip(),
            "station_type": clean_text(station.get("station_type"))
        })

    return cleaned_stations
```

## What this code does

This function:

* removes rows with missing station IDs
* removes duplicate station IDs
* cleans station names
* preserves the keys needed for joins

## Why this matters

Stations are reference data.
Reference data should be clean before being used in reporting joins.

---

# 14. Step 10 - Clean line records

## Your task

Add this function:

```python
def clean_lines(lines):
    cleaned_lines = []
    seen_line_ids = set()

    for line in lines:
        line_id = str(line.get("line_id", "")).strip()

        if not line_id:
            continue

        if line_id in seen_line_ids:
            continue

        seen_line_ids.add(line_id)

        cleaned_lines.append({
            "line_id": line_id,
            "line_name": clean_text(line.get("line_name")),
            "transport_mode": clean_text(line.get("transport_mode")),
            "operator_id": str(line.get("operator_id", "")).strip(),
            "vehicle_type_id": str(line.get("vehicle_type_id", "")).strip()
        })

    return cleaned_lines
```

## Why this matters

Lines are also reference data, and the reporting output needs a clean line name and transport mode.

---

# 15. Step 11 - Clean borough records

## Your task

Add this function:

```python
def clean_boroughs(boroughs):
    cleaned_boroughs = []
    seen_borough_ids = set()

    for borough in boroughs:
        borough_id = str(borough.get("borough_id", "")).strip()

        if not borough_id:
            continue

        if borough_id in seen_borough_ids:
            continue

        seen_borough_ids.add(borough_id)

        cleaned_boroughs.append({
            "borough_id": borough_id,
            "borough_name": clean_text(borough.get("borough_name")),
            "region_group": clean_text(borough.get("region_group"))
        })

    return cleaned_boroughs
```

## Why this matters

Borough data enriches the reporting output with location context.

---

# 16. Step 12 - Clean zone records

## Your task

Add this function:

```python
def clean_zones(zones):
    cleaned_zones = []
    seen_zone_ids = set()

    for zone in zones:
        zone_id = str(zone.get("zone_id", "")).strip()

        if not zone_id:
            continue

        if zone_id in seen_zone_ids:
            continue

        seen_zone_ids.add(zone_id)

        cleaned_zones.append({
            "zone_id": zone_id,
            "zone_name": clean_text(zone.get("zone_name")),
            "fare_group": clean_text(zone.get("fare_group"))
        })

    return cleaned_zones
```

## Why this matters

Zone data helps us interpret transport geography and reporting context.

---

# 17. Step 13 - Clean journey records

## Your task

Now add the main event-data cleaning function:

```python
def clean_journeys(journeys):
    cleaned_journeys = []

    for journey in journeys:
        journey_id = str(journey.get("journey_id", "")).strip()
        station_id = str(journey.get("station_id", "")).strip()
        line_id = str(journey.get("line_id", "")).strip()
        passenger_count_value = str(journey.get("passenger_count", "")).strip()
        delay_minutes_value = str(journey.get("delay_minutes", "")).strip()
        journey_date = str(journey.get("journey_date", "")).strip()

        if not journey_id or not station_id or not line_id:
            continue

        try:
            passenger_count = int(passenger_count_value)
            delay_minutes = int(delay_minutes_value)
        except ValueError:
            continue

        cleaned_journeys.append({
            "journey_id": journey_id,
            "station_id": station_id,
            "line_id": line_id,
            "passenger_count": passenger_count,
            "delay_minutes": delay_minutes,
            "journey_date": journey_date,
            "time_band": clean_text(journey.get("time_band")),
            "entry_exit_flag": clean_text(journey.get("entry_exit_flag"))
        })

    return cleaned_journeys
```

## What this does

This function:

* skips missing key IDs
* converts passenger counts into integers
* converts delay minutes into integers
* removes invalid numeric rows
* standardizes text fields

## Why this matters

Journeys are the main fact-like dataset in this Day 1 reporting output.

This is where the most important business activity lives.

---

# 18. Step 14 - Create lookup dictionaries

## Your task

Add this helper function:

```python
def build_lookup(records, key_field):
    lookup = {}

    for record in records:
        lookup[record[key_field]] = record

    return lookup
```

## Why this matters

When joining datasets in Python, lookup dictionaries make the work faster and clearer.

For example:

* stations by `station_id`
* lines by `line_id`
* boroughs by `borough_id`
* zones by `zone_id`

---

# 19. Step 15 - Build the final ETL reporting table in Python

## Your task

Now add the main report-building function:

```python
def build_transport_report_etl(stations, lines, boroughs, zones, journeys):
    station_lookup = build_lookup(stations, "station_id")
    line_lookup = build_lookup(lines, "line_id")
    borough_lookup = build_lookup(boroughs, "borough_id")
    zone_lookup = build_lookup(zones, "zone_id")

    transport_report = []

    for journey in journeys:
        station = station_lookup.get(journey["station_id"])
        line = line_lookup.get(journey["line_id"])

        if not station or not line:
            continue

        borough = borough_lookup.get(station["borough_id"])
        zone = zone_lookup.get(station["zone_id"])

        transport_report.append({
            "journey_id": journey["journey_id"],
            "journey_date": journey["journey_date"],
            "station_id": station["station_id"],
            "station_name": station["station_name"],
            "borough_id": station["borough_id"],
            "borough_name": borough["borough_name"] if borough else "",
            "zone_id": station["zone_id"],
            "zone_name": zone["zone_name"] if zone else "",
            "line_id": line["line_id"],
            "line_name": line["line_name"],
            "transport_mode": line["transport_mode"],
            "passenger_count": journey["passenger_count"],
            "delay_minutes": journey["delay_minutes"],
            "time_band": journey["time_band"],
            "entry_exit_flag": journey["entry_exit_flag"]
        })

    return transport_report
```

## Why this matters

This is the core ETL business result.

Here you combine:

* journey activity
* station context
* borough context
* zone context
* line information

This is where the raw operational files become a usable reporting dataset.

---

# 20. Step 16 - Create a single ETL transformation entry function

## Your task

At the bottom of `src/transform_etl.py`, add:

```python
def run_etl_transform(stations_raw, lines_raw, boroughs_raw, zones_raw, journeys_raw):
    stations_clean = clean_stations(stations_raw)
    lines_clean = clean_lines(lines_raw)
    boroughs_clean = clean_boroughs(boroughs_raw)
    zones_clean = clean_zones(zones_raw)
    journeys_clean = clean_journeys(journeys_raw)

    transport_report = build_transport_report_etl(
        stations_clean,
        lines_clean,
        boroughs_clean,
        zones_clean,
        journeys_clean
    )

    return transport_report
```

## Why this matters

This keeps your ETL logic organized and easy to run.

That is much better than scattering transformation steps across different places.

---

# 21. Step 17 - Prepare PostgreSQL loading

## Your task

Open:

```text
src/load_postgres.py
```

and add this connection function:

```python
import psycopg2


def get_connection():
    return psycopg2.connect(
        dbname="transport_project",
        user="postgres",
        password="your_password",
        host="localhost",
        port="5432"
    )
```

## Important note

Replace `"your_password"` with your real PostgreSQL password.

---

# 22. Step 18 - Add the ETL loader function

## Your task

Still in `src/load_postgres.py`, add:

```python
def load_transport_report_etl(records):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("DELETE FROM transport_report_etl;")

    insert_query = """
        INSERT INTO transport_report_etl (
            journey_id,
            journey_date,
            station_id,
            station_name,
            borough_id,
            borough_name,
            zone_id,
            zone_name,
            line_id,
            line_name,
            transport_mode,
            passenger_count,
            delay_minutes,
            time_band,
            entry_exit_flag
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for record in records:
        cursor.execute(
            insert_query,
            (
                record["journey_id"],
                record["journey_date"],
                record["station_id"],
                record["station_name"],
                record["borough_id"],
                record["borough_name"],
                record["zone_id"],
                record["zone_name"],
                record["line_id"],
                record["line_name"],
                record["transport_mode"],
                record["passenger_count"],
                record["delay_minutes"],
                record["time_band"],
                record["entry_exit_flag"]
            )
        )

    connection.commit()
    cursor.close()
    connection.close()
```

## Why this matters

In ETL, PostgreSQL receives the final cleaned business-ready output.

That is exactly what this function does.

---

# 23. Step 19 - Create the ETL runner script

## Your task

Open:

```text
src/run_etl.py
```

and add this code:

```python
from extract import (
    read_stations_csv,
    read_lines_csv,
    read_boroughs_csv,
    read_zones_csv,
    read_journeys_json
)
from transform_etl import run_etl_transform
from load_postgres import load_transport_report_etl


def main():
    stations_raw = read_stations_csv()
    lines_raw = read_lines_csv()
    boroughs_raw = read_boroughs_csv()
    zones_raw = read_zones_csv()
    journeys_raw = read_journeys_json()

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
```

## Why this matters

This is the ETL pipeline entry point.

It clearly shows the order:

* extract
* transform
* load

---

# 24. Step 20 - Run the ETL pipeline

## Your task

From the project root, run:

```bash
python src/run_etl.py
```

## What you should expect

If everything works, you should see something like:

```text
ETL completed successfully. Loaded X rows into transport_report_etl.
```

If you see an error, check:

* PostgreSQL connection details
* whether the table was created first
* whether the files exist in `data/raw/`
* whether you are running from the project root

That is a normal part of project work.

---

# 25. Step 21 - Validate the ETL result in PostgreSQL

## Your task

Now inspect the data with SQL.

You can place these checks in:

```text
sql/etl_checks.sql
```

Add:

```sql
SELECT COUNT(*) FROM transport_report_etl;
```

```sql
SELECT * FROM transport_report_etl LIMIT 10;
```

```sql
SELECT station_name, SUM(passenger_count) AS total_passengers
FROM transport_report_etl
GROUP BY station_name
ORDER BY total_passengers DESC
LIMIT 10;
```

```sql
SELECT line_name, AVG(delay_minutes) AS avg_delay
FROM transport_report_etl
GROUP BY line_name
ORDER BY avg_delay DESC;
```

```sql
SELECT borough_name, SUM(passenger_count) AS total_passengers
FROM transport_report_etl
GROUP BY borough_name
ORDER BY total_passengers DESC;
```

## Why this matters

A pipeline is not complete only because the code ran.

A data engineer must validate that the final result makes sense.

---

# 26. Step 22 - Write project notes

## Your task

Open:

```text
docs/project_notes.md
```

and write short notes about:

* which files were used in the main ETL join
* what kinds of data quality problems were found
* which records were skipped and why
* what the final reporting table represents

## Why this matters

Real engineers document what they did.

A project is stronger when someone can read both the code and the notes.

---

# 27. Step 23 - Commit and push your ETL progress

## Your task

After completing the ETL part, push your work.

Example:

```bash
git add .
git commit -m "Complete Day 1 ETL pipeline"
git push
```

## Why this matters

This project is part of your public portfolio work.

Keep your progress visible and professional.

---

# 28. What makes this ETL version realistic

This ETL design reflects real engineering habits:

* working with multiple raw source files
* cleaning reference data
* cleaning event data
* joining operational and descriptive datasets
* producing a final reporting table
* validating business output in PostgreSQL

This is why this project should be taken seriously.

It is guided, but it is still realistic.

---

# 29. What you should understand after finishing

By the end of this ETL part, you should understand that:

* ETL means transform before load
* Python performs the main cleaning and joining in this version
* PostgreSQL stores the final clean output
* not all raw files must be fully used in the first reporting version
* a real project can start with a subset of sources and expand later

That last point is important.

Real projects often start with a realistic first scope, not with everything at once.

---

# 30. What comes next

Once you finish the ETL tasks, move to:

* [README 04 - Day 1 ELT Tasks](./README_04_Day1_ELT.md)

In the ELT version, you will keep the same business context, but the architecture will change:

* raw data loaded first into PostgreSQL
* transformation happens later using SQL

That comparison is one of the most important learning goals of the whole Day 1 project.

---

# 31. Final message

Treat this ETL part like real junior data engineering work.

The goal is not only to run a script.

The goal is to understand how raw transport data becomes useful reporting data through a structured ETL workflow.

Work carefully, validate your output, document your progress, and keep your GitHub repository organized.

That is exactly how strong project habits begin.


