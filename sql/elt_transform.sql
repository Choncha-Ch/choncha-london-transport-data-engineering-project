CREATE TABLE IF NOT EXISTS transport_report_elt (
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

DELETE FROM transport_report_elt;

WITH dedup_stations AS (
    SELECT DISTINCT ON (station_id)
        station_id,
        station_name,
        borough_id,
        zone_id,
        line_id,
        station_type
    FROM raw_stations
    WHERE station_id IS NOT NULL
      AND TRIM(station_id) <> ''
    ORDER BY station_id
),
dedup_lines AS (
    SELECT DISTINCT ON (line_id)
        line_id,
        line_name,
        transport_mode,
        operator_id,
        vehicle_type_id
    FROM raw_lines
    WHERE line_id IS NOT NULL
      AND TRIM(line_id) <> ''
    ORDER BY line_id
),
dedup_boroughs AS (
    SELECT DISTINCT ON (borough_id)
        borough_id,
        borough_name,
        region_group
    FROM raw_boroughs
    WHERE borough_id IS NOT NULL
      AND TRIM(borough_id) <> ''
    ORDER BY borough_id
),
dedup_zones AS (
    SELECT DISTINCT ON (zone_id)
        zone_id,
        zone_name,
        fare_group
    FROM raw_zones
    WHERE zone_id IS NOT NULL
      AND TRIM(zone_id) <> ''
    ORDER BY zone_id
)
INSERT INTO transport_report_elt (
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
SELECT
    rj.journey_id,
    CAST(rj.journey_date AS DATE) AS journey_date,
    ds.station_id,
    INITCAP(TRIM(ds.station_name)) AS station_name,
    ds.borough_id,
    INITCAP(TRIM(db.borough_name)) AS borough_name,
    ds.zone_id,
    INITCAP(TRIM(dz.zone_name)) AS zone_name,
    dl.line_id,
    INITCAP(TRIM(dl.line_name)) AS line_name,
    INITCAP(TRIM(dl.transport_mode)) AS transport_mode,
    CAST(rj.passenger_count AS INTEGER) AS passenger_count,
    CAST(rj.delay_minutes AS INTEGER) AS delay_minutes,
    INITCAP(TRIM(rj.time_band)) AS time_band,
    INITCAP(TRIM(rj.entry_exit_flag)) AS entry_exit_flag
FROM raw_journeys rj
JOIN dedup_stations ds
    ON rj.station_id = ds.station_id
JOIN dedup_lines dl
    ON rj.line_id = dl.line_id
LEFT JOIN dedup_boroughs db
    ON ds.borough_id = db.borough_id
LEFT JOIN dedup_zones dz
    ON ds.zone_id = dz.zone_id
WHERE rj.journey_id IS NOT NULL
  AND TRIM(rj.journey_id) <> ''
  AND rj.station_id IS NOT NULL
  AND TRIM(rj.station_id) <> ''
  AND rj.line_id IS NOT NULL
  AND TRIM(rj.line_id) <> ''
  AND rj.passenger_count ~ '^[0-9]+$'
  AND rj.delay_minutes ~ '^[0-9]+$'
  AND rj.journey_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

