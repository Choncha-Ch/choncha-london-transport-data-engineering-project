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