import pandas as pd

def clean_text(column):
    return column.fillna("").astype(str).str.strip().str.title()


def clean_stations(df):
    # 1. Drop rows where station_id is completely missing (NaN)
    df = df.dropna(subset=['station_id'])
    
    # 2. Clean 'station_id' and remove rows that are just empty strings after stripping
    df['station_id'] = df['station_id'].astype(str).str.strip()
    df = df[df['station_id'].notna() & (df['station_id'] != "")].copy()

    # 3. Deduplicate based on station_id (keeping the first occurrence)
    df = df.drop_duplicates(subset=['station_id'], keep='first')
    
    # 4. Clean ID columns (Strip and convert to String)
    id_cols = ['borough_id', 'zone_id', 'line_id']
    for col in id_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()
        
    # 5. Clean Text columns (Title Case, Strip, handle Nulls)
    text_cols = ['station_name', 'station_type']
    for col in text_cols:
        df[col] = clean_text(df[col])
        
    return df.reset_index(drop=True)



def clean_lines(df):
    # 1. Strip whitespace from line_id and remove rows that are null or empty
    df['line_id'] = df['line_id'].astype(str).str.strip()
    df = df[df['line_id'].notna() & (df['line_id'] != "")].copy()

    # 2. Remove duplicates based on line_id
    df = df.drop_duplicates(subset=['line_id'], keep='first')

    # 3. Clean Text columns (Title Case + Strip)
    text_cols = ['line_name', 'transport_mode']
    for col in text_cols:
        df[col] = clean_text(df[col])

    # 4. Clean ID columns (Strip + String conversion)
    id_cols = ['operator_id', 'vehicle_type_id']
    for col in id_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df.reset_index(drop=True)


def clean_boroughs(df):
    # 1. Clean the ID column and remove empty/null rows
    df['borough_id'] = df['borough_id'].astype(str).str.strip()
    df = df[df['borough_id'].notna() & (df['borough_id'] != "")].copy()

    # 2. Remove duplicates based on borough_id
    df = df.drop_duplicates(subset=['borough_id'], keep='first')

    # 3. Clean Text columns (Title Case + Strip)
    text_cols = ['borough_name', 'region_group']
    for col in text_cols:
        df[col] = clean_text(df[col])

    # 4. Refresh the row numbering
    return df.reset_index(drop=True)


def clean_zones(df):
    # 1. Clean the ID column and remove empty or null rows
    df['zone_id'] = df['zone_id'].astype(str).str.strip()
    df = df[df['zone_id'].notna() & (df['zone_id'] != "")]

    # 2. Remove duplicates 
    df = df.drop_duplicates(subset=['zone_id'], keep='first')

    # 3. Clean Text columns (Title Case + Strip)
    text_cols = ['zone_name', 'fare_group']
    for col in text_cols:
        df[col] = clean_text(df[col])

    # 4. Finalize the index
    return df.reset_index(drop=True)


def clean_journeys(df):

    # 1. Clean IDs and drop rows where essential IDs are missing
    id_cols = ['journey_id', 'station_id', 'line_id']
    for col in id_cols:
        df[col] = df[col].astype(str).str.strip()
    
    # drop when the columns in id_cols not exist
    df = df.dropna(subset=id_cols)
    df = df[(df[id_cols] != "").all(axis=1)]

    # 2. Numeric Conversion 
    # errors='coerce' turns non-numeric junk into NaN (Null)
    num_cols = ['passenger_count', 'delay_minutes']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows that failed the numeric conversion 
    df = df.dropna(subset=num_cols)
    
    # Convert to integer now that it's safe
    df[num_cols] = df[num_cols].astype(int)

    # 3. Clean Date and Text
    df['journey_date'] = df['journey_date'].astype(str).str.strip()
    
    text_cols = ['time_band', 'entry_exit_flag']
    for col in text_cols:
        df[col] = clean_text(df[col])

    return df.reset_index(drop=True)


# ===================================================================
# Put everything together: Build the final ETL report

import pandas as pd

def build_transport_report_etl(df_stations, df_lines, df_boroughs, df_zones, df_journeys):
    # 1. Start with Journeys (The base table)
    report = df_journeys.copy()

    # 2. Add Station info: We ONLY want name, borough_id, and zone_id
    report = pd.merge(
        report, 
        df_stations[['station_id', 'station_name', 'borough_id', 'zone_id']], 
        on='station_id', 
        how='inner'
    )

    # 3. Add Line info: We ONLY want name and transport mode
    report = pd.merge(
        report, 
        df_lines[['line_id', 'line_name', 'transport_mode']], 
        on='line_id', 
        how='inner'
    )

    # 4. Add Borough info: We ONLY want borough_name
    report = pd.merge(
        report, 
        df_boroughs[['borough_id', 'borough_name']], 
        on='borough_id', 
        how='left'
    )

    # 5. Add Zone info: We ONLY want zone_name
    report = pd.merge(
        report, 
        df_zones[['zone_id', 'zone_name']], 
        on='zone_id', 
        how='left'
    )

    # 6. Final selection and order (matching order exactly)
    final_cols = [
        "journey_id", "journey_date", "station_id", "station_name",
        "borough_id", "borough_name", "zone_id", "zone_name",
        "line_id", "line_name", "transport_mode", "passenger_count",
        "delay_minutes", "time_band", "entry_exit_flag"
    ]
    
    return report[final_cols]


# =================================================================
# Create a single ETL transformation entry function

def run_etl_transform(stations_raw, lines_raw, boroughs_raw, zones_raw, journeys_raw):
    stations_clean = clean_stations(pd.DataFrame(stations_raw))
    lines_clean = clean_lines(pd.DataFrame(lines_raw))
    boroughs_clean = clean_boroughs(pd.DataFrame(boroughs_raw))
    zones_clean = clean_zones(pd.DataFrame(zones_raw))
    journeys_clean = clean_journeys(pd.DataFrame(journeys_raw))

    # Use explicit names so the order doesn't matter
    transport_report = build_transport_report_etl(
        df_stations=stations_clean,
        df_lines=lines_clean,
        df_boroughs=boroughs_clean,
        df_zones=zones_clean,
        df_journeys=journeys_clean
    )

    return transport_report

