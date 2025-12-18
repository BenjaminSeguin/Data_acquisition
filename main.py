import argparse
import sqlite3
import time
from datetime import datetime

from Functions.Open_Meteo_API import *
from Functions.RTE_API import *
from Functions.RTE_to_database import *
from Functions.transparency_api import *
from Functions.utils import *

OUTPUT_FOLDER = "Database"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# OpenMeteo
api = OpenMeteoAPI(output_folder=OUTPUT_FOLDER)

start_date_hour = "2023-12-31"
end_date_hour = "2024-12-31"

start_date_day = "2015-01-01"
end_date_day = "2024-12-31"

locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
    {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
    {'name': 'Lille', 'latitude': 50.6292, 'longitude': 3.0573}
]

hourly_data = api.fetch_multiple_locations_hourly(locations, start_date_hour, end_date_hour)
hourly_records = api.process_multiple_locations_hourly(hourly_data)
api.save_to_database(hourly_records, 'weather_hourly', 'weather_hourly.db')

time.sleep(60)

daily_data = api.fetch_multiple_locations_daily(locations, start_date_day, end_date_day)
daily_records = api.process_multiple_locations_daily(daily_data)
api.save_to_database(daily_records, 'weather_daily', 'weather_daily.db')

for db_file, table, time_col in [('weather_hourly.db', 'weather_hourly', 'datetime'),
                                  ('weather_daily.db', 'weather_daily', 'date')]:
    conn = sqlite3.connect(f'{OUTPUT_FOLDER}/{db_file}')
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM {table} 
        WHERE rowid NOT IN (SELECT MAX(rowid) FROM {table} GROUP BY {time_col}, city)
    """)
    conn.commit()
    conn.close()

# RTE Scraper
rte = RTEAPI(OUTPUT_FOLDER)
rte.open_page()
rte.download_data("01/01/2024", "31/12/2024", final_filename="RTE_data.xls")
rte.close_page()

rte_db = RTEDatabase()
rte_db.create_database_from_folder(
    folder_path=OUTPUT_FOLDER,
    db_path=f'{OUTPUT_FOLDER}/rte.db',
    file_pattern="RTE_*.xls",
    table_name="rte_data"
)

# ENTSO-E
data_generation = get_transp_api(
    req_params={"documentType": "A69", "processType": "A01"},
    opt_params={},
    domains={"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"},
    periodStart="202312312300", periodEnd="202412312300",
    security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f"
)

data_market = get_transp_api(
    req_params={"documentType": "A44"},
    opt_params={},
    domains={"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"},
    periodStart="202312312300", periodEnd="202412312300",
    security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f"
)

data_load = get_transp_api(
    req_params={"documentType": "A65", "processType": "A01"},
    opt_params={},
    domains={"outBiddingZone_Domain": "10YFR-RTE------C"},
    periodStart="202312312300", periodEnd="202412312300",
    security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f"
)

final_data = process([data_generation, data_market, data_load])
final_data = final_data.rename(columns={
    "Price Document": "Energy prices",
    "System total load - Day ahead": "Total load forecast",
    "Solar generation - Solar - Wind and solar forecast - Day ahead": "Solar generation forecast",
    "Wind generation - Wind Offshore - Wind and solar forecast - Day ahead": "Wind offshore generation forecast",
    "Wind generation - Wind Onshore - Wind and solar forecast - Day ahead": "Wind onshore generation forecast"
})
save_to_sqlite(final_data, 'entsoe_data', f'{OUTPUT_FOLDER}/entsoe.db')

# Merge all hourly databases
df_weather = pd.read_sql("SELECT * FROM weather_hourly", sqlite3.connect(f'{OUTPUT_FOLDER}/weather_hourly.db'))
df_rte = pd.read_sql("SELECT * FROM rte_data", sqlite3.connect(f'{OUTPUT_FOLDER}/rte.db'))
df_entsoe = pd.read_sql("SELECT * FROM entsoe_data", sqlite3.connect(f'{OUTPUT_FOLDER}/entsoe.db'))

df_weather['datetime'] = pd.to_datetime(df_weather['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df_rte['datetime'] = pd.to_datetime(df_rte['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
df_entsoe['datetime'] = pd.to_datetime(df_entsoe['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

df_merged = df_weather.merge(df_rte, on='datetime', how='outer')
df_merged = df_merged.merge(df_entsoe, on='datetime', how='outer')

conn = sqlite3.connect(f'{OUTPUT_FOLDER}/final.db')
df_merged.to_sql('energy_data', conn, if_exists='replace', index=False)
conn.close()