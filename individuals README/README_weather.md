# Weather Data Collection with Open-Meteo API

Python scripts to fetch historical weather data from the Open-Meteo API and store it in SQLite databases.

## Requirements

```bash
pip install requests jmespath
```

## Files

- `Open_Meteo_API.py` - Main script to fetch and store weather data
- `Open_Meteo_verifications.py` - Simple script to check database integrity

## Quick Start

Run the main script to create databases:

```bash
python Open_Meteo_API.py
```

This will create two databases in the `energy_weather_data/` folder:
- `energy_weather_hourly.db` - Hourly weather data
- `energy_weather_daily.db` - Daily aggregated data

Then verify data quality:

```bash
python Open_Meteo_verifications.py
```

## Configuration

Edit the `__main__` section in `Open_Meteo_API.py` to change:

**Date range:**
```python
start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")  # 1 year
```

**Locations:**
```python
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
    {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
    {'name': 'Lille', 'latitude': 50.6292, 'longitude': 3.0573}
]
```

**Weather variables:**
Check the `DataFetcher.__init__()` method to see available variables or modify them. Some exemples are 
temperature, wind speed, precipitations, and cloud cover.

## API Structure

The code is organized into 4 classes:

```
┌─────────────────────────────────────────────┐
│           OpenMeteoAPI (Main)               │
│  ┌──────────────────────────────────────┐   │
│  │  - fetcher (DataFetcher)             │   │
│  │  - processor (DataProcessor)         │   │
│  │  - storage (DataStorage)             │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
            │           │           │
            ▼           ▼           ▼
    ┌───────────┐  ┌──────────┐  ┌──────────┐
    │DataFetcher│  │Processor │  │ Storage  │
    ├───────────┤  ├──────────┤  ├──────────┤
    │ Fetch API │  │Parse JSON│  │Save Files│
    │   data    │  │  to flat │  │Save to DB│
    │           │  │  records │  │Query DB  │
    └───────────┘  └──────────┘  └──────────┘
         │              │              │
         ▼              ▼              ▼
    JSON from       Records        SQLite DB
      API           (list)         & JSON
```

### 1. DataFetcher
Handles all API requests.

**Hourly data methods:**
- `fetch_forecast_hourly(lat, lon, variables, days)` - Future forecast
- `fetch_historical_hourly(lat, lon, start_date, end_date, variables)` - Historical data
- `fetch_multiple_locations_hourly(locations, start_date, end_date, variables)` - Multiple cities

**Daily data methods:**
- `fetch_forecast_daily(lat, lon, variables, days)` - Future forecast
- `fetch_historical_daily(lat, lon, start_date, end_date, variables)` - Historical data
- `fetch_multiple_locations_daily(locations, start_date, end_date, variables)` - Multiple cities

### 2. DataProcessor
Converts JSON responses to flat database records.

**Main methods:**
- `extract_hourly_records(json_data, location_name)` - Parse hourly JSON
- `extract_daily_records(json_data, location_name)` - Parse daily JSON
- `process_multiple_locations_hourly(multi_location_data)` - Combine multiple locations
- `process_multiple_locations_daily(multi_location_data)` - Combine multiple locations

### 3. DataStorage
Saves data to files and databases.

**Methods:**
- `save_json(data, filepath)` - Save raw JSON
- `save_to_sqlite(records, table_name, db_path)` - Save to database
- `query_sqlite(db_path, query)` - Run SQL queries

### 4. OpenMeteoAPI
Main interface combining all components. Use this class for most operations.

## Usage Examples

### Fetch data for one city

```python
from Open_Meteo_API import OpenMeteoAPI

api = OpenMeteoAPI(output_folder='my_data')

# Get hourly data
data = api.fetch_historical_hourly(
    latitude=48.8566,
    longitude=2.3522,
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Convert to records
records = api.process_to_records_hourly(data, location_name='Paris')

# Save to database
api.save_to_database(records, 'weather_hourly', 'weather.db')
```

### Fetch data for multiple cities

```python
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357}
]

# Fetch all at once
multi_data = api.fetch_multiple_locations_hourly(
    locations, 
    start_date='2024-01-01', 
    end_date='2024-12-31'
)

# Process all together
records = api.process_multiple_locations_hourly(multi_data)

# Save to database
api.save_to_database(records, 'weather_hourly', 'weather.db')
```

### Query the database

```python
import sqlite3

conn = sqlite3.connect('energy_weather_data/energy_weather_hourly.db')
cursor = conn.cursor()

# Get average temperature by city
cursor.execute("""
    SELECT city, AVG(CAST(temperature_2m AS REAL)) as avg_temp
    FROM weather_hourly
    GROUP BY city
""")

for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]:.1f}°C")

conn.close()
```

### Save raw JSON files

```python
# Fetch data
data = api.fetch_historical_hourly(48.8566, 2.3522, '2024-01-01', '2024-12-31')

# Save as JSON
api.save_json(data, 'paris_hourly.json')
```

## Database Schema

### Hourly Table (weather_hourly)
- `datetime` - Timestamp (YYYY-MM-DD HH:MM)
- `city` - City name
- `latitude`, `longitude` - Coordinates
- `timezone` - Timezone string
- `elevation` - Elevation in meters
- Weather variables: `temperature_2m`, `precipitation`, `wind_speed_10m`, `wind_speed_100m`, etc.

### Daily Table (weather_daily)
- `date` - Date (YYYY-MM-DD)
- `city` - City name
- `latitude`, `longitude` - Coordinates
- `timezone` - Timezone string
- `elevation` - Elevation in meters
- Weather variables: `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`, etc.

## Weather Variables

**Hourly variables:**
- Temperature, precipitation, wind speed/direction
- Humidity, pressure, cloud cover
- Solar radiation (shortwave, direct, diffuse)

**Daily variables:**
- Temperature (max, min, mean)
- Precipitation sum and hours
- Wind speed max and gusts
- Solar radiation sum
- Sunshine and daylight duration

See `DataFetcher` class for complete lists.

## Notes

- The script automatically removes duplicates if you run it multiple times
- Free API has rate limits
- Data is stored in SQLite databases (portable, no server needed)
- Uses JMESPath for JSON processing
- All dates are in the location's local timezone

## API Documentation

Open-Meteo API docs: https://open-meteo.com/en/docs
