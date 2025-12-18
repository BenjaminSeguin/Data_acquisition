# Energy Data Collection Pipeline

Automated pipeline to fetch weather and energy data from multiple APIs and consolidate them into a unified SQLite database for analysis.

## Overview

This project collects data from three sources:
1. **Open-Meteo API** - Historical weather data for French cities
2. **RTE (Réseau de Transport d'Électricité)** - French power grid data via web scraping
3. **ENTSO-E Transparency Platform** - European energy market data

All data is merged into a single database (`final.db`) with hourly timestamps.

## Requirements

```bash
pip install requests jmespath pandas lxml selenium
```

Additional requirement: ENTSO-E API security token ([register here](https://transparency.entsoe.eu/))

## Quick Start

### 1. Configure API Token

Edit `main.py` and replace the ENTSO-E security token:

```python
security_token="YOUR_TOKEN_HERE"
```

### 2. Run the Pipeline

```bash
python main.py
```

This will:
- Fetch weather data for 5 French cities (2024 hourly + 2015-2024 daily)
- Scrape RTE power grid data (2024)
- Download ENTSO-E energy market data (2024)
- Merge everything into `Database/final.db`

### 3. Output Files

The `Database/` folder will contain:
- `final.db` - **Main database** with merged hourly data
- `weather_hourly.db` - Hourly weather data
- `weather_daily.db` - Daily weather aggregates
- `rte.db` - RTE power grid data
- `entsoe.db` - ENTSO-E market data
- `RTE_data.xls` - Raw RTE download

## Configuration

### Date Ranges

Edit in `main.py`:

```python
# Weather hourly data
start_date_hour = "2023-12-31"
end_date_hour = "2024-12-31"

# Weather daily data
start_date_day = "2015-01-01"
end_date_day = "2024-12-31"

# ENTSO-E data (format: YYYYMMDDHHMM)
periodStart="202312312300"
periodEnd="202412312300"
```

### Locations

Modify the cities in `main.py`:

```python
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
    {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
    {'name': 'Lille', 'latitude': 50.6292, 'longitude': 3.0573}
]
```

## Final Database Schema

The merged database (`final.db`, table: `energy_data`) contains:

### Time Columns
- `datetime` - Timestamp (YYYY-MM-DD HH:MM:SS)

### Weather Data (per city)
- `city`, `latitude`, `longitude`, `timezone`, `elevation`
- `temperature_2m`, `precipitation`, `wind_speed_10m`, `wind_speed_100m`
- `cloud_cover`, `relative_humidity_2m`, `pressure_msl`
- Solar radiation: `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`

### RTE Power Grid Data
- `consommation` - Actual consumption
- `prevision_j1`, `prevision_j` - Forecasts
- Energy sources: `nucleaire`, `eolien`, `solaire`, `hydraulique`, `gaz`, `charbon`, `fioul`, `bioenergies`
- Storage: `pompage`, `stockage_batterie`, `destockage_batterie`
- Exchanges: `ech_physiques`, `ech_comm_angleterre`, `ech_comm_espagne`, etc.
- `taux_co2` - CO2 emission rate

### ENTSO-E Market Data
- `Energy prices` - Market prices
- `Total load forecast` - System load forecast
- `Solar generation forecast`
- `Wind offshore generation forecast`
- `Wind onshore generation forecast`



## Component Details

### 1. Open-Meteo API (Weather Data)

Fetches historical weather data from the Open-Meteo API.

#### API Structure

The code uses 4 classes:

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
```

#### DataFetcher Methods

**Hourly data:**
- `fetch_forecast_hourly(lat, lon, variables, days)` - Future forecast
- `fetch_historical_hourly(lat, lon, start_date, end_date, variables)` - Historical data
- `fetch_multiple_locations_hourly(locations, start_date, end_date, variables)` - Multiple cities

**Daily data:**
- `fetch_forecast_daily(lat, lon, variables, days)` - Future forecast
- `fetch_historical_daily(lat, lon, start_date, end_date, variables)` - Historical data
- `fetch_multiple_locations_daily(locations, start_date, end_date, variables)` - Multiple cities

#### Available Weather Variables

**Hourly:** temperature, precipitation, wind speed/direction, humidity, pressure, cloud cover, solar radiation (shortwave, direct, diffuse)

**Daily:** temperature (max, min, mean), precipitation sum/hours, wind speed max/gusts, solar radiation sum, sunshine/daylight duration

See `DataFetcher.__init__()` in `Functions/Open_Meteo_API.py` for complete lists.

#### Standalone Usage

```python
from Functions.Open_Meteo_API import OpenMeteoAPI

api = OpenMeteoAPI(output_folder='my_data')

# Single city
data = api.fetch_historical_hourly(48.8566, 2.3522, '2024-01-01', '2024-12-31')
records = api.process_to_records_hourly(data, location_name='Paris')
api.save_to_database(records, 'weather_hourly', 'weather.db')

# Multiple cities
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357}
]
multi_data = api.fetch_multiple_locations_hourly(locations, '2024-01-01', '2024-12-31')
records = api.process_multiple_locations_hourly(multi_data)
api.save_to_database(records, 'weather_hourly', 'weather.db')
```

**API Documentation:** https://open-meteo.com/en/docs

---

### 2. RTE API (French Power Grid)

Scrapes energy data from RTE's website using Selenium.

#### RTEAPI Class

**Methods:**
- `open_page()` - Initialize browser
- `download_data(start_date, end_date, final_filename)` - Download data range
- `close_page()` - Close browser

**Date format:** DD/MM/YYYY (e.g., "01/01/2024")

#### RTEDatabase Class

Converts downloaded .xls files (TSV format) to SQLite.

**Methods:**
- `create_database_from_folder(folder_path, db_path, file_pattern, table_name)` - Process all files

#### Standalone Usage

```python
from Functions.RTE_API import RTEAPI
from Functions.RTE_to_database import RTEDatabase

# Download data
rte = RTEAPI('output_folder')
rte.open_page()
rte.download_data("01/01/2024", "31/12/2024", final_filename="RTE_data.xls")
rte.close_page()

# Convert to database
rte_db = RTEDatabase()
rte_db.create_database_from_folder(
    folder_path='output_folder',
    db_path='rte_energy.db',
    file_pattern="RTE_*.xls",
    table_name="energy_data"
)
```

#### Data Handling
- Only rows with actual consumption data are stored
- Automatic handling of duplicates (INSERT OR REPLACE)
- Null/missing values stored as NULL

---

### 3. ENTSO-E Transparency Platform API

Fetches European energy market data from ENTSO-E.

#### Main Functions

**`get_transp_api()`** - Retrieves data from ENTSO-E API

**Parameters:**
- `req_params` (dict) - Required parameters (e.g., `{"documentType": "A69", "processType": "A01"}`)
- `opt_params` (dict) - Optional parameters
- `domains` (dict) - Domain arguments (e.g., `{"in_Domain": "10YFR-RTE------C"}`)
- `periodStart` (str) - Start period (YYYYMMDDHHMM format)
- `periodEnd` (str) - End period (YYYYMMDDHHMM format)
- `security_token` (str) - Your API token
- `return_url` (bool) - If True, returns URL instead of data

**Returns:** Pandas DataFrame with hourly data

**How it works:**
1. Constructs API URL and retrieves XML response
2. Checks for errors and identifies value element names
3. Detects metadata differences between TimeSeries
4. Extracts time intervals and converts positions to timestamps
5. Handles mixed resolutions (15-min and 60-min data)
6. Filters for exact hours to ensure clean hourly data

**`process()`** - Consolidates multiple DataFrames

**Parameters:**
- `df_list` (list) - List of DataFrames from `get_transp_api()`

**Returns:** Wide-format DataFrame with one row per timestamp

**How it works:**
1. Translates parameter codes to definitions (e.g., "B19" → "Wind Onshore")
2. Creates description columns from metadata
3. Converts to wide format (each description becomes a column)
4. Converts to Paris timezone and sorts chronologically

#### Adding More Time Series

```python
req_params = {"documentType": "A44"}
opt_params = {}
domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

new_data = get_transp_api(
    req_params=req_params,
    opt_params=opt_params,
    domains=domains,
    periodStart="202401010000",
    periodEnd="202412312300",
    security_token="YOUR_TOKEN"
)

# Add to processing list
data_list = [data_generation, data_market, data_load, new_data]
final_data = process(data_list)
```

#### Important Notes

- French domain code: `10YFR-RTE------C`
- For other countries, see [ENTSO-E Area Codes](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_areas)
- Generate security token from [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)
- Available parameters: [ENTSO-E Documentation](https://transparencyplatform.zendesk.com/hc/en-us/articles/15856744319380-Available-Parameters)

#### Standalone Usage

```python
from Functions.transparency_api import get_transp_api, process

# Fetch generation data
data_gen = get_transp_api(
    req_params={"documentType": "A69", "processType": "A01"},
    opt_params={},
    domains={"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"},
    periodStart="202401010000",
    periodEnd="202412312300",
    security_token="YOUR_TOKEN"
)

# Fetch market prices
data_prices = get_transp_api(
    req_params={"documentType": "A44"},
    opt_params={},
    domains={"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"},
    periodStart="202401010000",
    periodEnd="202412312300",
    security_token="YOUR_TOKEN"
)

# Process and merge
final_data = process([data_gen, data_prices])
```

---

## Notes

- Free APIs have rate limits (60-second delay between Open-Meteo calls in main.py)
- All timestamps are in Paris/CET timezone in final database
- Automatic duplicate removal for weather data
- Database uses `INSERT OR REPLACE` for handling duplicates
- Chrome browser required for RTE scraping

## Troubleshooting

**ENTSO-E API errors:** Check your security token and date format (YYYYMMDDHHMM)

**RTE scraping fails:** Ensure Chrome is installed and website structure hasn't changed

**Missing data in final.db:** Check individual databases first (weather_hourly.db, rte.db, entsoe.db)

**Rate limiting:** Add delays between API calls if you hit limits