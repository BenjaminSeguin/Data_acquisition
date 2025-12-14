# Quick Start Guide - OpenMeteo API

## Installation

Install required packages:
```powershell
pip install requests jmespath
```

## Basic Usage

### 1. Single Location - Quick Example

```python
from Open_Meteo_API import OpenMeteoAPI
from datetime import datetime, timedelta

# Initialize
api = OpenMeteoAPI(output_folder='weather_data')

# Get last year of data
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

# Fetch data for Paris
data = api.fetch_historical(48.8566, 2.3522, start_date, end_date)

# Convert to records
records = api.process_to_records(data, 'Paris')

# Save to database
api.save_to_database(records, 'weather_hourly')

print(f"✓ Saved {len(records)} records to database")
```

### 2. Multiple Locations

```python
# Define locations
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
    {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698}
]

# Fetch all at once
multi_data = api.fetch_multiple_locations(locations, start_date, end_date)

# Process all
all_records = api.process_multiple_locations(multi_data)

# Save to database
api.save_to_database(all_records, 'weather_hourly')
```

### 3. Query Your Data

```python
# Count records per city
results = api.query_database(
    "SELECT city, COUNT(*) as count FROM weather_hourly GROUP BY city"
)

for row in results:
    print(f"{row[0]}: {row[1]} records")
```

### 4. Custom Weather Variables

```python
# Define variables you want
variables = [
    'temperature_2m',
    'wind_speed_10m', 
    'precipitation',
    'cloud_cover',
    'pressure_msl'
]

# Fetch with custom variables
data = api.fetch_historical(
    48.8566, 2.3522,
    start_date, end_date,
    variables=variables
)
```

## Complete Workflow Example

```python
from Open_Meteo_API import OpenMeteoAPI
from datetime import datetime, timedelta

# Setup
api = OpenMeteoAPI(output_folder='temperature_json')

# Date range (10 years)
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")

# French cities
cities = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
    {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
    {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
    {'name': 'Nice', 'latitude': 43.7102, 'longitude': 7.2620}
]

# Weather variables
variables = [
    'temperature_2m',
    'precipitation',
    'wind_speed_10m',
    'relative_humidity_2m',
    'cloud_cover'
]

print("=== Fetching data ===")
# Fetch data
multi_data = api.fetch_multiple_locations(cities, start_date, end_date, variables)

print("\n=== Saving raw JSON ===")
# Save raw JSON per city
for city_name, data in multi_data.items():
    api.save_json(data, f'{city_name.lower()}_hourly_10y.json')

print("\n=== Processing records ===")
# Process to records
all_records = api.process_multiple_locations(multi_data)
print(f"Total records: {len(all_records):,}")

print("\n=== Saving to database ===")
# Save to database
api.save_to_database(all_records, 'weather_hourly')

print("\n=== Analyzing data ===")
# Query for statistics
results = api.query_database("""
    SELECT 
        city, 
        COUNT(*) as record_count,
        MIN(datetime) as first_date,
        MAX(datetime) as last_date
    FROM weather_hourly 
    GROUP BY city
""")

print("\nData Summary:")
for row in results:
    print(f"  {row[0]}: {row[1]} records from {row[2]} to {row[3]}")

print("\n✓ Complete!")
```

## Available Weather Variables

### Temperature
- `temperature_2m` - Temperature at 2m height (°C)
- `temperature_80m` - Temperature at 80m height (°C)
- `apparent_temperature` - Feels-like temperature (°C)
- `dew_point_2m` - Dew point temperature (°C)

### Precipitation
- `precipitation` - Total precipitation (mm)
- `rain` - Rain only (mm)
- `snowfall` - Snowfall amount (cm)

### Wind
- `wind_speed_10m` - Wind speed at 10m (km/h)
- `wind_speed_80m` - Wind speed at 80m (km/h)
- `wind_direction_10m` - Wind direction (°)
- `wind_gusts_10m` - Wind gusts (km/h)

### Humidity & Clouds
- `relative_humidity_2m` - Relative humidity (%)
- `cloud_cover` - Total cloud cover (%)
- `cloud_cover_low` - Low level clouds (%)
- `cloud_cover_mid` - Mid level clouds (%)
- `cloud_cover_high` - High level clouds (%)

### Solar & Radiation
- `shortwave_radiation` - Solar radiation (W/m²)
- `direct_radiation` - Direct solar radiation (W/m²)
- `diffuse_radiation` - Diffuse solar radiation (W/m²)

### Pressure
- `pressure_msl` - Mean sea level pressure (hPa)
- `surface_pressure` - Surface pressure (hPa)

### Other
- `evapotranspiration` - Evapotranspiration (mm)
- `soil_temperature_0cm` - Surface soil temperature (°C)
- `visibility` - Visibility (m)

## Common SQL Queries

### Average Temperature by City
```python
results = api.query_database("""
    SELECT 
        city,
        AVG(CAST(temperature_2m AS REAL)) as avg_temp
    FROM weather_hourly
    GROUP BY city
    ORDER BY avg_temp DESC
""")
```

### Monthly Precipitation
```python
results = api.query_database("""
    SELECT 
        city,
        strftime('%Y-%m', datetime) as month,
        SUM(CAST(precipitation AS REAL)) as total_precip
    FROM weather_hourly
    GROUP BY city, month
    ORDER BY city, month
""")
```

### Maximum Wind Speed
```python
results = api.query_database("""
    SELECT 
        city,
        MAX(CAST(wind_speed_10m AS REAL)) as max_wind,
        datetime as when_occurred
    FROM weather_hourly
    GROUP BY city
    ORDER BY max_wind DESC
""")
```

### Hourly Averages
```python
results = api.query_database("""
    SELECT 
        city,
        CAST(strftime('%H', datetime) AS INTEGER) as hour,
        AVG(CAST(temperature_2m AS REAL)) as avg_temp
    FROM weather_hourly
    GROUP BY city, hour
    ORDER BY city, hour
""")
```

## Tips

1. **Start Small**: Test with 1 week of data first
2. **Save Raw JSON**: Always save raw responses before processing
3. **Check Data**: Print first few records to verify structure
4. **Use Descriptive Names**: Name tables and files clearly
5. **Query Often**: Use SQL to analyze instead of reprocessing

## Troubleshooting

### Problem: "Not Found" error
**Solution**: Using historical endpoint? Date range valid? Check API limits.

### Problem: Empty records
**Solution**: Check variable names match API docs exactly.

### Problem: Slow fetching
**Solution**: Normal! Fetching 10 years × 5 cities takes time. Be patient.

### Problem: Large file sizes
**Solution**: Use custom variables to reduce data size.

## Next Steps

1. Run the examples in `Open_Meteo_API.py`
2. Review `README_API_DOCUMENTATION.md` for full details
3. Customize for your specific needs
4. Add new data sources as needed

## Support

- Open-Meteo API Docs: https://open-meteo.com/en/docs
- Check code comments for detailed explanations
- Review docstrings in each class/method
