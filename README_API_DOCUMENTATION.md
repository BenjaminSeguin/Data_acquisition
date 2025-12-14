# OpenMeteo API Documentation

## Overview

This module provides a modular, extensible interface for collecting hourly weather data from the Open-Meteo API and storing it in structured formats (JSON, SQLite).

## Architecture

The code is organized into four main components:

```
┌─────────────────────────────────────────────────────┐
│              OpenMeteoAPI (Main Interface)          │
│  - Combines all components                          │
│  - Provides simple high-level methods               │
└───────────┬─────────────────────────────────────────┘
            │
            ├──► DataFetcher (API Requests)
            │    - fetch_forecast()
            │    - fetch_historical()
            │    - fetch_multiple_locations()
            │
            ├──► DataProcessor (JSON Processing)
            │    - extract_hourly_records()
            │    - extract_specific_variables()
            │    - process_multiple_locations()
            │
            └──► DataStorage (File & Database I/O)
                 - save_json()
                 - save_to_sqlite()
                 - query_sqlite()
```

## Module Components

### 1. DataFetcher Class

**Purpose**: Handles all HTTP requests to Open-Meteo API endpoints.

**Key Methods**:
- `fetch_forecast(lat, lon, variables, days)` - Get forecast data
- `fetch_historical(lat, lon, start_date, end_date, variables)` - Get historical data
- `fetch_multiple_locations(locations, start_date, end_date)` - Fetch data for multiple cities

**Default Variables**:
```python
[
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "relative_humidity_2m"
]
```

### 2. DataProcessor Class

**Purpose**: Transforms API JSON responses into structured records using JMESPath.

**Key Methods**:
- `extract_metadata(json_data)` - Extract location info (lat, lon, timezone, elevation)
- `extract_hourly_records(json_data, location_name)` - Convert to flat record list
- `extract_specific_variables(json_data, variables)` - Extract only specific fields
- `process_multiple_locations(multi_location_data)` - Combine multiple locations

**Record Structure**:
```python
{
    'datetime': '2024-01-01T00:00',
    'city': 'Paris',
    'latitude': 48.8566,
    'longitude': 2.3522,
    'timezone': 'Europe/Paris',
    'elevation': 42.0,
    'temperature_2m': 12.5,
    'precipitation': 0.0,
    'wind_speed_10m': 5.2,
    'relative_humidity_2m': 75.0
}
```

### 3. DataStorage Class

**Purpose**: Manages file and database operations.

**Key Methods**:
- `save_json(data, filepath)` - Save to JSON file
- `load_json(filepath)` - Load from JSON file
- `save_to_sqlite(records, table_name, db_path)` - Save to SQLite
- `query_sqlite(db_path, query)` - Execute SQL queries

**Features**:
- Automatic directory creation
- Dynamic table creation based on record structure
- UTF-8 encoding support

### 4. OpenMeteoAPI Class

**Purpose**: Main user-facing interface that combines all components.

**Initialization**:
```python
api = OpenMeteoAPI(output_folder='temperature_json')
```

**High-Level Methods**:
- Fetch: `fetch_forecast()`, `fetch_historical()`, `fetch_multiple_locations()`
- Process: `process_to_records()`, `process_multiple_locations()`, `extract_variables()`
- Storage: `save_json()`, `save_to_database()`, `query_database()`

## Basic Usage

### Simple Workflow

```python
from Open_Meteo_API import OpenMeteoAPI

# 1. Initialize
api = OpenMeteoAPI(output_folder='weather_data')

# 2. Fetch data
data = api.fetch_historical(
    latitude=48.8566,
    longitude=2.3522,
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# 3. Process to records
records = api.process_to_records(data, location_name='Paris')

# 4. Save to database
api.save_to_database(records, 'weather_hourly')
```

### Multiple Locations

```python
# Define locations
locations = [
    {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
    {'name': 'London', 'latitude': 51.5074, 'longitude': -0.1278},
    {'name': 'Berlin', 'latitude': 52.5200, 'longitude': 13.4050}
]

# Fetch all
multi_data = api.fetch_multiple_locations(
    locations,
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Process all
all_records = api.process_multiple_locations(multi_data)

# Save to database
api.save_to_database(all_records, 'weather_hourly')
```

### Custom Variables

```python
# Define custom weather variables
custom_vars = [
    'temperature_2m',
    'wind_speed_10m',
    'precipitation',
    'cloud_cover',
    'shortwave_radiation'
]

# Fetch with custom variables
data = api.fetch_historical(
    48.8566, 2.3522,
    '2024-01-01', '2024-12-31',
    variables=custom_vars
)
```

## Adding New Data Sources

### Step 1: Add New Endpoint to DataFetcher

```python
class DataFetcher:
    def __init__(self):
        # Existing endpoints
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        
        # Add new endpoint
        self.marine_url = "https://marine-api.open-meteo.com/v1/marine"
    
    def fetch_marine_data(self, latitude, longitude, start_date, end_date):
        """Fetch marine/ocean data"""
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "wave_height,wave_direction,wind_wave_height"
        }
        
        response = requests.get(self.marine_url, params=params)
        return response.json()
```

### Step 2: Add Corresponding Method to OpenMeteoAPI

```python
class OpenMeteoAPI:
    def fetch_marine_data(self, latitude, longitude, start_date, end_date):
        """Fetch marine data. See DataFetcher.fetch_marine_data for details."""
        return self.fetcher.fetch_marine_data(latitude, longitude, start_date, end_date)
```

### Step 3: Use Existing Processing Pipeline

```python
# Fetch new data
marine_data = api.fetch_marine_data(48.8566, 2.3522, '2024-01-01', '2024-12-31')

# Process with existing methods (works automatically!)
records = api.process_to_records(marine_data, 'Paris_Marine')

# Save with existing methods
api.save_to_database(records, 'marine_hourly')
```

## Adding New Storage Formats

### Example: Add CSV Export

```python
class DataStorage:
    def save_to_csv(self, records, filepath):
        """Save records to CSV file"""
        import csv
        
        if not records:
            print("No records to save")
            return
        
        self.ensure_directory(filepath)
        
        # Get column names from first record
        columns = list(records[0].keys())
        
        # Write CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(records)
        
        print(f"✓ {len(records)} records saved to {filepath}")
```

Add to OpenMeteoAPI:
```python
class OpenMeteoAPI:
    def save_to_csv(self, records, filename):
        """Save to CSV file."""
        filepath = os.path.join(self.output_folder, filename)
        self.storage.save_to_csv(records, filepath)
```

## Database Schema

The SQLite tables are created dynamically based on the record structure:

```sql
CREATE TABLE IF NOT EXISTS weather_hourly (
    datetime TEXT,
    city TEXT,
    latitude TEXT,
    longitude TEXT,
    timezone TEXT,
    elevation TEXT,
    temperature_2m TEXT,
    precipitation TEXT,
    wind_speed_10m TEXT,
    relative_humidity_2m TEXT
)
```

**Note**: All columns are TEXT type for simplicity. For production use, consider using proper types (REAL for numbers, etc.).

## SQL Query Examples

```python
# Count records per city
results = api.query_database(
    "SELECT city, COUNT(*) FROM weather_hourly GROUP BY city"
)

# Get average temperature by city
results = api.query_database(
    "SELECT city, AVG(CAST(temperature_2m AS REAL)) as avg_temp FROM weather_hourly GROUP BY city"
)

# Get data for specific date range
results = api.query_database(
    "SELECT * FROM weather_hourly WHERE datetime >= '2024-06-01' AND datetime < '2024-07-01'"
)
```

## Error Handling

The API responses may contain errors. The processor handles this automatically:

```python
# API returns error
{
    "error": true,
    "reason": "Not Found"
}

# extract_hourly_records() will:
# - Print error message
# - Return empty list []
```

## Best Practices

1. **Start with small date ranges** for testing (1 week or 1 month)
2. **Use custom variables** to reduce data size if you don't need all metrics
3. **Save raw JSON** before processing (for debugging and reprocessing)
4. **Query database** instead of reprocessing files for analysis
5. **Use meaningful table names** (e.g., 'weather_hourly', 'marine_data')

## Performance Considerations

- **API Rate Limits**: Open-Meteo is generous but add delays for many locations
- **Memory**: 10 years of hourly data for 5 cities = ~438,000 records (~100MB)
- **Database Size**: SQLite handles millions of records efficiently
- **Processing Time**: Fetching is the bottleneck, processing is fast

## Troubleshooting

### "Not Found" Error
- Check that you're using `archive_url` for historical data
- Verify date format (YYYY-MM-DD)
- Historical data has limits (check Open-Meteo docs)

### Empty Records
- Check that API response has 'hourly' key
- Verify location coordinates are valid
- Check variable names match API documentation

### Database Locked
- Close previous database connections
- Use single connection for multiple operations

## Future Extensions

### Potential Additions:
1. **Pandas Integration**: Add DataFrame export methods
2. **PostgreSQL Support**: Add methods for PostgreSQL databases
3. **Data Validation**: Add schema validation before saving
4. **Parallel Fetching**: Use asyncio for faster multi-location fetches
5. **Caching**: Cache API responses to avoid duplicate requests
6. **Data Aggregation**: Add methods for daily/weekly aggregates

### Example: Adding Pandas Support

```python
class DataProcessor:
    def to_dataframe(self, records):
        """Convert records to pandas DataFrame"""
        import pandas as pd
        return pd.DataFrame(records)

class OpenMeteoAPI:
    def export_to_dataframe(self, records):
        """Export records to pandas DataFrame"""
        return self.processor.to_dataframe(records)
```

## License and Attribution

This module uses the Open-Meteo API (https://open-meteo.com/).
Open-Meteo is free for non-commercial use with attribution.

## Contact and Support

For issues with this module, review the code comments and docstrings.
For API-specific questions, see: https://open-meteo.com/en/docs
