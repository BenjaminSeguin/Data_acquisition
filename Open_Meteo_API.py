"""
OpenMeteo API Client - Hourly Weather Data Collection and Processing

This module provides a modular interface for:
1. Fetching hourly weather data from Open-Meteo API
2. Processing JSON responses using JMESPath
3. Converting data to structured records for database storage
4. Saving data to JSON files and SQLite databases

Main Components:
- DataFetcher: Handles all API requests
- DataProcessor: Processes JSON to structured records
- DataStorage: Manages file and database operations
- OpenMeteoAPI: Main interface combining all components
"""

import requests
import json
from datetime import datetime, timedelta
import jmespath
import sqlite3
import os
import time

class DataFetcher:
    """
    Handles all API requests to Open-Meteo services.
    
    To add new data sources:
    1. Add endpoint URL as class attribute
    2. Create a new fetch method following the pattern below
    """
    
    def __init__(self):
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        
        # Hourly variables
        self.default_hourly_variables = [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "wind_speed_100m",
            "wind_direction_10m",
            "relative_humidity_2m",
            "surface_pressure",
            "cloud_cover",
            "shortwave_radiation",
            "direct_radiation",
            "diffuse_radiation"
        ]
        
        # Daily variables
        self.default_daily_variables = [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
            "shortwave_radiation_sum",
            "sunshine_duration",
            "daylight_duration"
        ]
    
    def fetch_forecast_hourly(self, latitude, longitude, variables=None, days=7):
        """
        Fetch hourly forecast data for a single location.
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            variables (list, optional): Weather variables to fetch
            days (int): Number of forecast days
        
        Returns:
            dict: JSON response from API
        """
        if variables is None:
            variables = self.default_hourly_variables
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(variables),
            "forecast_days": days,
            "timezone": "auto"
        }
        
        response = requests.get(self.forecast_url, params=params)
        return response.json()
    
    def fetch_forecast_daily(self, latitude, longitude, variables=None, days=7):
        """
        Fetch daily forecast data for a single location.
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            variables (list, optional): Weather variables to fetch
            days (int): Number of forecast days
        
        Returns:
            dict: JSON response from API
        """
        if variables is None:
            variables = self.default_daily_variables
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": ",".join(variables),
            "forecast_days": days,
            "timezone": "auto"
        }
        
        response = requests.get(self.forecast_url, params=params)
        return response.json()
    
    def fetch_historical_hourly(self, latitude, longitude, start_date, end_date, variables=None):
        """
        Fetch hourly historical data for a single location.
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            variables (list, optional): Weather variables to fetch
        
        Returns:
            dict: JSON response from API
        """
        if variables is None:
            variables = self.default_hourly_variables
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(variables),
            "timezone": "auto"
        }
        
        response = requests.get(self.archive_url, params=params)
        return response.json()
    
    def fetch_historical_daily(self, latitude, longitude, start_date, end_date, variables=None):
        """
        Fetch daily historical data for a single location.
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            variables (list, optional): Weather variables to fetch
        
        Returns:
            dict: JSON response from API
        """
        if variables is None:
            variables = self.default_daily_variables
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(variables),
            "timezone": "auto"
        }
        
        response = requests.get(self.archive_url, params=params)
        return response.json()
    
    def fetch_multiple_locations_hourly(self, locations, start_date, end_date, variables=None):
        """
        Fetch hourly historical data for multiple locations.
        
        Args:
            locations (list): List of dicts with 'name', 'latitude', 'longitude'
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            variables (list, optional): Weather variables to fetch
        
        Returns:
            dict: Dictionary with location names as keys and JSON data as values
        """
        results = {}
        
        for location in locations:
            print(f"Fetching hourly data for {location['name']}...")
            data = self.fetch_historical_hourly(
                location['latitude'],
                location['longitude'],
                start_date,
                end_date,
                variables
            )
            results[location['name']] = data
        
        return results
    
    def fetch_multiple_locations_daily(self, locations, start_date, end_date, variables=None):
        """
        Fetch daily historical data for multiple locations.
        
        Args:
            locations (list): List of dicts with 'name', 'latitude', 'longitude'
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            variables (list, optional): Weather variables to fetch
        
        Returns:
            dict: Dictionary with location names as keys and JSON data as values
        """
        results = {}
        
        for location in locations:
            print(f"Fetching daily data for {location['name']}")
            data = self.fetch_historical_daily(
                location['latitude'],
                location['longitude'],
                start_date,
                end_date,
                variables
            )
            results[location['name']] = data
            time.sleep(20)
        
        return results

class DataProcessor:
    """
    Processes API JSON responses into structured records.
    Uses JMESPath for JSON querying and transformation.
    """
    
    def extract_metadata(self, json_data):
        """
        Extract location metadata from API response.
        
        Args:
            json_data (dict): API response JSON
        
        Returns:
            dict: Metadata including latitude, longitude, timezone, elevation
        """
        query = """{
            latitude: latitude,
            longitude: longitude,
            timezone: timezone,
            elevation: elevation
        }"""
        return jmespath.search(query, json_data)
    
    def extract_hourly_records(self, json_data, location_name=None):
        """
        Convert hourly JSON data to flat records list.
        
        Each record is a dictionary with:
        - datetime: timestamp
        - location metadata (city, lat, lon, timezone, elevation)
        - all weather variables
        
        Args:
            json_data (dict): API response JSON
            location_name (str, optional): Name to add to each record
        
        Returns:
            list: List of record dictionaries ready for database insertion
        """
        # Validate response
        if json_data.get('error'):
            print(f"Error: {json_data.get('reason')}")
            return []
        
        # Extract metadata
        metadata = self.extract_metadata(json_data)
        
        # Get hourly data section
        hourly_data = json_data.get('hourly', {})
        if not hourly_data:
            print("No hourly data found")
            return []
        
        # Get time array
        timestamps = hourly_data.get('time', [])
        
        # Build records
        records = []
        for i, timestamp in enumerate(timestamps):
            # Start with timestamp and metadata
            record = {
                'datetime': timestamp,
                'city': location_name,
                'latitude': metadata.get('latitude'),
                'longitude': metadata.get('longitude'),
                'timezone': metadata.get('timezone'),
                'elevation': metadata.get('elevation')
            }
            
            # Add all weather variables
            for variable, values in hourly_data.items():
                if variable != 'time' and i < len(values):
                    record[variable] = values[i]
            
            records.append(record)
        
        return records
    
    def extract_daily_records(self, json_data, location_name=None):
        """
        Convert daily JSON data to flat records list.
        
        Each record is a dictionary with:
        - date: date string
        - location metadata (city, lat, lon, timezone, elevation)
        - all weather variables
        
        Args:
            json_data (dict): API response JSON
            location_name (str, optional): Name to add to each record
        
        Returns:
            list: List of record dictionaries ready for database insertion
        """
        # Validate response
        if json_data.get('error'):
            print(f"Error: {json_data.get('reason')}")
            return []
        
        # Extract metadata
        metadata = self.extract_metadata(json_data)
        
        # Get daily data section
        daily_data = json_data.get('daily', {})
        if not daily_data:
            print("No daily data found")
            return []
        
        # Get time array
        dates = daily_data.get('time', [])
        
        # Build records
        records = []
        for i, date in enumerate(dates):
            # Start with date and metadata
            record = {
                'date': date,
                'city': location_name,
                'latitude': metadata.get('latitude'),
                'longitude': metadata.get('longitude'),
                'timezone': metadata.get('timezone'),
                'elevation': metadata.get('elevation')
            }
            
            # Add all weather variables
            for variable, values in daily_data.items():
                if variable != 'time' and i < len(values):
                    record[variable] = values[i]
            
            records.append(record)
        
        return records
    
    def extract_specific_variables(self, json_data, variables, data_type='hourly'):
        """
        Extract only specific variables using JMESPath.
        
        Useful for reducing data size or focusing on specific metrics.
        
        Args:
            json_data (dict): API response JSON
            variables (list): List of variable names to extract
            data_type (str): 'hourly' or 'daily'
        
        Returns:
            dict: Dictionary with requested variables and timestamps
        """
        var_queries = ", ".join([f"{var}: {data_type}.{var}" for var in variables])
        query = f"{{{var_queries}, time: {data_type}.time}}"
        
        return jmespath.search(query, json_data)
    
    def process_multiple_locations_hourly(self, multi_location_data):
        """
        Process multiple locations hourly data into combined records list.
        
        Args:
            multi_location_data (dict): Dictionary with location names as keys
        
        Returns:
            list: Combined list of all hourly records from all locations
        """
        all_records = []
        
        for location_name, json_data in multi_location_data.items():
            records = self.extract_hourly_records(json_data, location_name)
            all_records.extend(records)
            print(f"Processed {len(records)} hourly records for {location_name}")
        
        return all_records
    
    def process_multiple_locations_daily(self, multi_location_data):
        """
        Process multiple locations daily data into combined records list.
        
        Args:
            multi_location_data (dict): Dictionary with location names as keys
        
        Returns:
            list: Combined list of all daily records from all locations
        """
        all_records = []
        
        for location_name, json_data in multi_location_data.items():
            records = self.extract_daily_records(json_data, location_name)
            all_records.extend(records)
            print(f"Processed {len(records)} daily records for {location_name}")
        
        return all_records

class DataStorage:
    """
    Manages saving data to files and databases.
    
    Supports:
    - JSON file storage
    - SQLite database storage
    - Automatic directory creation
    
    To add new storage formats:
    1. Create new save method following naming pattern save_to_*
    2. Add necessary imports and error handling
    """
    
    @staticmethod
    def ensure_directory(filepath):
        """Create directory for file if it doesn't exist."""
        directory = os.path.dirname(filepath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
    
    def save_json(self, data, filepath):
        """
        Save data to JSON file.
        
        Args:
            data: Data to save (dict or list)
            filepath (str): Path to output file
        """
        self.ensure_directory(filepath)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to {filepath}")
    
    def load_json(self, filepath):
        """
        Load data from JSON file.
        
        Args:
            filepath (str): Path to input file
        
        Returns:
            Data from JSON file
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def save_to_sqlite(self, records, table_name, db_path):
        """
        Save records to SQLite database.
        
        Creates table based on record structure.
        
        Args:
            records (list): List of record dictionaries
            table_name (str): Name for database table
            db_path (str): Path to SQLite database file
        """
        if not records:
            print("No records to save")
            return
        
        self.ensure_directory(db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table based on first record's keys
        columns = list(records[0].keys())
        col_defs = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})")
        
        # Insert all records
        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        for record in records:
            cursor.execute(insert_query, [record[col] for col in columns])
        
        conn.commit()
        conn.close()
        
        print(f"{len(records)} records saved to table '{table_name}' in {db_path}")
    
    def query_sqlite(self, db_path, query):
        """
        Execute SQL query on database.
        
        Args:
            db_path (str): Path to SQLite database
            query (str): SQL query to execute
        
        Returns:
            list: Query results
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        return results

# Main API Class
class OpenMeteoAPI:
    """
    Main interface for weather data collection and processing.
    
    This class combines the DataFetcher, DataProcessor, and DataStorage
    components.
    
    Basic workflow:
    1. Fetch data using fetch_* methods
    2. Process data using process_* methods
    3. Save data using save_* methods
    """
    
    def __init__(self, output_folder='weather_data'):
        """
        Initialize API client.
        
        Args:
            output_folder (str): Default folder for output files
        """
        self.fetcher = DataFetcher()
        self.processor = DataProcessor()
        self.storage = DataStorage()
        self.output_folder = output_folder
    
    # Fetch Methods 
    
    # Hourly methods
    def fetch_forecast_hourly(self, latitude, longitude, variables=None, days=7):
        """Fetch hourly forecast data. See DataFetcher.fetch_forecast_hourly for details."""
        return self.fetcher.fetch_forecast_hourly(latitude, longitude, variables, days)
    
    def fetch_historical_hourly(self, latitude, longitude, start_date, end_date, variables=None):
        """Fetch hourly historical data. See DataFetcher.fetch_historical_hourly for details."""
        return self.fetcher.fetch_historical_hourly(latitude, longitude, start_date, end_date, variables)
    
    def fetch_multiple_locations_hourly(self, locations, start_date, end_date, variables=None):
        """Fetch hourly data for multiple locations. See DataFetcher.fetch_multiple_locations_hourly for details."""
        return self.fetcher.fetch_multiple_locations_hourly(locations, start_date, end_date, variables)
    
    # Daily methods
    def fetch_forecast_daily(self, latitude, longitude, variables=None, days=7):
        """Fetch daily forecast data. See DataFetcher.fetch_forecast_daily for details."""
        return self.fetcher.fetch_forecast_daily(latitude, longitude, variables, days)
    
    def fetch_historical_daily(self, latitude, longitude, start_date, end_date, variables=None):
        """Fetch daily historical data. See DataFetcher.fetch_historical_daily for details."""
        return self.fetcher.fetch_historical_daily(latitude, longitude, start_date, end_date, variables)
    
    def fetch_multiple_locations_daily(self, locations, start_date, end_date, variables=None):
        """Fetch daily data for multiple locations. See DataFetcher.fetch_multiple_locations_daily for details."""
        return self.fetcher.fetch_multiple_locations_daily(locations, start_date, end_date, variables)
    
    # Process Methods
    
    # Hourly processing
    def process_to_records_hourly(self, json_data, location_name=None):
        """Convert hourly JSON to records list. See DataProcessor.extract_hourly_records for details."""
        return self.processor.extract_hourly_records(json_data, location_name)
    
    def process_multiple_locations_hourly(self, multi_location_data):
        """Process multiple locations hourly data. See DataProcessor.process_multiple_locations_hourly for details."""
        return self.processor.process_multiple_locations_hourly(multi_location_data)
    
    # Daily processing
    def process_to_records_daily(self, json_data, location_name=None):
        """Convert daily JSON to records list. See DataProcessor.extract_daily_records for details."""
        return self.processor.extract_daily_records(json_data, location_name)
    
    def process_multiple_locations_daily(self, multi_location_data):
        """Process multiple locations daily data. See DataProcessor.process_multiple_locations_daily for details."""
        return self.processor.process_multiple_locations_daily(multi_location_data)
    
    # Variable extraction
    def extract_variables(self, json_data, variables, data_type='hourly'):
        """Extract specific variables. See DataProcessor.extract_specific_variables for details."""
        return self.processor.extract_specific_variables(json_data, variables, data_type)
    
    # Storage Methods
    
    def save_json(self, data, filename):
        """Save to JSON file. See DataStorage.save_json for details."""
        filepath = os.path.join(self.output_folder, filename)
        self.storage.save_json(data, filepath)
    
    def save_to_database(self, records, table_name, db_filename='weather_data.db'):
        """Save to SQLite database. See DataStorage.save_to_sqlite for details."""
        db_path = os.path.join(self.output_folder, db_filename)
        self.storage.save_to_sqlite(records, table_name, db_path)
    
    def query_database(self, query, db_filename='weather_data.db'):
        """Query SQLite database. See DataStorage.query_sqlite for details."""
        db_path = os.path.join(self.output_folder, db_filename)
        return self.storage.query_sqlite(db_path, query)

# Create daily and hourly databases
if __name__ == "__main__":
    # Initialize API with output folder
    api = OpenMeteoAPI(output_folder='energy_weather_data')
    
    # Define date range 
    end_date_hour = datetime.now().strftime("%Y-%m-%d")
    start_date_hour = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    end_date_day = datetime.now().strftime("%Y-%m-%d")
    start_date_day = (datetime.now() - timedelta(days=10*365)).strftime("%Y-%m-%d")
    
    # Define locations
    locations = [
        {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
        {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
        {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
        {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
        {'name': 'Lille', 'latitude': 50.6292, 'longitude': 3.0573}
    ]
    
    # Collect hourly data
    print("\nFetching hourly data")
    hourly_data = api.fetch_multiple_locations_hourly(locations, start_date_hour, end_date_hour)
    
    print("Processing hourly records")
    hourly_records = api.process_multiple_locations_hourly(hourly_data)
    api.save_to_database(hourly_records, 'weather_hourly', 'energy_weather_hourly.db')

    time.sleep(60)
    
    # Collect daily data
    print("Fetching daily data")
    daily_data = api.fetch_multiple_locations_daily(locations, start_date_day, end_date_day)
    
    print("Processing daily records")
    daily_records = api.process_multiple_locations_daily(daily_data)
    api.save_to_database(daily_records, 'weather_daily', 'energy_weather_daily.db')
    
    # Remove duplicates from both databases
    print("\nRemoving duplicates")
    
    for db_file, table, time_col in [('energy_weather_hourly.db', 'weather_hourly', 'datetime'), 
                                      ('energy_weather_daily.db', 'weather_daily', 'date')]:
        conn = sqlite3.connect(f'energy_weather_data/{db_file}')
        cursor = conn.cursor()
        
        cursor.execute(f"""
            DELETE FROM {table} 
            WHERE rowid NOT IN (
                SELECT MIN(rowid) 
                FROM {table} 
                GROUP BY {time_col}, city
            )
        """)
        
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"Removed {deleted} duplicates from {table}")
        
        conn.commit()
        conn.close()
    
    print("\nDatabases created.")
