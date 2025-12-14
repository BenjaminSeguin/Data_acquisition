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


# ============================================================================
# DATA FETCHER - Handles all API interactions
# ============================================================================

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
        self.default_variables = [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m"
        ]
    
    def fetch_forecast(self, latitude, longitude, variables=None, days=7):
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
            variables = self.default_variables
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(variables),
            "forecast_days": days,
            "timezone": "auto"
        }
        
        response = requests.get(self.forecast_url, params=params)
        return response.json()
    
    def fetch_historical(self, latitude, longitude, start_date, end_date, variables=None):
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
            variables = self.default_variables
        
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
    
    def fetch_multiple_locations(self, locations, start_date, end_date, variables=None):
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
            print(f"Fetching data for {location['name']}...")
            data = self.fetch_historical(
                location['latitude'],
                location['longitude'],
                start_date,
                end_date,
                variables
            )
            results[location['name']] = data
        
        return results


# ============================================================================
# DATA PROCESSOR - Transforms JSON to structured records
# ============================================================================

class DataProcessor:
    """
    Processes API JSON responses into structured records.
    
    Uses JMESPath for flexible JSON querying and transformation.
    
    To add new processing patterns:
    1. Create new JMESPath queries in extract methods
    2. Add transformation logic in new methods
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
    
    def extract_specific_variables(self, json_data, variables):
        """
        Extract only specific variables using JMESPath.
        
        Useful for reducing data size or focusing on specific metrics.
        
        Args:
            json_data (dict): API response JSON
            variables (list): List of variable names to extract
        
        Returns:
            dict: Dictionary with requested variables and timestamps
        """
        var_queries = ", ".join([f"{var}: hourly.{var}" for var in variables])
        query = f"{{{var_queries}, time: hourly.time}}"
        
        return jmespath.search(query, json_data)
    
    def process_multiple_locations(self, multi_location_data):
        """
        Process multiple locations into combined records list.
        
        Args:
            multi_location_data (dict): Dictionary with location names as keys
        
        Returns:
            list: Combined list of all records from all locations
        """
        all_records = []
        
        for location_name, json_data in multi_location_data.items():
            records = self.extract_hourly_records(json_data, location_name)
            all_records.extend(records)
            print(f"✓ Processed {len(records)} records for {location_name}")
        
        return all_records


# ============================================================================
# DATA STORAGE - Handles file and database operations
# ============================================================================

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
        
        Creates table dynamically based on record structure.
        
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


# ============================================================================
# MAIN API CLASS - Combines all components
# ============================================================================

class OpenMeteoAPI:
    """
    Main interface for weather data collection and processing.
    
    This class combines the DataFetcher, DataProcessor, and DataStorage
    components to provide a simple, high-level API.
    
    Basic workflow:
    1. Fetch data using fetch_* methods
    2. Process data using process_* methods
    3. Save data using save_* methods
    
    Example:
        api = OpenMeteoAPI()
        
        # Fetch data
        data = api.fetch_historical(48.8566, 2.3522, '2024-01-01', '2024-12-31')
        
        # Process to records
        records = api.process_to_records(data, 'Paris')
        
        # Save to database
        api.save_to_database(records, 'weather_data', 'weather.db')
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
    
    # === Fetch Methods (delegate to DataFetcher) ===
    
    def fetch_forecast(self, latitude, longitude, variables=None, days=7):
        """Fetch hourly forecast data. See DataFetcher.fetch_forecast for details."""
        return self.fetcher.fetch_forecast(latitude, longitude, variables, days)
    
    def fetch_historical(self, latitude, longitude, start_date, end_date, variables=None):
        """Fetch hourly historical data. See DataFetcher.fetch_historical for details."""
        return self.fetcher.fetch_historical(latitude, longitude, start_date, end_date, variables)
    
    def fetch_multiple_locations(self, locations, start_date, end_date, variables=None):
        """Fetch data for multiple locations. See DataFetcher.fetch_multiple_locations for details."""
        return self.fetcher.fetch_multiple_locations(locations, start_date, end_date, variables)
    
    # === Process Methods (delegate to DataProcessor) ===
    
    def process_to_records(self, json_data, location_name=None):
        """Convert JSON to records list. See DataProcessor.extract_hourly_records for details."""
        return self.processor.extract_hourly_records(json_data, location_name)
    
    def process_multiple_locations(self, multi_location_data):
        """Process multiple locations. See DataProcessor.process_multiple_locations for details."""
        return self.processor.process_multiple_locations(multi_location_data)
    
    def extract_variables(self, json_data, variables):
        """Extract specific variables. See DataProcessor.extract_specific_variables for details."""
        return self.processor.extract_specific_variables(json_data, variables)
    
    # === Storage Methods (delegate to DataStorage) ===
    
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




# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    # Initialize API with output folder
    api = OpenMeteoAPI(output_folder='open_meteo_data')
    
    # Define date range (last 1 year for testing - change to 10 years if needed)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    # Define locations
    locations = [
        {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
        {'name': 'Toulouse', 'latitude': 43.6047, 'longitude': 1.4442},
        {'name': 'Lyon', 'latitude': 45.7640, 'longitude': 4.8357},
        {'name': 'Marseille', 'latitude': 43.2965, 'longitude': 5.3698},
        {'name': 'Lille', 'latitude': 50.6292, 'longitude': 3.0573}
    ]
    
    # Example 1: Fetch and save raw JSON for multiple locations
    print("=== Fetching hourly data for multiple locations ===")
    multi_location_data = api.fetch_multiple_locations(locations, start_date, end_date)
    
    # Save raw JSON for each location
    for location_name, data in multi_location_data.items():
        api.save_json(data, f'{location_name.lower()}_hourly.json')
    
    # Example 2: Process to structured records
    print("\n=== Processing to structured records ===")
    all_records = api.process_multiple_locations(multi_location_data)
    print(f"Total records: {len(all_records)}")
    print(f"Sample record: {all_records[0]}")
    
    # Example 3: Save processed records to JSON
    print("\n=== Saving processed records ===")
    api.save_json(all_records, 'all_locations_processed.json')
    
    # Example 4: Save to SQLite database
    print("\n=== Saving to database ===")
    api.save_to_database(all_records, 'weather_hourly')
    
    # Example 5: Query database
    print("\n=== Querying database ===")
    results = api.query_database("SELECT city, COUNT(*) as record_count FROM weather_hourly GROUP BY city")
    print("Records per city:")
    for row in results:
        print(f"  {row[0]}: {row[1]} records")
    
    # Example 6: Single location with custom variables
    print("\n=== Fetching single location with custom variables ===")
    custom_vars = ['temperature_2m', 'wind_speed_10m', 'precipitation']
    paris_data = api.fetch_historical(48.8566, 2.3522, start_date, end_date, custom_vars)
    paris_records = api.process_to_records(paris_data, 'Paris')
    print(f"Paris records with custom variables: {len(paris_records)}")
    
    print("\nAll examples complete!")