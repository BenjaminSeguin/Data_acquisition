import requests
import json
from datetime import datetime, timedelta
import jmespath

class OpenMeteoAPI:
    def __init__(self):
        self.base_url = "https://api.open-meteo.com/v1"
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
    
    # ===== HOURLY DATA METHODS =====
    
    def get_forecast_hourly(self, latitude, longitude, variables=None, days=7):
        """Get hourly forecast data in JSON format"""
        if variables is None:
            variables = [
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "relative_humidity_2m"
            ]
        
        endpoint = f"{self.base_url}/forecast"
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(variables),
            "forecast_days": days,
            "timezone": "auto"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    def get_historical_hourly(self, latitude, longitude, start_date, end_date, variables=None):
        """Get hourly historical data in JSON format"""
        if variables is None:
            variables = [
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "relative_humidity_2m"
            ]
        
        endpoint = self.archive_url
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(variables),
            "timezone": "auto"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    # ===== DAILY DATA METHODS =====
    
    def get_forecast_daily(self, latitude, longitude, variables=None, days=7):
        """Get daily forecast data in JSON format"""
        if variables is None:
            variables = [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max"
            ]
        
        endpoint = f"{self.base_url}/forecast"
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": ",".join(variables),
            "forecast_days": days,
            "timezone": "auto"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    def get_historical_daily(self, latitude, longitude, start_date, end_date, variables=None):
        """Get daily historical data in JSON format"""
        if variables is None:
            variables = [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max"
            ]
        
        endpoint = self.archive_url
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(variables),
            "timezone": "auto"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    # ===== MULTIPLE CITIES METHODS =====
    
    def get_multiple_cities_hourly(self, cities, start_date, end_date, variables=None):
        """
        Get hourly historical data for multiple cities
        
        Args:
            cities: List of dicts with 'name', 'latitude', 'longitude'
                   Example: [{'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522}]
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            variables: List of variables to fetch
        
        Returns:
            Dictionary with city names as keys and their data as values
        """
        results = {}
        
        for city in cities:
            print(f"Fetching hourly data for {city['name']}...")
            data = self.get_historical_hourly(
                city['latitude'],
                city['longitude'],
                start_date,
                end_date,
                variables
            )
            results[city['name']] = data
        
        return results
    
    def get_multiple_cities_daily(self, cities, start_date, end_date, variables=None):
        """
        Get daily historical data for multiple cities
        
        Args:
            cities: List of dicts with 'name', 'latitude', 'longitude'
                   Example: [{'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522}]
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            variables: List of variables to fetch
        
        Returns:
            Dictionary with city names as keys and their data as values
        """
        results = {}
        
        for city in cities:
            print(f"Fetching daily data for {city['name']}...")
            data = self.get_historical_daily(
                city['latitude'],
                city['longitude'],
                start_date,
                end_date,
                variables
            )
            results[city['name']] = data
        
        return results
    
    # ===== UTILITY METHODS =====
    
    # Keep old method names for backward compatibility
    def get_forecast_json(self, latitude, longitude, variables=None, days=7):
        """Deprecated: Use get_forecast_hourly instead"""
        return self.get_forecast_hourly(latitude, longitude, variables, days)
    
    def get_historical_json(self, latitude, longitude, start_date, end_date, variables=None):
        """Deprecated: Use get_historical_hourly instead"""
        return self.get_historical_hourly(latitude, longitude, start_date, end_date, variables)
    
    def save_json(self, data, filename):
        """Save JSON data to file"""
        import os
        # Create directory if it doesn't exist
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✓ Data saved to {filename}")
    
    def load_json(self, filename):
        """Load JSON data from file"""
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # ===== DATA PROCESSING WITH JMESPATH =====
    
    def extract_metadata(self, json_data):
        """Extract location metadata using JMESPath"""
        query = """{
            latitude: latitude,
            longitude: longitude,
            timezone: timezone,
            elevation: elevation
        }"""
        return jmespath.search(query, json_data)
    
    def extract_daily_records(self, json_data, city_name=None):
        """
        Extract daily records as list of dictionaries using JMESPath
        
        Returns list of records ready for SQL insertion
        """
        # Check for errors
        if json_data.get('error'):
            print(f"Error: {json_data.get('reason')}")
            return []
        
        # Get metadata
        metadata = self.extract_metadata(json_data)
        
        # Extract daily data
        daily_data = json_data.get('daily', {})
        if not daily_data:
            print("No daily data found")
            return []
        
        # Get time array
        times = daily_data.get('time', [])
        
        # Build records
        records = []
        for i, date in enumerate(times):
            record = {
                'date': date,
                'city': city_name,
                'latitude': metadata.get('latitude'),
                'longitude': metadata.get('longitude'),
                'timezone': metadata.get('timezone'),
                'elevation': metadata.get('elevation')
            }
            
            # Add all other variables
            for key, values in daily_data.items():
                if key != 'time' and i < len(values):
                    record[key] = values[i]
            
            records.append(record)
        
        return records
    
    def extract_hourly_records(self, json_data, city_name=None):
        """
        Extract hourly records as list of dictionaries using JMESPath
        
        Returns list of records ready for SQL insertion
        """
        # Check for errors
        if json_data.get('error'):
            print(f"Error: {json_data.get('reason')}")
            return []
        
        # Get metadata
        metadata = self.extract_metadata(json_data)
        
        # Extract hourly data
        hourly_data = json_data.get('hourly', {})
        if not hourly_data:
            print("No hourly data found")
            return []
        
        # Get time array
        times = hourly_data.get('time', [])
        
        # Build records
        records = []
        for i, timestamp in enumerate(times):
            record = {
                'datetime': timestamp,
                'city': city_name,
                'latitude': metadata.get('latitude'),
                'longitude': metadata.get('longitude'),
                'timezone': metadata.get('timezone'),
                'elevation': metadata.get('elevation')
            }
            
            # Add all other variables
            for key, values in hourly_data.items():
                if key != 'time' and i < len(values):
                    record[key] = values[i]
            
            records.append(record)
        
        return records
    
    def extract_specific_variables(self, json_data, variables, data_type='daily'):
        """
        Extract specific variables using JMESPath
        
        Args:
            json_data: API response JSON
            variables: List of variable names to extract
            data_type: 'daily' or 'hourly'
        
        Returns:
            Dictionary with requested variables
        """
        # Build JMESPath query dynamically
        var_queries = ", ".join([f"{var}: {data_type}.{var}" for var in variables])
        query = f"{{{var_queries}, time: {data_type}.time}}"
        
        return jmespath.search(query, json_data)
    
    def process_multiple_cities_to_records(self, multi_city_data, data_type='daily'):
        """
        Process multiple cities data into flat records list
        
        Args:
            multi_city_data: Dictionary with city names as keys
            data_type: 'daily' or 'hourly'
        
        Returns:
            List of all records from all cities
        """
        all_records = []
        
        for city_name, json_data in multi_city_data.items():
            if data_type == 'daily':
                records = self.extract_daily_records(json_data, city_name)
            else:
                records = self.extract_hourly_records(json_data, city_name)
            
            all_records.extend(records)
            print(f"✓ Processed {len(records)} records for {city_name}")
        
        return all_records
    
    def records_to_sql(self, records, table_name, db_path):
        """
        Save records to SQLite database
        
        Args:
            records: List of dictionaries
            table_name: SQL table name
            db_path: Path to SQLite database file
        """
        import sqlite3
        
        if not records:
            print("No records to save")
            return
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table dynamically based on first record
        columns = records[0].keys()
        
        # Create table if not exists
        col_defs = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})")
        
        # Insert records
        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        for record in records:
            cursor.execute(insert_query, [record[col] for col in columns])
        
        conn.commit()
        conn.close()
        
        print(f"✓ {len(records)} records saved to {table_name} in {db_path}")
    
    def save_records_to_json(self, records, filename):
        """Save processed records to JSON file"""
        import os
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print(f"✓ {len(records)} records saved to {filename}")


# Usage examples
if __name__ == "__main__":
    api = OpenMeteoAPI()
    
    # Output folder
    output_folder = "temperature_json"
    
    # Example 1: Single city - hourly data
    print("=== Getting hourly forecast data ===")
    forecast_hourly = api.get_forecast_hourly(48.8566, 2.3522, days=7)
    api.save_json(forecast_hourly, f'{output_folder}/paris_forecast_hourly.json')
    
    # Example 2: Single city - daily data
    print("\n=== Getting daily forecast data ===")
    forecast_daily = api.get_forecast_daily(48.8566, 2.3522, days=7)
    api.save_json(forecast_daily, f'{output_folder}/paris_forecast_daily.json')
    
    # Example 3: Single city - historical daily data (10 years)
    print("\n=== Getting 10 years of daily historical data ===")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
    
    historical_daily = api.get_historical_daily(
        48.8566, 2.3522,
        start_date, end_date
    )
    api.save_json(historical_daily, f'{output_folder}/paris_historical_daily_10y.json')
    
    # Example 4: Multiple cities - daily data (10 years)
    print("\n=== Getting data for multiple cities ===")
    cities = [
        {'name': 'Paris', 'latitude': 48.8566, 'longitude': 2.3522},
        {'name': 'London', 'latitude': 51.5074, 'longitude': -0.1278},
        {'name': 'Berlin', 'latitude': 52.5200, 'longitude': 13.4050},
        {'name': 'Madrid', 'latitude': 40.4168, 'longitude': -3.7038},
        {'name': 'Rome', 'latitude': 41.9028, 'longitude': 12.4964}
    ]
    
    multi_city_data = api.get_multiple_cities_daily(
        cities, 
        start_date, 
        end_date
    )
    
    # Save each city's data to a separate file
    for city_name, data in multi_city_data.items():
        api.save_json(data, f'{output_folder}/{city_name.lower()}_daily_10y.json')
    
    # Or save all together
    api.save_json(multi_city_data, f'{output_folder}/all_cities_daily_10y.json')
    
    # Example 5: Process data using JMESPath
    print("\n=== Processing data with JMESPath ===")
    
    # Extract records from all cities
    all_records = api.process_multiple_cities_to_records(multi_city_data, data_type='daily')
    print(f"\nTotal records: {len(all_records)}")
    print(f"First record: {all_records[0]}")
    
    # Save processed records as JSON
    api.save_records_to_json(all_records, f'{output_folder}/processed_records.json')
    
    # Example 6: Save to SQLite database
    print("\n=== Saving to SQLite database ===")
    db_path = f'{output_folder}/weather_data.db'
    api.records_to_sql(all_records, 'weather_daily', db_path)
    
    # Example 7: Extract specific variables using JMESPath
    print("\n=== Extract specific variables ===")
    paris_data = multi_city_data['Paris']
    temp_precip = api.extract_specific_variables(
        paris_data, 
        ['temperature_2m_max', 'temperature_2m_min', 'precipitation_sum'],
        data_type='daily'
    )
    print(f"Extracted variables: {list(temp_precip.keys())}")
    print(f"First 3 dates: {temp_precip['time'][:3]}")
    
    print("\n✓ All processing complete!")