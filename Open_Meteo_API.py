import requests
import json
from datetime import datetime, timedelta

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