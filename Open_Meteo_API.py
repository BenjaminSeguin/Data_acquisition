import requests
import json
from datetime import datetime, timedelta

class OpenMeteoAPI:
    def __init__(self):
        self.base_url = "https://api.open-meteo.com/v1"
    
    def get_forecast_json(self, latitude, longitude, variables=None, days=7):
        """Get forecast data in JSON format"""
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
            "timezone": "Europe/Paris"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    def get_historical_json(self, latitude, longitude, start_date, end_date, variables=None):
        """Get historical data in JSON format"""
        if variables is None:
            variables = [
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "relative_humidity_2m"
            ]
        
        endpoint = f"{self.base_url}/archive"
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(variables),
            "timezone": "Europe/Paris"
        }
        
        response = requests.get(endpoint, params=params)
        return response.json()
    
    def save_json(self, data, filename):
        """Save JSON data to file"""
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
    
    # Example 1: Get forecast as JSON
    print("=== Getting forecast data ===")
    forecast_data = api.get_forecast_json(48.8566, 2.3522, days=7)
    
    # Print the JSON structure
    print("\nJSON structure:")
    print(json.dumps(forecast_data, indent=2)[:500])  # First 500 chars
    
    # Save to file
    api.save_json(forecast_data, 'paris_forecast.json')
    
    # Example 2: Get historical data as JSON
    print("\n=== Getting historical data ===")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    historical_data = api.get_historical_json(
        48.8566, 2.3522, 
        start_date, end_date
    )
    
    api.save_json(historical_data, 'paris_historical.json')