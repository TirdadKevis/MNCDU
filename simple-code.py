import requests
import csv
from datetime import datetime, timedelta
import time

def get_newyork_data():
    # Known URL for the Citi Bike network in New York
    citi_bike_nyc_url = "http://api.citybik.es/v2/networks/citi-bike-nyc"
    
    try:
        # Make an HTTP GET request to the specific URL to get detailed information
        response = requests.get(citi_bike_nyc_url)
        citi_bike_nyc_data = response.json()

        # Extract network information
        network_info = citi_bike_nyc_data.get('network', {})
        stations_info = network_info.get('stations', [])

        selected_fields = []

        # Extract only the desired fields from each station
        for station_info in stations_info:
            station_fields = {
                'empty_slots': station_info.get('empty_slots', ''),
                'ebikes': station_info.get('extra', {}).get('ebikes', ''),
                'has_ebikes': station_info.get('extra', {}).get('has_ebikes', ''),
                'last_updated': station_info.get('extra', {}).get('last_updated', ''),
                'payment-terminal': station_info.get('extra', {}).get('payment-terminal', ''),
                'renting': station_info.get('extra', {}).get('renting', ''),
                'returning': station_info.get('extra', {}).get('returning', ''),
                'slots': station_info.get('extra', {}).get('slots', ''),
                'uid': station_info.get('extra', {}).get('uid', ''),
                'free_bikes': station_info.get('free_bikes', ''),
                'id': station_info.get('id', ''),
                'latitude': station_info.get('latitude', ''),
                'longitude': station_info.get('longitude', ''),
                'name': station_info.get('name', ''),
                'timestamp': station_info.get('timestamp', ''),
                'request_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            selected_fields.append(station_fields)

        return selected_fields
    
    except requests.exceptions.RequestException as e:
        print(f"Error during data retrieval: {e}")
        return None

def save_to_csv(data, filename):
    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            # Write header only if the file is empty
            writer.writerow(data[0].keys())
        for station_fields in data:
            writer.writerow(station_fields.values())

current_time = datetime.now()
filename = f"{current_time.strftime('%d_%m_%y_%H_%M')}.csv"
citi_bike_nyc_data = get_newyork_data()
save_to_csv(citi_bike_nyc_data, filename)
