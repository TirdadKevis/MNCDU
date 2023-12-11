import os
import requests
import csv
from datetime import datetime, timedelta
import time
from azure.storage.blob import BlobServiceClient

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
directory_path = "/home/result/"
filename = os.path.join(directory_path, f"{current_time.strftime('%d_%m_%y_%H_%M')}.csv")
citi_bike_nyc_data = get_newyork_data()

# Azure Storage configuration
storage_account_connection_string = "DefaultEndpointsProtocol=https;AccountName=analysisstotage;AccountKey=2ZBsdJIGMjN0Zo9eqQ7/zZOcE1v9k5Qe/325kSK4hSyiEulRzJs/6iSdJg+u9kCicnnirRqYC7bI+ASt0oObgw==;EndpointSuffix=core.windows.net"
container_name = "analysis"
blob_name = f"citi_bike_data_{current_time.strftime('%Y%m%d%H%M%S')}.csv"

def save_to_csv_2(data, container_name, blob_name, connection_string):
    # Connect to Azure Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Convert data to CSV format
    csv_data = "\n".join([",".join(map(str, record.values())) for record in data])

    # Upload data to the blob
    blob_client.upload_blob(csv_data, overwrite=True)

# Save data to Azure Storage
save_to_csv_2(citi_bike_nyc_data, container_name, blob_name, storage_account_connection_string)
