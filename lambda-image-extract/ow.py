import requests
import csv
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
import tempfile

load_dotenv()

aws_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_ACCESS_SECRET")
bucket_name = os.getenv("s3_upload_bucket")

def upload_to_s3(file_path, object_name):
    # Upload the file to S3
    try:
        s3_client = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name='eu-central-1')
        s3_client.upload_file(file_path, bucket_name, f"raw/{object_name}")
        print(f"Upload to S3 successful: {object_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def get_weather(city_name, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data for {city_name}. Status Code: {response.status_code}")
        return None

def handler(event, context):
    cities = ["Tokyo", "London", "Stockholm", "Sao Paulo", "Mexico City"]
    api_key = os.environ.get("API_KEY")

    weather_data_list = []
    for city in cities:
        weather_data = get_weather(city, api_key)
        if weather_data:
            weather_data_list.append({
                "City": city,
                "Temperature (°C)": weather_data["main"]["temp"],
                "Description": weather_data["weather"][0]["description"],
                "Humidity (%)": weather_data["main"]["humidity"],
                "Wind Speed (m/s)": weather_data["wind"]["speed"],
                "Visibility (m)": weather_data["visibility"] if "visibility" in weather_data else None
            })
    # Generate CSV data
    csv_data = []
    for data in weather_data_list:
        csv_data.append([data["City"], data["Temperature (°C)"], data["Description"], data["Humidity (%)"], data["Wind Speed (m/s)"], data["Visibility (m)"]])
    # Save data to CSV
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Generate CSV file
        csv_filename = f"{tmpdir}/openweather.csv"
        with open(csv_filename, mode='w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["City", "Temperature", "Description", "Humidity", "Wind Speed", "Visibility"])
            csv_writer.writerows(csv_data)
            
        # Upload CSV file to S3
        current_date = datetime.now().strftime("%Y-%m-%d")
        s3_key = f"openweather-{current_date}.csv"
        upload_to_s3(csv_filename, s3_key)

    return {
        'statusCode': 200,
        'body': 'CSV file uploaded to S3'
    }
