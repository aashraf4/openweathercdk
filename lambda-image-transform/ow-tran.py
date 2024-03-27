import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import tempfile

load_dotenv()

aws_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_ACCESS_SECRET")
bucket_name = os.getenv("s3_upload_bucket")

def parse_weather_data(current_date):
    try:
        # Connect to S3
        s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name='eu-central-1')

        # Define the file prefix based on the current date
        s3_key = f"raw/openweather-{current_date}.csv"

        # Download the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(response['Body'])

        return df

    except Exception as e:
        print(f"Error downloading or parsing file: {e}")
        return None

def transform_weather_data(df):
    try:
        # Perform transformations on the DataFrame
        # Temperature Conversion (Celsius to Fahrenheit)
        df['Temperature_F'] = (df['Temperature'] * 9/5) + 32
        
        # Description Simplification
        df['Description_Simplified'] = df['Description'].str.split().str.get(0)
        
        # Humidity Normalization
        df['Humidity_Normalized'] = df['Humidity'] / 100
        
        # Wind Speed Units Conversion (m/s to km/h)
        df['Wind Speed_km/h'] = df['Wind Speed'] * 3.6
        
        # Visibility Units Conversion (m to km)
        df['Visibility_km'] = df['Visibility'] / 1000
        # Drop the old columns
        df = df.drop(['Temperature', 'Description', 'Humidity', 'Wind Speed', 'Visibility'], axis=1)
        return df

    except Exception as e:
        print(f"Error transforming data: {e}")
        return None

def upload_to_s3(file_path, object_name):
    # Upload the file to S3
    try:
        s3_client = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name='eu-central-1')
        s3_client.upload_file(file_path, bucket_name, f"transformed/{object_name}")
        print(f"Upload to S3 successful: {object_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def handler(event, context):
    # Define the current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Parse weather data from the specified S3 bucket
    df = parse_weather_data(current_date)

    if df is not None:
        # Transform weather data
        transformed_df = transform_weather_data(df)

        if transformed_df is not None:
            try:
                # Create a temporary directory
                with tempfile.TemporaryDirectory() as tmpdir:
                    # Define the temporary file path
                    temp_file_path = os.path.join(tmpdir, f"openweather-{current_date}-transformed.csv")

                    # Output the transformed data to the temporary file
                    transformed_df.to_csv(temp_file_path, index=False)

                    # Upload the temporary file to S3
                    upload_to_s3(temp_file_path, f"openweather-{current_date}-transformed.csv")

                return {
                    'statusCode': 200,
                    'body': 'Weather data processed and uploaded successfully'
                }
            except Exception as e:
                print(f"Error processing or uploading data: {e}")
                return {
                    'statusCode': 500,
                    'body': 'Error processing weather data'
                }
        else:
            return {
                'statusCode': 500,
                'body': 'Error transforming weather data'
            }
    else:
        return {
            'statusCode': 500,
            'body': 'Error parsing weather data'
        }
