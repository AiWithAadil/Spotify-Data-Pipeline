import boto3
import json
import csv
import io
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')

# S3 Bucket Name and Folder Prefixes
S3_BUCKET_NAME = 'spotify-ad'  # Replace with your S3 bucket name
RAW_FOLDER_PREFIX = 'raw_data/'
TRANSFORMED_FOLDER_PREFIX = 'transformed_data/'

def lambda_handler(event, context):
    # List to accumulate all transformed data
    all_transformed_data = []
    
    # Get records from the S3 event
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        
        # Check if the file is in the "raw_data" folder and is a JSON file
        if s3_key.startswith(RAW_FOLDER_PREFIX) and s3_key.endswith('.json'):
            print(f"Processing file: {s3_key}")
            raw_data = read_file_from_s3(s3_bucket, s3_key)
            
            # Transform JSON data
            transformed_data = transform_json(raw_data)
            all_transformed_data.extend(transformed_data)
    
    # Generate CSV data from all transformed JSON data
    if all_transformed_data:
        csv_data = convert_to_csv(all_transformed_data)
        
        # Generate a new S3 key for the CSV file in the transformed_data folder
        timestamp = int(datetime.utcnow().timestamp())
        transformed_s3_key = f"{TRANSFORMED_FOLDER_PREFIX}{timestamp}-transformed.csv"
        
        # Upload the CSV data to S3
        upload_transformed_data(s3_bucket, transformed_s3_key, csv_data)

def read_file_from_s3(bucket, key):
    """Read file from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        return file_content
    except Exception as e:
        print(f"Error reading file from S3: {str(e)}")
        raise e

def transform_json(raw_data):
    """Transform raw JSON data to a list of dictionaries"""
    try:
        # Parse the raw JSON string into a Python list of dictionaries
        data = json.loads(raw_data)
        
        if not isinstance(data, list):
            raise ValueError("Expected a list of dictionaries in JSON data.")
        
        transformed_data = []
        for item in data:
            if not isinstance(item, dict):
                raise ValueError(f"Expected a dictionary but got: {type(item)}")
            
            transformed_item = {
                'Artist Name': item.get('Artist Name', '').title(),
                'URI': item.get('URI', ''),
                'Followers': item.get('Followers', 0),
                'Genres': item.get('Genres', '').split(","),
                'Popularity': item.get('Popularity', 0),
                'Image URL': item.get('Image URL', '')
            }
            transformed_data.append(transformed_item)
        
        return transformed_data
    except Exception as e:
        print(f"Error transforming JSON data: {str(e)}")
        raise e

def convert_to_csv(data):
    """Convert list of dictionaries to CSV format"""
    try:
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)
        return output.getvalue()
    except Exception as e:
        print(f"Error converting data to CSV: {str(e)}")
        raise e

def upload_transformed_data(bucket, key, data):
    """Upload transformed data to S3"""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType='text/csv'  # Uploading as CSV
        )
        print(f"Successfully uploaded transformed data to: s3://{bucket}/{key}")
    except Exception as e:
        print(f"Error uploading transformed data to S3: {str(e)}")
        raise e
