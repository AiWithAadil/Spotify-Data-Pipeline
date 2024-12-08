import json
import boto3
import gzip
import base64
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
logs_client = boto3.client('logs')

# Specify the S3 bucket and folder for storing log data
S3_BUCKET_NAME = 'spotify-ad'  # Replace with your actual S3 bucket name
S3_LOG_PREFIX = 'raw_data/' 

def lambda_handler(event, context):
    # Extract log data from CloudWatch Logs event
    log_data = event['awslogs']['data']
    
    # Decode the base64-encoded log data
    decoded_data = base64.b64decode(log_data)
    
    # Gzip decompress the data (CloudWatch Logs are gzipped)
    decompressed_data = gzip.decompress(decoded_data)
    
    # Parse the JSON data from CloudWatch Logs
    log_json = json.loads(decompressed_data)
    
    # Extract relevant information from the log event
    log_events = log_json['logEvents']
    
    # Prepare the data to store in S3
    timestamp = int(datetime.utcnow().timestamp())
    log_data_json = {
        'timestamp': timestamp,
        'logGroupName': log_json['logGroup'],
        'logStreamName': log_json['logStream'],
        'logEvents': log_events
    }
    
    # Convert the log data to JSON string format
    log_data_str = json.dumps(log_data_json, indent=4)
    
    # Generate a unique file name using the timestamp and log stream name
    s3_key = f"{S3_LOG_PREFIX}{log_json['logStream']}-{timestamp}.json"
    
    # Upload the data to S3
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=log_data_str,
            ContentType='application/json'
        )
        print(f"Successfully uploaded logs to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading logs to S3: {str(e)}")
        raise e
