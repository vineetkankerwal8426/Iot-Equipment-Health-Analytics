import json
import random
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

DEVICE_IDS = [f"M{str(i).zfill(3)}" for i in range(1, 11)]
STATUS_WEIGHTS = {"OK": 0.7, "WARN": 0.2, "FAIL": 0.1}

kinesis_client = boto3.client("kinesis")
secrets_client = boto3.client("secretsmanager")

def get_kinesis_stream_name():
    secret_name = "iot-secrets"
    
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The secret {secret_name} was not found")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print(f"The request was invalid due to:", e)
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print(f"The request had invalid params:", e)
            raise e
        else:
            raise e
    
    secret = get_secret_value_response['SecretString']
    secret_data = json.loads(secret)
    
    stream_name = secret_data.get('KinesisStreamName')
    
    if not stream_name:
        raise ValueError("Stream name not found in secret")
    
    return stream_name

def generate_sensor_data(device_id):
    status = random.choices(
        population=list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values()),
        k=1
    )[0]
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensor_temp": round(random.uniform(60.0, 90.0), 2),
        "sensor_vibration": round(random.uniform(0.3, 1.5), 2),
        "sensor_pressure": round(random.uniform(1.0, 1.4), 2),
        "status_code": status
    }

def lambda_handler(event, context):
    try:
        stream_name = get_kinesis_stream_name()
        
        for device_id in DEVICE_IDS:
            payload = generate_sensor_data(device_id)
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(payload),
                PartitionKey=device_id
            )
        
        return {
            "statusCode": 200,
            "body": f"Sent data for {len(DEVICE_IDS)} devices to {stream_name}"
        }
        
    except ClientError as e:
        print(f"Secrets Manager error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error retrieving secret: {str(e)}"
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Unexpected error: {str(e)}"
        }