import json
import boto3
import psycopg2
import os
import logging

s3_client = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info("=== Lambda execution started ===")
        
        # Check environment variables exist
        db_host = os.environ.get('DB_HOST', 'NOT_SET')
        db_name = os.environ.get('DB_NAME', 'NOT_SET')
        db_user = os.environ.get('DB_USER', 'NOT_SET')
        db_password = os.environ.get('DB_PASSWORD', 'NOT_SET')
        
        logger.info(f"DB_HOST: {db_host}")
        logger.info(f"DB_NAME: {db_name}")
        logger.info(f"DB_USER: {db_user}")
        logger.info(f"Password set: {'Yes' if db_password != 'NOT_SET' else 'No'}")
        
        if db_host == 'NOT_SET':
            return {
                'statusCode': 500,
                'body': json.dumps('Environment variables not configured!')
            }
        
        logger.info("Attempting database connection...")

        # Get DB credentials from environment variables or Secrets Manager
        conn = psycopg2.connect(
            host = db_host,
            port=5432,
            database = db_name,
            user = db_user,
            password = db_password,
            connect_timeout=10
        )

        logger.info("✅ Connection successful!")
        
        # Get S3 object info from event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file: s3://{bucket}/{key}")
        
        # Load JSON from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        cursor = conn.cursor()
        
        # Insert current weather
        current = data['current']
        cursor.execute("""
            INSERT INTO weather_current 
            (date_time, temperature, humidity, precipitation, pressure, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)""", (
                current['time'], 
                current['temperature_2m'], 
                current['relative_humidity_2m'],
                current['precipitation'], 
                current['surface_pressure'], 
                current['wind_speed_10m']
            ))
        
        # Insert pred_6h weather
        pred_6h = data['6_hour']
        cursor.execute("""
            INSERT INTO weather_predict_6h 
            (date_time, temperature, humidity, precipitation, pressure, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)""", (
                pred_6h['time'], 
                pred_6h['temperature_2m'], 
                pred_6h['relative_humidity_2m'],
                pred_6h['precipitation'], 
                pred_6h['surface_pressure'], 
                pred_6h['wind_speed_10m']
            ))
        
        # Insert pred_1d weather
        pred_1d = data['1_day']
        cursor.execute("""
            INSERT INTO weather_predict_1d 
            (date_time, temperature, humidity, precipitation, pressure, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)""", (
                pred_1d['time'], 
                pred_1d['temperature_2m'], 
                pred_1d['relative_humidity_2m'],
                pred_1d['precipitation'], 
                pred_1d['surface_pressure'], 
                pred_1d['wind_speed_10m']
            ))
        
        # Insert pred_3d weather
        pred_3d = data['3_day']
        cursor.execute("""
            INSERT INTO weather_predict_3d 
            (date_time, temperature, humidity, precipitation, pressure, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)""", (
                pred_3d['time'], 
                pred_3d['temperature_2m'], 
                pred_3d['relative_humidity_2m'],
                pred_3d['precipitation'], 
                pred_3d['surface_pressure'], 
                pred_3d['wind_speed_10m']
            ))
        
        # Insert pred_6d weather
        pred_6d = data['6_day']
        cursor.execute("""
            INSERT INTO weather_predict_6d 
            (date_time, temperature, humidity, precipitation, pressure, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)""", (
                pred_6d['time'], 
                pred_6d['temperature_2m'], 
                pred_6d['relative_humidity_2m'],
                pred_6d['precipitation'], 
                pred_6d['surface_pressure'], 
                pred_6d['wind_speed_10m']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            'statusCode': 200, 
            'body': json.dumps('Database connection successful')
        }
    
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Connection error: {str(e)}')
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }