import pandas as pd
import json
import boto3
import urllib.request
import datetime as dt
import os
import logging

from helper_functions import timestamp_offset

s3_client = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
def lambda_handler(event, context):
    try:
        #URL for API call
        lat,lon = 50.088,14.4208
        api_url = f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,precipitation,surface_pressure,wind_speed_10m&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,surface_pressure&timezone=auto'

        #Request data from api
        logger.info(f"Fetching data from API: {api_url}")
        req = urllib.request.Request(api_url)
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                raise Exception(f"API returned status code {response.status}")
            data = json.loads(response.read().decode())

        # Validate response
        if 'current' not in data or 'hourly' not in data:
            raise ValueError("Invalid API response structure")
        
        #Round down time for the current data to previous hour. If the api is called at the correct time, this won't matter, but in case it calls late, need to round off the minutes
        current_time = data['current']['time']
        current_time = current_time[:-2] + '00'

        #Get the timestamps for the 4 predicted times
        offset_hours = [6, 24, 72, 144]
        offset_timestamps = [timestamp_offset(current_time, hours) for hours in offset_hours]

        #Create df with current weather data
        current_data = pd.DataFrame.from_dict(data['current'], orient='index').transpose()
        current_data.drop('interval', axis=1, inplace=True)
        current_data['time'] = current_time
        current_data['index'] = 'current'
        current_data.set_index('index', inplace=True)

        #create df with predicted weather data at 4 times
        prediction_data = pd.DataFrame({'time':offset_timestamps, 
                                        'index':['6_hour', '1_day', '3_day', '6_day']})
        prediction_data.set_index('index', inplace=True)
        prediction_data = prediction_data.join(pd.DataFrame(data['hourly']).set_index('time'), on='time')

        #Concat current and predcited weather dfs together
        df_data = pd.concat([current_data, prediction_data])
         
        #Convert df to json format
        json_data = df_data.to_json(orient='index', indent=2)

        # Create filename with timestamp
        timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'api_data_{timestamp}.json'

        # Save to S3
        BUCKET_NAME = os.environ.get('S3_BUCKET', 'weather-pipeline-json-storage')
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json_data,
            ContentType='application/json'
        )
        
        logger.info(f"Successfully saved {filename} to S3")
        return {
        'statusCode': 200,
        'body': json.dumps({'message': f'Successfully saved {filename} to S3'})
    }

    except urllib.error.URLError as e:
        logger.error(f"API request failed: {str(e)}")
        return {
            'statusCode': 502,
            'body': json.dumps({'error': f'API request failed: {str(e)}'})
        }
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }