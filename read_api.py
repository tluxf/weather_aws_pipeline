import pandas as pd
import json
import boto3
import urllib.request
import datetime as dt

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        #URL for API call
        lat,lon = 50.088,14.4208
        api_url = f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,precipitation,surface_pressure,wind_speed_10m&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,surface_pressure&timezone=auto'

        #Request data from api
        req = urllib.request.Request(api_url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
        
        #Round down time for the current data to previous hour. If the api is called at the coorect time, this won't matter, but in case it calls late, need to round off the minutes
        curent_time = data['current']['time']
        curent_time = curent_time[:-2]+'00'

        #Get the timestamps for the 4 predicted times
        offset_hours = [6, 24, 72, 144]
        offset_timestamps = []
        for i in offset_hours:
            offset_timestamps.append(timestamp_offset(curent_time, i))

        #Create df with current weather data
        current_data = pd.DataFrame.from_dict(data['current'], orient='index').transpose()
        current_data.drop('interval', axis=1, inplace=True)
        current_data['time'] = curent_time
        current_data['index'] = 'current'
        current_data.set_index('index', inplace=True)

        #create df with predicted weather data at 4 times
        prediction_data = pd.DataFrame({'time':offset_timestamps, 'index':['6_hour', '1_day', '3_day', '6_day']})
        prediction_data.set_index('index', inplace=True)
        prediction_data = prediction_data.join(pd.DataFrame(data['hourly']).set_index('time'), on='time')

        #Concat current and predcited weather dfs together
        df_data = pd.concat([current_data, prediction_data])
        
        #Convert df to json format
        json_data = df_data.to_json(orient='index', indent=2)

        # Create filename with timestamp
        timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
        filename = f'api_data_{timestamp}.json'

        # Save to S3
        s3_client.put_object(
            Bucket='test-bucket-api-proj-0001',
            Key=filename,
            Body=json.dumps(json_data, indent=2),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully saved {filename} to S3')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }