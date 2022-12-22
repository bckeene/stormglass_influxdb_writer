import config
import csv
import requests
import arrow
import json
import influxdb_client

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

# Intitialize influxdb_client
client = influxdb_client.InfluxDBClient(
    url=config.influx_url,   
    token=config.influx_token,
    org=config.influx_org
)

write_api = client.write_api(write_options=SYNCHRONOUS)

# Read in CSV file with beach meta data (name, county, state, and lat/lon)
with open('/Users/briankeene/Desktop/beach_meta_1.csv', encoding='utf-8') as csvfile:
    beach_meta_data = list(csv.DictReader(csvfile))

# Create list for populating stormglass api response data
stormglass_data = []

# Create variable to track UTC a time of script 
current_UTC = arrow.utcnow()

# Call Stormglass API for each item in coordinates_list
for beach in range(len(beach_meta_data)):
    print('Pulling data for - ' + str(beach_meta_data[beach]['name']))

    response = requests.get(
        'https://api.stormglass.io/v2/weather/point',
            params={
                'lat': float(beach_meta_data[beach]['lat']),
                'lng': float(beach_meta_data[beach]['lon']),
                'params': ','.join(['airTemperature','cloudCover','humidity','swellHeight','pressure','windDirection','windSpeed','waterTemperature','waveHeight']),
                'source': 'noaa',
                'start': (current_UTC.shift(days=-5).timestamp()),
                'end': (current_UTC.timestamp()),
                },
            headers={
        'Authorization': config.stormglass_auth
        },
    )

    # Append each JSON response representing weather metrics for lat/long used to make Stormglass API call
    stormglass_data.append(response.json()['hours'])

for current_row, location in zip(stormglass_data, beach_meta_data):
    current_row.append(location)

for stormglass_metrics in stormglass_data:
    meta_data = next((d for d in stormglass_metrics if 'lat' in d), None)

    print('Chunk of metrics for - ' + str(meta_data['name']))
    for hourly_weather_metrics in stormglass_metrics:
        if 'airTemperature' in hourly_weather_metrics:

            # Tagset variables
            name = meta_data['name']
            state = meta_data['state']
            county = meta_data['county']

            # Fieldset variables
            air_temp = hourly_weather_metrics['airTemperature']['noaa']
            cloud_cover = hourly_weather_metrics['cloudCover']['noaa']
            humidity = hourly_weather_metrics['humidity']['noaa']
            swell_height = hourly_weather_metrics['swellHeight']['noaa']
            pressure = hourly_weather_metrics['pressure']['noaa']
            wind_direction = hourly_weather_metrics['windDirection']['noaa']
            wind_speed = hourly_weather_metrics['windSpeed']['noaa']
            water_temp = hourly_weather_metrics['waterTemperature']['noaa']
            wave_height = hourly_weather_metrics['waveHeight']['noaa']
            latitude = meta_data['lat']
            longitude = meta_data['lon']

            # Timestamp variable
            time = arrow.get(hourly_weather_metrics['time']).timestamp()

            # Create point of data in line protocol format
            weather_point = influxdb_client.Point('weather').tag('source','noaa').tag('name',name).tag('state',state).tag('county',county).tag("prediction", "hindcast").field("air_temp",float(air_temp)).field("cloud_cover",float(cloud_cover)).field("humidity",float(humidity)).field("swell_height",float(swell_height)).field("pressure",float(pressure)).field("wind_direction",float(wind_direction)).field("wind_speed",float(wind_speed)).field("water_temp",float(water_temp)).field("wave_height",float(wave_height)).field("lat",float(latitude)).field("lon",float(longitude)).time((int(time)), write_precision=WritePrecision.S)
            
            # Print data point and write to InfluxDB
            print(weather_point.to_line_protocol())
            write_api.write(bucket=config.influx_bucket, org=config.influx_org, record=weather_point)
