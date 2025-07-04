import csv
import json
import requests
import arrow
import config
import influxdb_client

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision


# Initialize InfluxDB client
client = influxdb_client.InfluxDBClient(
    url=config.influx_url,
    token=config.influx_token,
    org=config.influx_org
)
write_api = client.write_api(write_options=SYNCHRONOUS)


# Supporting Functions

def load_beach_metadata(path):
    with open(path, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def fetch_weather_data(lat, lon, start, end):
    url = 'https://api.stormglass.io/v2/weather/point'
    params = {
        'lat': float(lat),
        'lng': float(lon),
        'params': ','.join([
            'airTemperature', 'cloudCover', 'humidity', 'swellHeight',
            'pressure', 'windDirection', 'windSpeed', 'waterTemperature',
            'waveHeight'
        ]),
        'source': 'noaa',
        'start': start,
        'end': end,
    }
    headers = {
        'Authorization': config.stormglass_auth
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json().get('hours', [])


def create_weather_point(metadata, metrics):
    time = arrow.get(metrics['time']).timestamp()

    return (
        influxdb_client.Point('weather')
        .tag('source', 'noaa')
        .tag('name', metadata['name'])
        .tag('state', metadata['state'])
        .tag('county', metadata['county'])
        .tag('prediction', 'hindcast')
        .field('air_temp', float(metrics['airTemperature']['noaa']))
        .field('cloud_cover', float(metrics['cloudCover']['noaa']))
        .field('humidity', float(metrics['humidity']['noaa']))
        .field('swell_height', float(metrics['swellHeight']['noaa']))
        .field('pressure', float(metrics['pressure']['noaa']))
        .field('wind_direction', float(metrics['windDirection']['noaa']))
        .field('wind_speed', float(metrics['windSpeed']['noaa']))
        .field('water_temp', float(metrics['waterTemperature']['noaa']))
        .field('wave_height', float(metrics['waveHeight']['noaa']))
        .field('lat', float(metadata['lat']))
        .field('lon', float(metadata['lon']))
        .time(int(time), write_precision=WritePrecision.S)
    )


# Main

def main():
    beach_data_path = '/Users/briankeene/Desktop/beach_meta_1.csv'
    beaches = load_beach_metadata(beach_data_path)

    now = arrow.utcnow()
    start_time = now.shift(days=-5).timestamp()
    end_time = now.timestamp()

    for beach in beaches:
        print(f"Pulling data for - {beach['name']}")

        try:
            hourly_data = fetch_weather_data(beach['lat'], beach['lon'], start_time, end_time)
        except requests.RequestException as e:
            print(f"Failed to fetch data for {beach['name']}: {e}")
            continue

        for hourly_metrics in hourly_data:
            if 'airTemperature' in hourly_metrics:
                point = create_weather_point(beach, hourly_metrics)
                print(point.to_line_protocol())
                write_api.write(bucket=config.influx_bucket, org=config.influx_org, record=point)


if __name__ == '__main__':
    main()
