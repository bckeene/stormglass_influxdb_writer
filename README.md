# stormglass_influxdb_writer

This script collects historical weather metrics from https://stormglass.io/, converts them to line protocol, and then writes each point into a user-defined bucket in InfluxDB Cloud.

config.py should be updated with user's personal access credentials as follows:

InfluxDB Cloud - free tier account use (https://cloud2.influxdata.com/signup)

Stormglass - base account permits 50 API requests per day (https://dashboard.stormglass.io/register)
