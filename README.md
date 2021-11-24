# stormglass_hindcast

This script collects historical weather metrics from https://stormglass.io/, converts them to line protocol, and then writes each point into a user-defined bucket in InfluxDB Cloud.

```beache_meta.csv``` should be updated with user's prefered locations.

```config.py``` should be updated with user's personal access credentials as follows:

**InfluxDB Cloud** - free tier account use  from https://cloud2.influxdata.com/signup

**Stormglass.io** - base account permits 50 API requests per day from https://dashboard.stormglass.io/register
