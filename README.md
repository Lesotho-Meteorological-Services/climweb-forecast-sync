# ClimWeb Forecast Sync

Utility to sync cleaned LMS forecast files into ClimWeb Forecast Manager.

## What it supports

- Hourly forecasts only
- Daily forecasts only
- Both hourly and daily forecasts
- Optional city-name mapping before posting to ClimWeb
- Optional city filtering so only selected locations are published
- Safe handling of missing numeric placeholders such as `**`
- Skip-daily-overlap mode to avoid daily/hourly collisions in Forecast Manager

## LMS source files

By default the utility pulls these cleaned files from the LMS server:

- `/home/lmsnwp/DA/Met_App/Output/HrlyFcWx.csv`
- `/home/lmsnwp/DA/Met_App/Output/DailyFcWx.csv`

## Included configuration

- `config/city_mapping_districts_plus_special.csv`
- `config/allowed_cities_districts_plus_special.txt`
- `deploy/climweb_forecast_sync.env.example`
- `deploy/systemd/climweb-forecast-sync.service`
- `deploy/systemd/climweb-forecast-sync.timer`

## Included city mapping

The packaged mapping converts LMS input names to ClimWeb names like this:

- `Maseru (Maseru District)` -> `Maseru`
- `Maseru (Berea Distict)` -> `Berea`
- `Mafeteng` -> `Mafeteng`
- `Mohale's Hoek` -> `Mohale's Hoek`
- `Quthing` -> `Quthing`
- `Qacha's Nek` -> `Qacha's Nek`
- `Mokhotlong` -> `Mokhotlong`
- `Thaba-Tseka` -> `Thaba-Tseka`
- `Leribe` -> `Leribe`
- `Butha-Buthe (Butha-Buthe District)` -> `Butha-Buthe`
- `Moshoeshoe` -> `Moshoeshoe I`
- `oxbowd` -> `Oxbow`
- `Semonkong` -> `Semonkong`

## Requirements

- Python 3.9+
- `requests`
- SSH key-based access from the sync host to the LMS server

Install dependencies:

```bash
python3 -m pip install -r requirements.txt