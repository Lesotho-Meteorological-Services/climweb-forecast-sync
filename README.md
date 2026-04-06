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
```

## Run examples

### Daily only

```bash
python3 scripts/climweb_forecast_sync.py   --mode daily   --city-mapping-csv config/city_mapping_districts_plus_special.csv   --allowed-cities-file config/allowed_cities_districts_plus_special.txt   --base-url https://share.csis.gov.ls   --username YOUR_USER   --password 'YOUR_PASSWORD'   --remote-host 41.203.191.69   --remote-user lmsnwp   --ssh-identity ~/.ssh/lms_hourly_sync_key   --workdir ./work   --state-file ./work/state_daily.json
```

### Hourly only

```bash
python3 scripts/climweb_forecast_sync.py   --mode hourly   --city-mapping-csv config/city_mapping_districts_plus_special.csv   --allowed-cities-file config/allowed_cities_districts_plus_special.txt   --base-url https://share.csis.gov.ls   --username YOUR_USER   --password 'YOUR_PASSWORD'   --remote-host 41.203.191.69   --remote-user lmsnwp   --ssh-identity ~/.ssh/lms_hourly_sync_key   --workdir ./work   --state-file ./work/state_hourly.json
```

### Both hourly and daily

```bash
python3 scripts/climweb_forecast_sync.py   --mode both   --city-mapping-csv config/city_mapping_districts_plus_special.csv   --allowed-cities-file config/allowed_cities_districts_plus_special.txt   --base-url https://share.csis.gov.ls   --username YOUR_USER   --password 'YOUR_PASSWORD'   --remote-host 41.203.191.69   --remote-user lmsnwp   --ssh-identity ~/.ssh/lms_hourly_sync_key   --workdir ./work   --state-file ./work/state_both.json
```

## Important behavior in both mode

When `--mode both` is used, the script skips daily dates already covered by the hourly file by default. This avoids Forecast Manager collisions where daily and hourly forecasts would otherwise try to use the same date and effective period.

## Notes

- The script normalizes `T/SHOWER` to `T/SHOWERS`.
- The script ignores numeric placeholders such as `**` instead of crashing.
- The sync is hash-based, so unchanged source files will not be reposted unless the state file changes or is removed.
