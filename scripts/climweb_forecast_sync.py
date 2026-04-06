#!/usr/bin/env python3
"""
LMS hourly + daily forecasts -> ClimWeb Forecast Manager sync
with mode selection and city filtering.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests

MISSING_NUMERIC_MARKERS = {"", "*", "**", "***", "NA", "N/A", "na", "n/a", "null", "NULL"}

DISTRICTS_PLUS_SPECIAL = {
    "Maseru (Maseru District)",
    "Maseru (Berea Distict)",
    "Mafeteng",
    "Mohale's Hoek",
    "Quthing",
    "Qacha's Nek",
    "Mokhotlong",
    "Thaba-Tseka",
    "Leribe",
    "Butha-Buthe (Butha-Buthe District)",
    "Moshoeshoe",
    "oxbowd",
    "Semonkong",
}


def log(msg: str) -> None:
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), msg, flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def build_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return resp.text


def fetch_token(base_url: str, username: str, password: str, verify: bool) -> str:
    resp = requests.post(
        build_url(base_url, "/api/token/"),
        json={"username": username, "password": password},
        timeout=30,
        verify=verify,
    )
    if not resp.ok:
        raise RuntimeError(f"Token request failed: {resp.status_code} {safe_json(resp)}")
    data = safe_json(resp)
    if isinstance(data, dict):
        token = data.get("token") or data.get("key") or data.get("auth_token")
        if token:
            return token
    raise RuntimeError(f"Could not find token in token response: {data}")


def pull_with_scp(
    remote_user: str,
    remote_host: str,
    remote_port: int,
    remote_path: str,
    local_path: Path,
    ssh_identity: str | None = None,
) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")

    cmd = [
        "scp",
        "-P", str(remote_port),
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    if ssh_identity:
        cmd += ["-i", ssh_identity]
    cmd += [f"{remote_user}@{remote_host}:{remote_path}", str(tmp_path)]

    log(f"Pulling {remote_path} from {remote_user}@{remote_host}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"scp failed for {remote_path}: {result.stderr.strip()}")
    tmp_path.replace(local_path)


def ensure_local_copy(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def normalize_condition(value: Any) -> str:
    raw = str(value).strip().upper()
    mapping = {
        "SUNNY": "SUNNY",
        "PCLOUDY": "PCLOUDY",
        "CLOUDY": "CLOUDY",
        "RAIN": "RAIN",
        "SNOW": "SNOW",
        "T/SHOWER": "T/SHOWERS",
        "T/SHOWERS": "T/SHOWERS",
    }
    return mapping.get(raw, raw)


def hhmm_to_hhmmss(value: str) -> str:
    value = value.strip()
    return value + ":00" if len(value) == 5 else value


def parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if s in MISSING_NUMERIC_MARKERS:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_city_mapping(path: str | None) -> Dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"City mapping file not found: {p}")

    mapping: Dict[str, str] = {}
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"source_city", "target_city"}
        if not expected.issubset(set(reader.fieldnames or [])):
            raise RuntimeError("City mapping CSV must contain source_city,target_city columns")
        for row in reader:
            source_city = str(row.get("source_city", "")).strip()
            target_city = str(row.get("target_city", "")).strip()
            if source_city and target_city:
                mapping[source_city] = target_city
    return mapping


def load_allowed_cities(args: argparse.Namespace) -> set[str] | None:
    if args.city_filter_mode == "all" and not args.allowed_cities_file:
        return None

    if args.allowed_cities_file:
        p = Path(args.allowed_cities_file)
        if not p.exists():
            raise RuntimeError(f"Allowed cities file not found: {p}")
        cities = set()
        for line in p.read_text(encoding="utf-8").splitlines():
            name = line.strip()
            if name:
                cities.add(name)
        return cities

    if args.city_filter_mode == "districts_plus_special":
        return set(DISTRICTS_PLUS_SPECIAL)

    return None


def _clean_dict_row(row: dict) -> dict:
    return {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def load_hourly(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    dedup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    stats = {
        "raw_rows": 0,
        "deduplicated_keys": 0,
        "duplicate_rows_overwritten": 0,
        "rows_with_missing_numbers": 0,
        "rows_without_any_numeric_values": 0,
        "rows_skipped_missing_identity": 0,
    }

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            stats["raw_rows"] += 1
            clean = _clean_dict_row(row)

            place = str(clean.get("place", "")).strip()
            date = str(clean.get("date", "")).strip()
            time_str = str(clean.get("time", "")).strip()
            if not place or not date or not time_str:
                stats["rows_skipped_missing_identity"] += 1
                continue

            temperature = parse_optional_float(clean.get("temperature"))
            humidity = parse_optional_float(clean.get("humidity"))
            wind_speed = parse_optional_float(clean.get("wind_speed(km/h)"))

            if temperature is None or humidity is None or wind_speed is None:
                stats["rows_with_missing_numbers"] += 1
            if temperature is None and humidity is None and wind_speed is None:
                stats["rows_without_any_numeric_values"] += 1
                continue

            parsed = {
                "place": place,
                "latitude": str(clean.get("latitude", "")).strip(),
                "longitude": str(clean.get("longitude", "")).strip(),
                "date": date,
                "time": time_str,
                "temperature": temperature,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "weather": normalize_condition(clean.get("weather", "")),
            }
            key = (place, date, time_str)
            if key in dedup:
                stats["duplicate_rows_overwritten"] += 1
            dedup[key] = parsed

    stats["deduplicated_keys"] = len(dedup)
    return list(dedup.values()), stats


def load_daily(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    stats = {
        "raw_rows": 0,
        "deduplicated_keys": 0,
        "duplicate_rows_overwritten": 0,
        "rows_with_missing_numbers": 0,
        "rows_without_any_numeric_values": 0,
        "rows_skipped_missing_identity": 0,
    }

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            stats["raw_rows"] += 1
            clean = _clean_dict_row(row)

            place = str(clean.get("place", "")).strip()
            date = str(clean.get("date", "")).strip()
            if not place or not date:
                stats["rows_skipped_missing_identity"] += 1
                continue

            min_temperature = parse_optional_float(clean.get("min_temperature"))
            max_temperature = parse_optional_float(clean.get("max_temperature"))
            humidity = parse_optional_float(clean.get("humidity"))
            wind_speed = parse_optional_float(clean.get("wind_speed(km/h)"))

            if min_temperature is None or max_temperature is None or humidity is None or wind_speed is None:
                stats["rows_with_missing_numbers"] += 1
            if min_temperature is None and max_temperature is None and humidity is None and wind_speed is None:
                stats["rows_without_any_numeric_values"] += 1
                continue

            parsed = {
                "place": place,
                "latitude": str(clean.get("latitude", "")).strip(),
                "longitude": str(clean.get("longitude", "")).strip(),
                "date": date,
                "min_temperature": min_temperature,
                "max_temperature": max_temperature,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "weather": normalize_condition(clean.get("weather", "")),
            }
            key = (place, date)
            if key in dedup:
                stats["duplicate_rows_overwritten"] += 1
            dedup[key] = parsed

    stats["deduplicated_keys"] = len(dedup)
    return list(dedup.values()), stats


def filter_rows_by_allowed_cities(
    rows: List[Dict[str, Any]],
    allowed_cities: set[str] | None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if allowed_cities is None:
        return rows, {
            "input_rows": len(rows),
            "kept_rows": len(rows),
            "dropped_rows": 0,
            "unique_kept_places": len({r["place"] for r in rows}),
            "unique_dropped_places": 0,
            "dropped_place_names": [],
        }

    kept = []
    dropped_places = set()
    for row in rows:
        if row["place"] in allowed_cities:
            kept.append(row)
        else:
            dropped_places.add(row["place"])

    return kept, {
        "input_rows": len(rows),
        "kept_rows": len(kept),
        "dropped_rows": len(rows) - len(kept),
        "unique_kept_places": len({r["place"] for r in kept}),
        "unique_dropped_places": len(dropped_places),
        "dropped_place_names": sorted(dropped_places),
    }


def _apply_city_mapping(source_city: str, city_map: Dict[str, str], map_stats: Dict[str, int], unmapped_names: set[str]) -> str:
    mapped = city_map.get(source_city)
    if mapped:
        map_stats["mapped_city_names"] += 1
        return mapped
    map_stats["unmapped_city_names"] += 1
    unmapped_names.add(source_city)
    return source_city


def build_hourly_payloads(
    rows: List[Dict[str, Any]],
    source: str,
    replace_existing: bool,
    city_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    map_stats = {"mapped_city_names": 0, "unmapped_city_names": 0}
    unmapped_names: set[str] = set()

    for row in rows:
        key = (row["date"], hhmm_to_hhmmss(row["time"]))
        grouped.setdefault(key, []).append(row)

    payloads: List[Dict[str, Any]] = []
    for (forecast_date, effective_time), group in sorted(grouped.items()):
        city_forecasts = []
        seen = set()
        for row in sorted(group, key=lambda r: r["place"]):
            city = _apply_city_mapping(row["place"], city_map, map_stats, unmapped_names)
            if city in seen:
                continue
            seen.add(city)

            data_values = {}
            # if row["temperature"] is not None:
            #     data_values["air_temperature"] = row["temperature"]
            if row["temperature"] is not None:
                # Pragmatic ClimWeb workaround:
                # store hourly temperature under air_temperature_max so we do not need a separate
                # air_temperature parameter in Forecast Manager.
                data_values["air_temperature_max"] = row["temperature"]
            if row["humidity"] is not None:
                data_values["relative_humidity"] = row["humidity"]
            if row["wind_speed"] is not None:
                data_values["wind_speed"] = row["wind_speed"]
            if not data_values:
                continue

            city_forecasts.append({
                "city": city,
                "condition": row["weather"],
                "data_values": data_values,
            })

        if city_forecasts:
            payloads.append({
                "forecast_date": forecast_date,
                "effective_time": effective_time,
                "source": source,
                "replace_existing": replace_existing,
                "city_forecasts": city_forecasts,
            })

    return payloads, map_stats, sorted(unmapped_names)


def build_daily_payloads(
    rows: List[Dict[str, Any]],
    daily_effective_time: str,
    source: str,
    replace_existing: bool,
    city_map: Dict[str, str],
    skip_dates: Iterable[str] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str], List[str]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    skip_dates_set = set(skip_dates or [])
    skipped_dates_found: set[str] = set()
    map_stats = {"mapped_city_names": 0, "unmapped_city_names": 0}
    unmapped_names: set[str] = set()

    for row in rows:
        if row["date"] in skip_dates_set:
            skipped_dates_found.add(row["date"])
            continue
        grouped.setdefault(row["date"], []).append(row)

    payloads: List[Dict[str, Any]] = []
    for forecast_date, group in sorted(grouped.items()):
        city_forecasts = []
        seen = set()
        for row in sorted(group, key=lambda r: r["place"]):
            city = _apply_city_mapping(row["place"], city_map, map_stats, unmapped_names)
            if city in seen:
                continue
            seen.add(city)

            data_values = {}
            if row["min_temperature"] is not None:
                data_values["air_temperature_min"] = row["min_temperature"]
            if row["max_temperature"] is not None:
                data_values["air_temperature_max"] = row["max_temperature"]
            if row["humidity"] is not None:
                data_values["relative_humidity"] = row["humidity"]
            if row["wind_speed"] is not None:
                data_values["wind_speed"] = row["wind_speed"]
            if not data_values:
                continue

            city_forecasts.append({
                "city": city,
                "condition": row["weather"],
                "data_values": data_values,
            })

        if city_forecasts:
            payloads.append({
                "forecast_date": forecast_date,
                "effective_time": daily_effective_time,
                "source": source,
                "replace_existing": replace_existing,
                "city_forecasts": city_forecasts,
            })

    return payloads, map_stats, sorted(unmapped_names), sorted(skipped_dates_found)


def post_payload(base_url: str, token: str, payload: Dict[str, Any], verify: bool) -> None:
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
    resp = requests.post(
        build_url(base_url, "/api/forecasts/post"),
        headers=headers,
        json=payload,
        timeout=90,
        verify=verify,
    )
    if not resp.ok:
        raise RuntimeError(
            f"POST failed for {payload['forecast_date']} {payload['effective_time']}: "
            f"HTTP {resp.status_code} {safe_json(resp)}"
        )
    log(f"Posted {payload['forecast_date']} {payload['effective_time']} ({len(payload['city_forecasts'])} cities)")


def compute_daily_skip_dates(
    mode: str,
    daily_overlap_policy: str,
    hourly_rows: List[Dict[str, Any]],
) -> List[str]:
    """
    Daily-only mode:
    - never skip dates
    - current day is included

    Both mode:
    - may skip daily dates already covered by hourly rows, depending on policy
    """
    if mode != "both":
        return []
    if daily_overlap_policy != "skip_hourly_dates":
        return []
    return sorted({row["date"] for row in hourly_rows})


def sync_once(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    hourly_local = workdir / "HrlyFcWx.csv"
    daily_local = workdir / "DailyFcWx.csv"

    hourly_enabled = args.mode in {"hourly", "both"}
    daily_enabled = args.mode in {"daily", "both"}

    if args.remote_host:
        if hourly_enabled:
            pull_with_scp(args.remote_user, args.remote_host, args.remote_port, args.remote_hourly_path, hourly_local, args.ssh_identity)
        if daily_enabled:
            pull_with_scp(args.remote_user, args.remote_host, args.remote_port, args.remote_daily_path, daily_local, args.ssh_identity)
    else:
        if hourly_enabled:
            if not args.hourly_csv:
                raise RuntimeError("In local mode, --hourly-csv is required for mode hourly or both.")
            ensure_local_copy(Path(args.hourly_csv), hourly_local)
        if daily_enabled:
            if not args.daily_csv:
                raise RuntimeError("In local mode, --daily-csv is required for mode daily or both.")
            ensure_local_copy(Path(args.daily_csv), daily_local)

    fingerprint = {
        "mode": args.mode,
        "hourly_hash": sha256_file(hourly_local) if hourly_enabled else None,
        "daily_hash": sha256_file(daily_local) if daily_enabled else None,
        "daily_effective_time": args.daily_effective_time if daily_enabled else None,
        "daily_overlap_policy": args.daily_overlap_policy if args.mode == "both" else None,
        "city_mapping_csv": str(args.city_mapping_csv or ""),
        "city_filter_mode": args.city_filter_mode,
        "allowed_cities_file": str(args.allowed_cities_file or ""),
    }

    state_path = Path(args.state_file).resolve()
    state = read_state(state_path)
    if state.get("fingerprint") == fingerprint:
        log("No file changes detected. Nothing to sync.")
        return 0

    city_map = load_city_mapping(args.city_mapping_csv)
    allowed_cities = load_allowed_cities(args)

    hourly_rows: List[Dict[str, Any]] = []
    daily_rows: List[Dict[str, Any]] = []
    hourly_stats: Dict[str, int] = {}
    daily_stats: Dict[str, int] = {}
    hourly_filter_stats: Dict[str, Any] = {}
    daily_filter_stats: Dict[str, Any] = {}

    if hourly_enabled:
        hourly_rows, hourly_stats = load_hourly(hourly_local)
        hourly_rows, hourly_filter_stats = filter_rows_by_allowed_cities(hourly_rows, allowed_cities)
    if daily_enabled:
        daily_rows, daily_stats = load_daily(daily_local)
        daily_rows, daily_filter_stats = filter_rows_by_allowed_cities(daily_rows, allowed_cities)

    hourly_payloads: List[Dict[str, Any]] = []
    daily_payloads: List[Dict[str, Any]] = []
    hourly_map_stats = {"mapped_city_names": 0, "unmapped_city_names": 0}
    daily_map_stats = {"mapped_city_names": 0, "unmapped_city_names": 0}
    hourly_unmapped: List[str] = []
    daily_unmapped: List[str] = []
    skipped_daily_dates: List[str] = []

    if hourly_enabled:
        hourly_payloads, hourly_map_stats, hourly_unmapped = build_hourly_payloads(
            rows=hourly_rows,
            source=args.source,
            replace_existing=True,
            city_map=city_map,
        )

    if daily_enabled:
        daily_skip_dates = compute_daily_skip_dates(
            mode=args.mode,
            daily_overlap_policy=args.daily_overlap_policy,
            hourly_rows=hourly_rows,
        )
        daily_payloads, daily_map_stats, daily_unmapped, skipped_daily_dates = build_daily_payloads(
            rows=daily_rows,
            daily_effective_time=args.daily_effective_time,
            source=args.source,
            replace_existing=True,
            city_map=city_map,
            skip_dates=daily_skip_dates,
        )

    payload_dir = workdir / "payload_debug"
    payload_dir.mkdir(exist_ok=True)

    for payload in hourly_payloads:
        p = payload_dir / f"hourly_{payload['forecast_date']}_{payload['effective_time'].replace(':', '-')}.json"
        p.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    for payload in daily_payloads:
        p = payload_dir / f"daily_{payload['forecast_date']}_{payload['effective_time'].replace(':', '-')}.json"
        p.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    all_unmapped = sorted(set(hourly_unmapped) | set(daily_unmapped))
    if all_unmapped:
        (workdir / "unmapped_city_names.txt").write_text("\n".join(all_unmapped) + "\n", encoding="utf-8")

    if hourly_enabled:
        log(f"Prepared {len(hourly_payloads)} hourly payloads")
        log(f"Hourly CSV stats: {json.dumps(hourly_stats, sort_keys=True)}")
        log(f"Hourly filter stats: {json.dumps({k:v for k,v in hourly_filter_stats.items() if k != 'dropped_place_names'}, sort_keys=True)}")
        log(f"Hourly city mapping stats: {json.dumps(hourly_map_stats, sort_keys=True)}")

    if daily_enabled:
        log(f"Prepared {len(daily_payloads)} daily payloads")
        log(f"Daily CSV stats: {json.dumps(daily_stats, sort_keys=True)}")
        log(f"Daily filter stats: {json.dumps({k:v for k,v in daily_filter_stats.items() if k != 'dropped_place_names'}, sort_keys=True)}")
        log(f"Daily city mapping stats: {json.dumps(daily_map_stats, sort_keys=True)}")
        if args.mode == "daily":
            log("Daily-only mode: current day is included. No hourly-overlap skipping is applied.")

    dropped_names = sorted(set(hourly_filter_stats.get("dropped_place_names", [])) | set(daily_filter_stats.get("dropped_place_names", [])))
    if dropped_names:
        (workdir / "dropped_city_names.txt").write_text("\n".join(dropped_names) + "\n", encoding="utf-8")
        log(f"Dropped city names written to {workdir / 'dropped_city_names.txt'}")

    if skipped_daily_dates:
        log(f"Skipped daily dates because they are already covered by hourly forecasts: {', '.join(skipped_daily_dates)}")
    if all_unmapped:
        log(f"Unmapped city names written to {workdir / 'unmapped_city_names.txt'}")

    if args.dry_run:
        log("Dry run enabled. Payloads were built but not posted.")
    else:
        verify = not args.insecure
        token = args.token or fetch_token(args.base_url, args.username, args.password, verify)
        for payload in daily_payloads:
            post_payload(args.base_url, token, payload, verify)
        for payload in hourly_payloads:
            post_payload(args.base_url, token, payload, verify)

    new_state = {
        "fingerprint": fingerprint,
        "last_sync_epoch": int(time.time()),
        "last_hourly_rows": len(hourly_rows),
        "last_daily_rows": len(daily_rows),
        "last_hourly_payloads": len(hourly_payloads),
        "last_daily_payloads": len(daily_payloads),
    }
    write_state(state_path, new_state)
    log("State updated.")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", required=True, help="ClimWeb base URL, e.g. https://share.csis.gov.ls")
    p.add_argument("--username", help="Forecast Manager username")
    p.add_argument("--password", help="Forecast Manager password")
    p.add_argument("--token", help="Existing API token; use instead of username/password")
    p.add_argument("--source", default="local", help="Forecast source to send to Forecast Manager")
    p.add_argument("--mode", choices=["hourly", "daily", "both"], default="both")
    p.add_argument("--workdir", default="./lms_forecastmanager_work")
    p.add_argument("--state-file", default="./lms_forecastmanager_work/state.json")
    p.add_argument("--city-mapping-csv", help="Optional CSV with source_city,target_city columns")
    p.add_argument("--daily-effective-time", default="00:00:00")
    p.add_argument("--daily-overlap-policy", choices=["skip_hourly_dates", "keep_all"], default="skip_hourly_dates")

    p.add_argument("--city-filter-mode", choices=["all", "districts_plus_special"], default="districts_plus_special")
    p.add_argument("--allowed-cities-file", help="Optional text file with one allowed city per line. Overrides the preset when provided.")

    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--insecure", action="store_true")

    p.add_argument("--remote-host", help="Remote LMS host for scp pull")
    p.add_argument("--remote-user", default="lmsnwp")
    p.add_argument("--remote-port", type=int, default=22)
    p.add_argument("--ssh-identity", help="Path to SSH private key for scp")
    p.add_argument("--remote-hourly-path", default="/home/lmsnwp/DA/Met_App/Output/HrlyFcWx.csv")
    p.add_argument("--remote-daily-path", default="/home/lmsnwp/DA/Met_App/Output/DailyFcWx.csv")

    p.add_argument("--hourly-csv", help="Local path to HrlyFcWx.csv")
    p.add_argument("--daily-csv", help="Local path to DailyFcWx.csv")

    p.add_argument("--loop", action="store_true")
    p.add_argument("--poll-seconds", type=int, default=300)
    return p


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if not args.token and not (args.username and args.password):
        parser.error("Provide either --token or both --username and --password")

    if args.loop:
        while True:
            try:
                sync_once(args)
            except Exception as exc:
                log(f"ERROR: {exc}")
            time.sleep(args.poll_seconds)
        return 0

    try:
        return sync_once(args)
    except Exception as exc:
        log(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())