import csv
import os
import sys
import time
import requests

DB_URL = "http://influxdb:8086"
DB_ORG = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_ORG")
DB_BUCKET = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_BUCKET")
DB_TOKEN = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_TOKEN")

def parse_count(raw):
    if raw.endswith('K'):
        return int(float(raw[:-1]) * 1000)
    elif raw.endswith('Mil'):
        return int(float(raw[:-3]) * 1000000)
    return int(raw)

def parse_duration(raw):
    if raw.endswith('K'):
        return int(float(raw[:-1]) * 1000 * 1000)
    elif raw.endswith('Mil'):
        return int(float(raw[:-3]) * 1000000 * 1000)
    return int(float(raw) * 1000)

def parse_cache_savings(raw):
    days = 0
    if ' ago' in raw:
        return 0
    if ' d' in raw:
        parts = raw.split(' d')
        days = int(parts[0])
        raw = parts[1].strip()
    hms = [int(x) for x in raw.split(':')]
    while len(hms) < 3:
        hms.append(0)
    hours, minutes, seconds = hms
    return (days * 86400 + hours * 3600 + minutes * 60 + seconds) * 1000

def process_csv(csv_file, process_timestamp_ms):
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        current_timestamp_ms = int(process_timestamp_ms) if process_timestamp_ms else int(time.time() * 1000)

        for row in reader:
            project = row[0].replace(" ", "-").replace("/", "-").replace("\\", "-").replace(",", "-")
            build_count_raw = row[1]
            build_duration_seconds_raw = row[2]
            build_cache_savings_seconds_raw = row[4]
            serial_clock_savings_rate_raw = row[19].replace("%", "")

            build_count = parse_count(build_count_raw)
            build_duration_ms = parse_duration(build_duration_seconds_raw)
            build_cache_savings_ms = parse_cache_savings(build_cache_savings_seconds_raw)

            print(f"Adding record: {project} / {build_count} / {build_duration_ms} / {build_cache_savings_ms} / {serial_clock_savings_rate_raw}")
            data = f"cache,project={project} build_count={build_count},build_duration_ms={build_duration_ms},build_cache_savings_ms={build_cache_savings_ms},serial_clock_savings_rate={serial_clock_savings_rate_raw} {current_timestamp_ms}"
            headers = {"Authorization": f"Token {DB_TOKEN}"}
            params = {
                "org": DB_ORG,
                "db": DB_BUCKET,
                "precision": "ms"
            }
            response = requests.post(f"{DB_URL}/write", params=params, headers=headers, data=data)
            if response.status_code == 404:
                print(f"Adding record failed [{response.status_code}]")
                print(f"Bucket is probably missing, run init.py to create it")
                sys.exit(1)
                break
            if response.status_code != 204:
                print(f"Adding record failed [{response.status_code}]")
                sys.exit(1)
                break


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('USAGE: inject-data.py <PATH_TO_CSV> [TIMESTAMP_IN_MS]')
        sys.exit(1)
    process_csv(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)