import csv
import os
import sys
import datetime
import time
import requests

DB_URL = "http://influxdb:8086"
DB_ORG = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_ORG")
DB_BUCKET = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_BUCKET")
DB_TOKEN = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_TOKEN")

def process_csv(cache_savings_csv_file, td_savings_csv_file=None):

    td_map = {}
    if td_savings_csv_file:
        with open(td_savings_csv_file, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row[9]:
                    print(f"Adding {row[1]} = {row[9]} to td-savings map")
                    td_map[row[1]] = row[9]
        print(f"Test distribution savings map: {len(td_map)} entries")

    with open(cache_savings_csv_file, newline='') as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        prev_timestamp = None
        timestamp_increment = 1
        for i, row in enumerate(reader):
            if i % 100 == 0 and i != 0:
                print("pause...")
                time.sleep(1)

            timestamp_str = row[0]
            try:
                dt = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            timestamp = int(time.mktime(dt.timetuple()) * 1000)
            if timestamp == prev_timestamp:
                print(f"Duplicate timestamp detected, incrementing by {timestamp_increment} ms")
                timestamp += timestamp_increment
                timestamp_increment += 1
            else:
                prev_timestamp = timestamp
                timestamp_increment = 1

            buildId = row[1]
            project = row[3] if row[3] else "unknown"
            project = project.replace(" ", "-").replace("/", "-").replace("\\", "-").replace(",", "-")
            if "Build" in row[5]:
                # before August format
                isCi = str(row[5] == "CI Build")
                durationMs = row[6]
                cacheAvoidanceSavingsMs = row[7] if row[7] else 0
            else:
                # from August format
                isCi = str(row[6] == "CI Build")
                durationMs = row[7]
                cacheAvoidanceSavingsMs = row[8] if row[8] else 0
                # if len(row) > 13:
                #     testDistributionSavingsMs = row[13]
                # else:
                #     testDistributionSavingsMs = 0

            testDistributionSavingsMs = td_map.get(buildId, 0)
            totalSavingsMs = float(cacheAvoidanceSavingsMs) + float(testDistributionSavingsMs)

            print(f"Adding record: {project} / {buildId} / {isCi} / {durationMs} / {cacheAvoidanceSavingsMs} / {testDistributionSavingsMs} / {totalSavingsMs} {timestamp_str}")
            data = f"build,project={project} build_id=0,is_ci={isCi},duration_ms={durationMs},cache_avoidance_savings_ms={cacheAvoidanceSavingsMs},test_distribution_savings_ms={testDistributionSavingsMs},total_savings_ms={totalSavingsMs} {timestamp}"
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
                print(f"Adding record failed [{response.status_code}], moving on to next")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('USAGE: add-combined-data.py <PATH_TO_CACHE_SAVINGS_CSV> [<PATH_TO_TD_SAVINGS_CSV>]')
        sys.exit(1)
    process_csv(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)